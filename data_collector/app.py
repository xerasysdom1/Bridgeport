"""Archive telemetry only while Redis says a machine is running and attended."""

from __future__ import annotations

import json
import logging
import os
import signal
import threading
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import paho.mqtt.client as mqtt
import redis


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
LOG = logging.getLogger("bridgeport.collector")
RUNNING = True
ACCESS_EVENTS = {"rfid.scan", "barcode.scan"}
ACCESS_CLEAR_EVENTS = {"rfid.logout", "barcode.logout", "rfid.clear", "barcode.clear"}
RUNNING_VALUES = {"1", "true", "running", "on"}


def utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def parse_json(value: str) -> Any:
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return value


def event_data(fields: dict[str, str]) -> dict[str, Any]:
    data = parse_json(fields.get("data", "{}"))
    return data if isinstance(data, dict) else {}


def first_string(*values: Any) -> str | None:
    for value in values:
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


class JsonlArchive:
    """Thread-safe append-only UTC-day archive plus Redis stream checkpoints."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)
        self.checkpoint_path = root / "checkpoints.json"
        self.lock = threading.Lock()
        self.checkpoints = self._load_checkpoints()

    def _load_checkpoints(self) -> dict[str, str]:
        try:
            with self.checkpoint_path.open(encoding="utf-8") as checkpoint_file:
                loaded = json.load(checkpoint_file)
            return loaded if isinstance(loaded, dict) else {}
        except FileNotFoundError:
            return {}
        except (OSError, json.JSONDecodeError):
            LOG.exception("Invalid checkpoint file; new Redis messages will be used")
            return {}

    def write(self, event: dict[str, Any]) -> None:
        day = datetime.now(UTC).strftime("%Y-%m-%d")
        destination = self.root / day
        with self.lock:
            destination.mkdir(parents=True, exist_ok=True)
            with (destination / "events.jsonl").open("a", encoding="utf-8") as event_file:
                event_file.write(json.dumps(event, separators=(",", ":"), ensure_ascii=False))
                event_file.write("\n")
                event_file.flush()
                os.fsync(event_file.fileno())

    def checkpoint(self, stream: str, message_id: str) -> None:
        with self.lock:
            self.checkpoints[stream] = message_id
            temporary = self.checkpoint_path.with_suffix(".tmp")
            with temporary.open("w", encoding="utf-8") as checkpoint_file:
                json.dump(self.checkpoints, checkpoint_file, separators=(",", ":"))
                checkpoint_file.flush()
                os.fsync(checkpoint_file.fileno())
            temporary.replace(self.checkpoint_path)


class CollectionGate:
    """Maintains access sessions and decides whether a message may be archived.

    Redis is the source of truth. The machine-running key must be refreshed by
    the machine/PLC integration; RFID and barcode scans refresh the user key.
    """

    def __init__(self, client: redis.Redis, default_machine_id: str, session_ttl: int) -> None:
        self.client = client
        self.default_machine_id = default_machine_id
        self.session_ttl = session_ttl

    @staticmethod
    def running_key(machine_id: str) -> str:
        return f"machine:{machine_id}:running"

    @staticmethod
    def active_user_key(machine_id: str) -> str:
        return f"machine:{machine_id}:active_user"

    def machine_from_redis(self, fields: dict[str, str]) -> str | None:
        data = event_data(fields)
        return first_string(fields.get("machine_id"), data.get("machine_id"), self.default_machine_id)

    def machine_from_mqtt(self, topic: str, payload: Any) -> str | None:
        # The documented topic format is sensors/<machine-id>/<measurement>.
        topic_parts = topic.split("/")
        topic_machine = topic_parts[1] if len(topic_parts) >= 3 and topic_parts[0] == "sensors" else None
        payload_machine = payload.get("machine_id") if isinstance(payload, dict) else None
        return first_string(payload_machine, topic_machine, self.default_machine_id)

    def update_user_session(self, fields: dict[str, str]) -> None:
        event_type = fields.get("event_type", "")
        if event_type not in ACCESS_EVENTS | ACCESS_CLEAR_EVENTS:
            return
        machine_id = self.machine_from_redis(fields)
        if not machine_id:
            LOG.warning("Ignoring access event with no machine_id")
            return
        key = self.active_user_key(machine_id)
        if event_type in ACCESS_CLEAR_EVENTS:
            self.client.delete(key)
            LOG.info("Cleared active user for machine=%s", machine_id)
            return

        data = event_data(fields)
        user_id = first_string(
            fields.get("user_id"),
            data.get("user_id"),
            data.get("operator_id"),
        )
        if not user_id:
            LOG.warning("Ignoring %s with no user_id for machine=%s", event_type, machine_id)
            return
        session = {
            "user_id": user_id,
            "source": fields.get("source", "unknown"),
            "event_type": event_type,
            "activated_at": fields.get("occurred_at", utc_now()),
        }
        self.client.setex(key, self.session_ttl, json.dumps(session, separators=(",", ":")))
        LOG.info("Activated user=%s for machine=%s", user_id, machine_id)

    def active_user_if_allowed(self, machine_id: str | None) -> dict[str, Any] | None:
        if not machine_id:
            return None
        running = self.client.get(self.running_key(machine_id))
        if not running or running.strip().lower() not in RUNNING_VALUES:
            return None
        raw_user = self.client.get(self.active_user_key(machine_id))
        if not raw_user:
            return None
        user = parse_json(raw_user)
        # All sessions created by this service are JSON. A non-JSON externally
        # written value is retained as user_id rather than allowing an ambiguity.
        return user if isinstance(user, dict) else {"user_id": raw_user}


def mqtt_client(archive: JsonlArchive, gate: CollectionGate) -> mqtt.Client:
    def on_connect(client: mqtt.Client, _userdata: Any, _flags: Any, reason_code: Any, _properties: Any) -> None:
        if reason_code == 0:
            topic = os.getenv("MQTT_TOPIC", "sensors/#")
            client.subscribe(topic, qos=1)
            LOG.info("Connected to MQTT; subscribed to %s", topic)
        else:
            LOG.error("MQTT connection rejected: %s", reason_code)

    def on_message(_client: mqtt.Client, _userdata: Any, message: mqtt.MQTTMessage) -> None:
        payload = parse_json(message.payload.decode("utf-8", errors="replace"))
        try:
            machine_id = gate.machine_from_mqtt(message.topic, payload)
            active_user = gate.active_user_if_allowed(machine_id)
            if not active_user:
                LOG.debug("Dropped MQTT event on %s: machine not running or no active user", message.topic)
                return
            archive.write(
                {
                    "transport": "mqtt",
                    "received_at": utc_now(),
                    "machine_id": machine_id,
                    "active_user": active_user,
                    "topic": message.topic,
                    "qos": message.qos,
                    "retain": message.retain,
                    "payload": payload,
                }
            )
        except (OSError, redis.RedisError):
            LOG.exception("Could not process MQTT message on %s", message.topic)

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="bridgeport-data-collector")
    client.on_connect = on_connect
    client.on_message = on_message
    return client


def redis_event(
    stream: str,
    message_id: str,
    fields: dict[str, str],
    machine_id: str,
    active_user: dict[str, Any],
) -> dict[str, Any]:
    normalized_fields: dict[str, Any] = fields
    if "data" in fields:
        normalized_fields = {**fields, "data": parse_json(fields["data"])}
    return {
        "transport": "redis",
        "received_at": utc_now(),
        "machine_id": machine_id,
        "active_user": active_user,
        "stream": stream,
        "stream_id": message_id,
        "fields": normalized_fields,
    }


def stop(_signal: int, _frame: object) -> None:
    global RUNNING
    RUNNING = False
    LOG.info("Shutdown requested")


def main() -> int:
    archive = JsonlArchive(Path(os.getenv("COLLECTION_DIR", "/data")))
    streams = [
        item.strip()
        for item in os.getenv("REDIS_STREAMS", "vision.events,rfid.events,barcode.events").split(",")
        if item.strip()
    ]
    # A fresh collector intentionally ignores old stream entries. There is no
    # reliable way to re-evaluate their historical machine/user state. On a
    # restart, the durable checkpoint continues from the last processed entry.
    positions = {stream: archive.checkpoints.get(stream, "$") for stream in streams}
    redis_client = redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis:6379/0"), decode_responses=True)
    session_ttl = int(os.getenv("USER_SESSION_TTL_SECONDS", "300"))
    if session_ttl < 1:
        raise ValueError("USER_SESSION_TTL_SECONDS must be at least 1")
    gate = CollectionGate(redis_client, os.getenv("DEFAULT_MACHINE_ID", "machine-01"), session_ttl)

    client = mqtt_client(archive, gate)
    client.connect_async(os.getenv("MQTT_HOST", "mqtt"), int(os.getenv("MQTT_PORT", "1883")), keepalive=60)
    client.loop_start()

    LOG.info("Collector started; it will archive only for running, attended machines")
    try:
        while RUNNING:
            try:
                messages = redis_client.xread(positions, count=100, block=1000)
                for stream, entries in messages:
                    for message_id, fields in entries:
                        gate.update_user_session(fields)
                        machine_id = gate.machine_from_redis(fields)
                        active_user = gate.active_user_if_allowed(machine_id)
                        if active_user and machine_id:
                            archive.write(redis_event(stream, message_id, fields, machine_id, active_user))
                        else:
                            LOG.debug("Dropped Redis %s entry %s: machine not running or no active user", stream, message_id)
                        # Discard gated-out messages too; their state has been applied
                        # and retaining them for a later state change would be wrong.
                        positions[stream] = message_id
                        archive.checkpoint(stream, message_id)
            except redis.RedisError:
                LOG.exception("Redis read failed; retrying")
                time.sleep(2)
            except OSError:
                LOG.exception("Could not write collected Redis event; retrying")
                time.sleep(2)
    finally:
        client.loop_stop()
        client.disconnect()
        redis_client.close()
    return 0


if __name__ == "__main__":
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    raise SystemExit(main())
