# Bridgeport Raspberry Pi data stack

This is one Docker Compose deployment for a Raspberry Pi. It runs Redis, MQTT, a Python YOLO person-presence camera, and a Python collector that saves data only for an attended, running machine.

| Service | Purpose | Interface |
| --- | --- | --- |
| `redis` | Event streams and short-lived control state for vision, RFID, barcode, and the machine integration | `localhost:6379` on the Pi; Docker name `redis` |
| `mqtt` | MQTT broker for RPM, vibration, and other sensor telemetry | port `1883`; Docker name `mqtt` |
| `camera` | Python USB-camera / YOLO process; publishes only person-presence changes, never an image | `/dev/video0` by default |
| `data-collector` | Python process that writes eligible Redis and MQTT data to JSON Lines | `./data/` |

## Important machine-safety boundary

This camera is an operational monitoring signal, **not** a safety guard or a safety-rated machine-stop system. Do not use a Raspberry Pi, Docker, Redis, a network connection, or a general-purpose YOLO model as the only protection against injury. A qualified controls/safety engineer must design, validate, and maintain the actual safety-rated guarding, stopping circuit, and risk assessment for the specific equipment. OSHA requires appropriate machine guarding and describes specific requirements for presence-sensing safeguarding, including failure behavior and safety distance. [OSHA 1910.212](https://www.osha.gov/laws-regs/regulations/standardnumber/1910/1910.212) and [OSHA presence-sensing guidance](https://www.osha.gov/etools/machine-guarding/presses/presence-sensing-devices) are useful starting points.

The Redis presence key below is appropriate as a *non-safety operational interlock* or monitoring input only. The machine controller must fail closed if the key is missing, Redis is unreachable, or the camera is unhealthy.

## Start on the Raspberry Pi

Use 64-bit Raspberry Pi OS, Docker Engine, and the Docker Compose plugin. From this directory:

```sh
cp .env.example .env
# Set CAMERA_DEVICE and MACHINE_ID in .env for the installed hardware.
ls /dev/video*

# One-time model download. This writes the small yolo11n model to ./models/.
docker compose --profile tools run --rm model-fetch

docker compose up -d --build
docker compose ps
docker compose logs -f camera data-collector
```

The `model-fetch` step needs internet access once. It deliberately keeps the model outside the image at `./models/yolo11n.pt`, so the deployed model can be reviewed, replaced, or downloaded on a staging machine and copied to the Pi. The `models/` directory is ignored by Git.

To stop services while retaining Redis/MQTT data and collected files:

```sh
docker compose down
```

`docker compose down -v` also deletes the named Redis and MQTT volumes. It does not delete `./data/` or `./models/`.

## Camera presence behavior

The camera uses the small COCO YOLO11 model, filters inference to the `person` class, and holds every frame only in RAM. It does not save frames, JPEGs, video, bounding boxes, or crops.

After `PRESENCE_CONFIRM_FRAMES` positive detections (default 2), it sends exactly one Redis Stream event:

```json
{
  "event_type": "human.present",
  "machine_id": "machine-01",
  "occurred_at": "2026-09-29T18:42:11.120Z"
}
```

After no person has been detected for `ABSENCE_TIMEOUT_SECONDS` (default 5), it sends exactly one matching `human.absent` event. The events are published to `vision.events`; their full fields include a UUID, source/camera ID, and a JSON `data` object.

While a person is present, the camera refreshes this Redis key:

```text
machine:<machine-id>:human_present = "1"   (TTL 60 seconds by default)
```

On a confirmed absence it becomes `"0"`. If the camera, its container, or its Redis connection fails while somebody was present, the `"1"` value expires after `PRESENCE_TTL_SECONDS` (default 60) instead of remaining valid. A downstream non-safety controller must accept only the literal `"1"`; a missing key is **not present**. Set `PRESENCE_TTL_SECONDS=60` or less to meet the one-minute operational timeout.

Useful checks:

```sh
docker compose logs -f camera
docker compose exec redis redis-cli XRANGE vision.events - + COUNT 10
docker compose exec redis redis-cli GET machine:machine-01:human_present
docker compose exec redis redis-cli TTL machine:machine-01:human_present
```

## Collection gate: machine must be running and have an active user

The collector still receives all Redis and MQTT messages, but it writes a JSONL record only when **both** Redis conditions are true for that event's machine ID:

| Redis key | Required value | How it is maintained |
| --- | --- | --- |
| `machine:<machine-id>:running` | `"1"` | The machine/PLC integration refreshes it with a TTL heartbeat while the machine is running. |
| `machine:<machine-id>:active_user` | A non-expired session JSON object | The collector creates/refreshes it when it receives an `rfid.scan` or `barcode.scan` event containing `machine_id` and `user_id`. |

The active-user session expires after `USER_SESSION_TTL_SECONDS` (default 300). A scanner can send `rfid.logout`, `barcode.logout`, `rfid.clear`, or `barcode.clear` to clear the session immediately. A missing machine ID, stopped/missing running key, or missing/expired user session means the message is consumed but not saved.

```text
RFID/barcode scan ──> active_user key ──┐
                                         ├──> collector archives Redis + MQTT data
machine running heartbeat ─> running key ┘
```

The user session is updated even if the machine is stopped, so an operator can scan in before the machine starts. The scan itself is archived only if the machine is already running. A fresh collector starts from new stream entries rather than applying today's state to historical events; after that, durable checkpoints let it resume from its last processed entry.

### Machine-running heartbeat

The machine integration should refresh the running key more often than its TTL. This Python reference command is useful for testing; in production, call the equivalent Redis operation from the PLC gateway or machine-state service.

```sh
# Run every 10 seconds while machine-01 is actually operating.
python clients/set_machine_state.py \
  --machine-id machine-01 --state running --ttl 30

# Write a stopped state immediately when the machine is off.
python clients/set_machine_state.py \
  --machine-id machine-01 --state stopped
```

Never treat a stale `running` value as running: the helper uses `SETEX` for `running`, so it expires automatically if its publisher dies.

## Event and telemetry contracts

### Redis: RFID and barcode

The collector reads `vision.events`, `rfid.events`, and `barcode.events`. RFID/barcode scans must include both `machine_id` and `user_id` in `data`; `data` is a JSON object encoded as a Redis Stream field. The generic Python publisher does this without custom Redis code:

```sh
pip install -r clients/requirements.txt

python clients/publish_redis_event.py \
  --redis-url redis://localhost:6379/0 \
  --stream rfid.events \
  --event-type rfid.scan \
  --source rfid-reader-01 \
  --data '{"machine_id":"machine-01","user_id":"operator-123","tag_id":"E2000017221101441890BEEF","antenna":1}'

python clients/publish_redis_event.py \
  --redis-url redis://localhost:6379/0 \
  --stream barcode.events \
  --event-type barcode.scan \
  --source barcode-reader-01 \
  --data '{"machine_id":"machine-01","user_id":"operator-123","barcode":"OPERATOR-123"}'
```

Inside Compose, use `redis` as the hostname; for a process running directly on the Pi, use `localhost`.

### MQTT: RPM, vibration, and other sensors

Use the topic pattern `sensors/<machine-id>/<measurement>` and JSON payloads:

```sh
python clients/publish_mqtt_sensor.py \
  --host localhost \
  --topic sensors/machine-01/rpm \
  --payload '{"value":1724.2,"unit":"rpm","observed_at":"2026-09-29T18:42:11.120Z"}'
```

The collector subscribes to `sensors/#`. It derives the machine ID from the topic, looks up the two Redis gate keys, and writes only eligible messages. It records the topic, QoS, retain flag, payload, active user, and receipt time.

## Collected data and operations

Eligible records are append-only JSON Lines files partitioned by UTC day:

```text
data/
  2026-09-29/
    events.jsonl
  checkpoints.json
```

Records contain `transport` (`redis` or `mqtt`), `machine_id`, `active_user`, receipt time, and the original data. Redis stream IDs are checkpointed only after an event is handled; on a crash the final record can be duplicated, so downstream processing should de-duplicate by `event_id` where present.

## Network and security note

Redis is localhost-only by default. MQTT is exposed on all Pi interfaces so remote sensor devices can publish, and the starter Mosquitto configuration permits anonymous connections. Use a trusted isolated network for development. Before deploying to an untrusted network, enable MQTT authentication/TLS, firewall port 1883, and never expose Redis publicly.
