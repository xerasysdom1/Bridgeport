"""Detect people from a USB camera and publish presence transitions to Redis.

Frames exist only in process memory. This service never writes images, video, or
detection crops to Redis or disk.
"""

from __future__ import annotations

import json
import logging
import os
import signal
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import cv2
import redis
from ultralytics import YOLO


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
LOG = logging.getLogger("bridgeport.camera")
RUNNING = True


def env_int(name: str, default: int, *, minimum: int | None = None) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except ValueError as error:
        raise ValueError(f"{name} must be an integer") from error
    if minimum is not None and value < minimum:
        raise ValueError(f"{name} must be at least {minimum}")
    return value


def env_float(name: str, default: float, *, minimum: float | None = None) -> float:
    try:
        value = float(os.getenv(name, str(default)))
    except ValueError as error:
        raise ValueError(f"{name} must be a number") from error
    if minimum is not None and value < minimum:
        raise ValueError(f"{name} must be at least {minimum}")
    return value


def utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def stop(_signal: int, _frame: object) -> None:
    global RUNNING
    RUNNING = False
    LOG.info("Shutdown requested")


def open_camera(device: str, width: int, height: int, fps: int) -> cv2.VideoCapture:
    # V4L2 is the normal USB-camera interface on Raspberry Pi OS/Linux.
    capture = cv2.VideoCapture(device, cv2.CAP_V4L2)
    if not capture.isOpened():
        capture.release()
        capture = cv2.VideoCapture(device)  # Fall back to another OpenCV backend.
    if capture.isOpened():
        capture.set(cv2.CAP_PROP_FRAME_WIDTH, width)
        capture.set(cv2.CAP_PROP_FRAME_HEIGHT, height)
        capture.set(cv2.CAP_PROP_FPS, fps)
    return capture


class PresencePublisher:
    """Debounce person detections and keep the machine-presence Redis key fresh."""

    def __init__(
        self,
        client: redis.Redis,
        *,
        camera_id: str,
        machine_id: str,
        confirm_frames: int,
        absence_timeout: float,
        presence_ttl: int,
        stream_maxlen: int,
    ) -> None:
        self.client = client
        self.camera_id = camera_id
        self.machine_id = machine_id
        self.confirm_frames = confirm_frames
        self.absence_timeout = absence_timeout
        self.presence_ttl = presence_ttl
        self.stream_maxlen = stream_maxlen
        self.stream = os.getenv("REDIS_STREAM", "vision.events")
        self.presence_key = f"machine:{machine_id}:human_present"
        self.status_key = f"vision:camera:{camera_id}:status"
        self.present = False
        self.consecutive_positive_frames = 0
        self.last_seen_monotonic: float | None = None
        self.last_seen_at: str | None = None

    def initialize(self) -> None:
        # A just-started camera is conservatively "not present" until it sees a
        # person. A controller must only permit operation for the literal value 1.
        self.client.set(self.presence_key, "0")

    def observe(self, has_person: bool, person_count: int, observed_at: str) -> None:
        now = time.monotonic()
        if has_person:
            self.consecutive_positive_frames += 1
            self.last_seen_monotonic = now
            self.last_seen_at = observed_at
            if not self.present and self.consecutive_positive_frames >= self.confirm_frames:
                self.present = True
                self._write_state("1", event_type="human.present", observed_at=observed_at, person_count=person_count)
                LOG.info("Person present for machine=%s", self.machine_id)
            elif self.present:
                # A TTL makes a camera/container/Redis-client failure fail closed:
                # after PRESENCE_TTL_SECONDS without a fresh detection, no literal
                # "1" remains for a downstream operational interlock to read.
                self._write_state("1")
            return

        self.consecutive_positive_frames = 0
        if (
            self.present
            and self.last_seen_monotonic is not None
            and now - self.last_seen_monotonic >= self.absence_timeout
        ):
            last_seen_at = self.last_seen_at
            self.present = False
            self._write_state(
                "0",
                event_type="human.absent",
                observed_at=observed_at,
                person_count=0,
                last_seen_at=last_seen_at,
            )
            LOG.info("Person absent for machine=%s", self.machine_id)

    def heartbeat(self) -> None:
        """Expose whether the camera process remains alive without storing frames."""
        self.client.setex(self.status_key, 15, utc_now())

    def _write_state(
        self,
        value: str,
        *,
        event_type: str | None = None,
        observed_at: str | None = None,
        person_count: int | None = None,
        last_seen_at: str | None = None,
    ) -> None:
        with self.client.pipeline(transaction=True) as pipe:
            if value == "1":
                pipe.setex(self.presence_key, self.presence_ttl, value)
            else:
                # An explicit absence must not expire back to a stale "present".
                pipe.set(self.presence_key, value)
            if event_type and observed_at:
                data: dict[str, Any] = {
                    "machine_id": self.machine_id,
                    "camera_id": self.camera_id,
                    "person_count": person_count,
                }
                if last_seen_at:
                    data["last_seen_at"] = last_seen_at
                pipe.xadd(
                    self.stream,
                    {
                        "event_id": str(uuid.uuid4()),
                        "event_type": event_type,
                        "source": self.camera_id,
                        "machine_id": self.machine_id,
                        "occurred_at": observed_at,
                        "data": json.dumps(data, separators=(",", ":")),
                    },
                    maxlen=self.stream_maxlen,
                    approximate=True,
                )
            pipe.execute()


def count_people(model: YOLO, frame: Any, confidence: float, image_size: int) -> int:
    # COCO class 0 is person. Filtering at inference avoids retaining or emitting
    # information about any other object the model can recognize.
    result = model.predict(
        frame,
        classes=[0],
        conf=confidence,
        imgsz=image_size,
        device="cpu",
        verbose=False,
    )[0]
    return 0 if result.boxes is None else len(result.boxes)


def main() -> int:
    redis_url = os.getenv("REDIS_URL", "redis://redis:6379/0")
    device = os.getenv("CAMERA_DEVICE", "/dev/video0")
    camera_id = os.getenv("CAMERA_ID", "pi-camera-01")
    machine_id = os.getenv("MACHINE_ID", "machine-01")
    model_path = Path(os.getenv("YOLO_MODEL", "/models/yolo11n.pt"))
    width = env_int("CAPTURE_WIDTH", 1280, minimum=1)
    height = env_int("CAPTURE_HEIGHT", 720, minimum=1)
    fps = env_int("CAPTURE_FPS", 5, minimum=1)
    image_size = env_int("YOLO_IMAGE_SIZE", 320, minimum=32)
    confidence = env_float("YOLO_CONFIDENCE", 0.50, minimum=0.0)
    confirm_frames = env_int("PRESENCE_CONFIRM_FRAMES", 2, minimum=1)
    absence_timeout = env_float("ABSENCE_TIMEOUT_SECONDS", 5, minimum=0.0)
    presence_ttl = env_int("PRESENCE_TTL_SECONDS", 60, minimum=1)
    stream_maxlen = env_int("EVENT_STREAM_MAXLEN", 10_000, minimum=1)
    reconnect_seconds = env_int("CAMERA_RECONNECT_SECONDS", 5, minimum=1)
    interval = 1 / fps

    if not model_path.is_file():
        raise FileNotFoundError(
            f"YOLO model not found at {model_path}. Run: "
            "docker compose --profile tools run --rm model-fetch"
        )

    LOG.info("Loading YOLO model from %s", model_path)
    model = YOLO(str(model_path))
    client = redis.Redis.from_url(redis_url, decode_responses=True)
    publisher = PresencePublisher(
        client,
        camera_id=camera_id,
        machine_id=machine_id,
        confirm_frames=confirm_frames,
        absence_timeout=absence_timeout,
        presence_ttl=presence_ttl,
        stream_maxlen=stream_maxlen,
    )
    try:
        publisher.initialize()
    except redis.RedisError:
        LOG.exception("Could not mark initial absence in Redis")

    LOG.info("Starting person detection camera=%s device=%s machine=%s", camera_id, device, machine_id)
    while RUNNING:
        capture = open_camera(device, width, height, fps)
        if not capture.isOpened():
            LOG.error("Cannot open camera %s; retrying in %s seconds", device, reconnect_seconds)
            capture.release()
            time.sleep(reconnect_seconds)
            continue

        try:
            while RUNNING:
                started = time.monotonic()
                ok, frame = capture.read()
                if not ok or frame is None:
                    LOG.warning("Camera read failed; reopening device")
                    break
                observed_at = utc_now()
                try:
                    people = count_people(model, frame, confidence, image_size)
                    publisher.observe(people > 0, people, observed_at)
                    publisher.heartbeat()
                except redis.RedisError:
                    # Do not pretend that presence is valid when Redis is down.
                    # The last "1" is intentionally allowed to expire.
                    LOG.exception("Redis presence update failed")
                except Exception:
                    LOG.exception("YOLO inference failed; dropping frame")

                sleep_for = interval - (time.monotonic() - started)
                if sleep_for > 0:
                    time.sleep(sleep_for)
        finally:
            capture.release()
        if RUNNING:
            time.sleep(reconnect_seconds)

    client.close()
    return 0


if __name__ == "__main__":
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    raise SystemExit(main())
