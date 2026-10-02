"""Reference publisher for RFID, barcode, and other Redis Stream events."""

from __future__ import annotations

import argparse
import json
import sys
import uuid
from datetime import UTC, datetime

import redis


def utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def main() -> int:
    parser = argparse.ArgumentParser(description="Publish a standard event to a Redis Stream")
    parser.add_argument("--redis-url", default="redis://localhost:6379/0")
    parser.add_argument("--stream", required=True, choices=("rfid.events", "barcode.events", "vision.events"))
    parser.add_argument("--event-type", required=True, help="For example: rfid.scan or barcode.scan")
    parser.add_argument("--source", required=True, help="Stable reader identifier, for example rfid-reader-01")
    parser.add_argument("--data", required=True, help="JSON object containing source-specific measurements")
    parser.add_argument("--maxlen", type=int, default=10_000, help="Approximate stream retention length")
    args = parser.parse_args()

    try:
        data = json.loads(args.data)
    except json.JSONDecodeError as error:
        parser.error(f"--data must be valid JSON: {error.msg}")
    if not isinstance(data, dict):
        parser.error("--data must be a JSON object")

    fields = {
        "event_id": str(uuid.uuid4()),
        "event_type": args.event_type,
        "source": args.source,
        "occurred_at": utc_now(),
        "data": json.dumps(data, separators=(",", ":")),
    }
    client = redis.Redis.from_url(args.redis_url, decode_responses=True)
    try:
        message_id = client.xadd(args.stream, fields, maxlen=args.maxlen, approximate=True)
    except redis.RedisError as error:
        print(f"Could not publish Redis event: {error}", file=sys.stderr)
        return 1
    finally:
        client.close()
    print(message_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
