"""Reference publisher for JSON sensor measurements via MQTT."""

from __future__ import annotations

import argparse
import json
import sys

import paho.mqtt.publish as publish


def main() -> int:
    parser = argparse.ArgumentParser(description="Publish one JSON sensor measurement to MQTT")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", default=1883, type=int)
    parser.add_argument("--topic", required=True, help="Use sensors/<machine-id>/<measurement>")
    parser.add_argument("--payload", required=True, help="JSON object containing a measurement")
    parser.add_argument("--qos", default=1, type=int, choices=(0, 1, 2))
    parser.add_argument("--retain", action="store_true")
    args = parser.parse_args()

    if not args.topic.startswith("sensors/"):
        parser.error("--topic must begin with sensors/")
    try:
        payload = json.loads(args.payload)
    except json.JSONDecodeError as error:
        parser.error(f"--payload must be valid JSON: {error.msg}")
    if not isinstance(payload, dict):
        parser.error("--payload must be a JSON object")

    try:
        publish.single(
            args.topic,
            payload=json.dumps(payload, separators=(",", ":")),
            qos=args.qos,
            retain=args.retain,
            hostname=args.host,
            port=args.port,
        )
    except OSError as error:
        print(f"Could not publish MQTT message: {error}", file=sys.stderr)
        return 1
    print(args.topic)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
