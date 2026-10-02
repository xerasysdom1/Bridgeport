"""Reference heartbeat publisher for the Redis machine-running gate."""

from __future__ import annotations

import argparse
import sys

import redis


def main() -> int:
    parser = argparse.ArgumentParser(description="Set or refresh a machine running state in Redis")
    parser.add_argument("--redis-url", default="redis://localhost:6379/0")
    parser.add_argument("--machine-id", required=True)
    parser.add_argument("--state", required=True, choices=("running", "stopped"))
    parser.add_argument(
        "--ttl",
        default=30,
        type=int,
        help="Seconds before a running heartbeat fails closed; ignored for stopped",
    )
    args = parser.parse_args()
    if args.ttl < 1:
        parser.error("--ttl must be at least 1")

    key = f"machine:{args.machine_id}:running"
    client = redis.Redis.from_url(args.redis_url, decode_responses=True)
    try:
        if args.state == "running":
            client.setex(key, args.ttl, "1")
            print(f"{key}=1 (expires in {args.ttl}s)")
        else:
            client.set(key, "0")
            print(f"{key}=0")
    except redis.RedisError as error:
        print(f"Could not set machine state: {error}", file=sys.stderr)
        return 1
    finally:
        client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
