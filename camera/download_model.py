"""Download the configured small YOLO model into the mounted models directory."""

from __future__ import annotations

import os
from pathlib import Path

from ultralytics import YOLO


def main() -> int:
    target = Path(os.getenv("YOLO_MODEL", "/models/yolo11n.pt"))
    if target.exists():
        print(f"Model already exists: {target}")
        return 0
    if target.suffix != ".pt":
        raise ValueError("model-fetch downloads official .pt weights; mount a custom model file directly")

    target.parent.mkdir(parents=True, exist_ok=True)
    original_directory = Path.cwd()
    try:
        # Ultralytics downloads a named official model to its working directory.
        os.chdir(target.parent)
        YOLO(target.name)
    finally:
        os.chdir(original_directory)

    if not target.exists():
        raise RuntimeError(f"Ultralytics did not create the requested model at {target}")
    print(f"Downloaded model: {target}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
