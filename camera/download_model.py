"""Download the configured small YOLO model into the mounted models directory."""

from __future__ import annotations

import os
from pathlib import Path

from ultralytics import YOLO


def main() -> int:
    target = Path(os.getenv("YOLO_MODEL", "models/yolo11n.pt")).resolve()
    if target.exists() and target.stat().st.size > 0:
        print(f"Model already exists: {target}")
        return 0
    try:
    	target.parent.mkdir(parents=True, exist_ok=True)
    except OSError:
	pass

    url = "https://github.com/ultralytics/assets/releases/download/v8.3.0/yolo11n.pt"
    tmp_file = Path("/tmp/downloaded_yolo11n.pt")

    print(f"Downloading Yolo11 model from {url}...")
    
    with urllib.request.urlopen(url) as response, open(tmp_file, "wb") as out_file:
	shutil.copyfileobj(response, out_file)

    try: 
	shutil.copyfile(str(tmp_file), str(target))
	print(f"Success to {target}")
    except OSError as err:
	print(f"Target path {target} is read only file saved locally at {tmp_file}")
	raise err
    finally:
	if tmp_file.exists():
		tmp_file.unlink()


    return 0


if __name__ == "__main__":
    raise SystemExit(main())
