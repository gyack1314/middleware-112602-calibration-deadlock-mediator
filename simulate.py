#!/usr/bin/env python3
"""快捷入口：运行本地模拟管理员的三组并行联调演示。"""

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from coordinator.cli import main

if __name__ == "__main__":
    raise SystemExit(main(["--parallel-demo", "--format", "text"]))
