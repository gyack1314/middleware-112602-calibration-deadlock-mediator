#!/usr/bin/env python3
"""项目入口。

示例：
    python main.py examples/stale-wait-edges.jsonl --format text
    cat scenario.jsonl | python main.py
    python main.py --demo --format text
"""

import sys
from pathlib import Path

# 兼容 ``python -I main.py``：隔离模式不会自动把脚本目录放上 sys.path。
_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from coordinator import handle_message
from coordinator.cli import main, replay_file
from coordinator.core import Coordinator, process_messages

__all__ = ["main", "replay_file", "Coordinator", "process_messages", "handle_message", "run_scenario"]


def run_scenario(path, state_path=None, slow_threshold=2):
    """Convenience API: replay one JSONL path and return ``(coordinator, responses)``."""
    return replay_file(path, state_path=state_path, slow_threshold=slow_threshold)


if __name__ == "__main__":
    raise SystemExit(main())
