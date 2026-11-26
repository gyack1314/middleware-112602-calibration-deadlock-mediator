"""Compatibility module for ``from coordinator.coordinator import Coordinator``."""

from .core import Coordinator, ProtocolError, process_messages
from .graph import build_wait_graph, find_cycles, select_victim

__all__ = [
    "Coordinator",
    "ProtocolError",
    "process_messages",
    "build_wait_graph",
    "find_cycles",
    "select_victim",
]
