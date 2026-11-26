"""Intuitive import alias: ``from deadlock_coordinator import Coordinator``."""

from coordinator.core import Coordinator, ProtocolError, process_messages
from coordinator.graph import build_wait_graph, find_cycles, select_victim

DeadlockCoordinator = Coordinator

__all__ = [
    "Coordinator",
    "DeadlockCoordinator",
    "ProtocolError",
    "process_messages",
    "build_wait_graph",
    "find_cycles",
    "select_victim",
]
