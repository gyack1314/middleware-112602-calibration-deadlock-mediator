"""Import alias: ``from transaction_coordinator import Coordinator``."""

from coordinator.core import Coordinator, ProtocolError, process_messages
from coordinator.graph import build_wait_graph, find_cycles, select_victim

TransactionCoordinator = Coordinator
DistributedTransactionCoordinator = Coordinator

__all__ = [
    "Coordinator",
    "TransactionCoordinator",
    "DistributedTransactionCoordinator",
    "ProtocolError",
    "process_messages",
    "build_wait_graph",
    "find_cycles",
    "select_victim",
]
