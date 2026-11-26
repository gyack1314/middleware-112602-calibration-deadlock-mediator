"""联合标定分布式事务协调中间件。"""

from .core import Coordinator, ProtocolError, process_messages
from .graph import build_wait_graph, find_cycles, select_victim
from .simulator import LocalAdminCluster, LocalResourceAdmin, run_builtin_demo, run_parallel_demo

ResourceManager = LocalResourceAdmin
SimulatedResourceManager = LocalResourceAdmin
MockResourceAdmin = LocalResourceAdmin
AdminCluster = LocalAdminCluster

DeadlockCoordinator = Coordinator
TransactionCoordinator = Coordinator
DistributedTransactionCoordinator = Coordinator
Middleware = Coordinator
create_coordinator = Coordinator


def handle_message(message, coordinator=None, **kwargs):
    """Convenience one-message API.

    If ``coordinator`` is omitted a new in-memory coordinator is created;
    callers that need multi-message state should construct one explicitly.
    """
    coordinator = coordinator or Coordinator(**kwargs)
    return coordinator.handle(message)

__all__ = [
    "Coordinator",
    "DeadlockCoordinator",
    "TransactionCoordinator",
    "DistributedTransactionCoordinator",
    "Middleware",
    "create_coordinator",
    "handle_message",
    "ProtocolError",
    "process_messages",
    "build_wait_graph",
    "find_cycles",
    "select_victim",
    "LocalAdminCluster",
    "LocalResourceAdmin",
    "ResourceManager",
    "SimulatedResourceManager",
    "MockResourceAdmin",
    "AdminCluster",
    "run_builtin_demo",
    "run_parallel_demo",
]

__version__ = "1.0.0"
