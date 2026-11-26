"""Import alias for deadlock-detection oriented callers."""

from coordinator.core import Coordinator
from coordinator.graph import build_wait_graph, find_cycles, select_victim


class DeadlockDetector(Coordinator):
    """Coordinator with a detector-named API."""

    def detect(self, at=None):
        return self.scan(at=at)

    def detect_read_only(self):
        graph = self.graph()
        return {
            "edges": graph["edges"],
            "evidence": graph["evidence"],
            "cycles": find_cycles(graph["edges"]),
        }


__all__ = ["DeadlockDetector", "build_wait_graph", "find_cycles", "select_victim"]
