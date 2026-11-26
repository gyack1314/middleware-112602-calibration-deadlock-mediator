"""对接本地模拟资源管理员的轻量适配器。

协调器只输出幂等 release 命令；:class:`LocalAdminCluster` 把命令送给
对应管理员，并按管理员本地阶段/网络延迟异步产生 release 回执。
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Callable

from .core import Coordinator


class LocalResourceAdmin:
    def __init__(
        self,
        owner: str,
        epoch: int = 1,
        *,
        release_delay: int | Callable[[dict[str, Any]], int] = 1,
        late_prepared: set[str] | dict[str, str] | None = None,
        unknown: set[str] | None = None,
    ) -> None:
        self.owner = owner
        self.epoch = epoch
        self.release_delay = release_delay
        # 键为 tx#attempt，或 tx#attempt@owner/resource；命中后 release 时晚报 prepared。
        self.late_prepared = set(late_prepared or ())
        self.unknown_rounds = set(unknown or ())
        self.holds: dict[str, dict[str, Any]] = {}
        self.scheduled: list[dict[str, Any]] = []
        self.sent_commands: list[dict[str, Any]] = []

    def register_hold(self, ref: str, round_id: str, phase: str = "reversible") -> None:
        self.holds[ref] = {"round": round_id, "phase": phase}

    def queue_release(self, command: dict[str, Any], at: int, coordinator: Coordinator) -> dict[str, Any]:
        self.sent_commands.append(command)
        delay = self.release_delay if isinstance(self.release_delay, int) else self.release_delay(command)
        key = command["round"]
        resource_keys = {f"{key}@{ref}" for ref in command.get("resources", [])}
        force_prepared = key in self.late_prepared or bool(resource_keys & set(self.late_prepared))
        force_unknown = key in self.unknown_rounds or bool(resource_keys & self.unknown_rounds)

        prepared = force_prepared
        if not prepared:
            for ref in command.get("resources", []):
                hold = coordinator.state["holds"].get(ref)
                if hold and hold.get("round") == key and hold.get("phase") == "prepared":
                    prepared = True
                    break
        if prepared:
            result = "prepared"
        elif force_unknown:
            result = "unknown"
        else:
            result = "released"
        response = {
            "deliver_at": at + max(0, int(delay)),
            "message": {
                "at": at + max(0, int(delay)),
                "kind": "release",
                "record": {
                    "owner": self.owner,
                    "epoch": self.epoch,
                    "tx": command["tx"],
                    "attempt": command["attempt"],
                    "result": result,
                    "decision_id": command["decision_id"],
                },
            },
        }
        self.scheduled.append(response)
        return response

    def receive_release_command(self, command: dict[str, Any], at: int, coordinator: Coordinator):
        return self.queue_release(command, at, coordinator)

    def due(self, at: int) -> list[dict[str, Any]]:
        due = [item for item in self.scheduled if item["deliver_at"] <= at]
        self.scheduled = [item for item in self.scheduled if item["deliver_at"] > at]
        return due


class LocalAdminCluster:
    def __init__(self, coordinator: Coordinator | None = None) -> None:
        self.coordinator = coordinator or Coordinator()
        self.admins: dict[str, LocalResourceAdmin] = {}
        self.transcript: list[dict[str, Any]] = []

    def add_admin(self, admin: LocalResourceAdmin) -> LocalResourceAdmin:
        self.admins[admin.owner] = admin
        return admin

    def register(
        self,
        owner: str,
        epoch: int = 1,
        *,
        release_delay: int | Callable[[dict[str, Any]], int] = 1,
        late_prepared: set[str] | None = None,
        unknown: set[str] | None = None,
    ) -> LocalResourceAdmin:
        return self.add_admin(
            LocalResourceAdmin(
                owner,
                epoch,
                release_delay=release_delay,
                late_prepared=late_prepared,
                unknown=unknown,
            )
        )

    def route_commands(self, commands: list[dict[str, Any]], at: int) -> list[dict[str, Any]]:
        queued = []
        for command in commands:
            admin = self.admins[command["owner"]]
            queued.append(admin.queue_release(command, at, self.coordinator))
        return queued

    def handle(self, message: dict[str, Any]) -> dict[str, Any]:
        result = self.coordinator.handle(message)
        self.transcript.append({"in": message, "out": result})
        if isinstance(result, dict) and result.get("release_commands"):
            self.route_commands(result["release_commands"], message.get("at", 0))
        return result

    def advance(self, at: int) -> list[dict[str, Any]]:
        responses = []
        for admin in self.admins.values():
            for scheduled in admin.due(at):
                message = scheduled["message"]
                result = self.coordinator.handle(message)
                self.transcript.append({"in": message, "out": result})
                responses.append(result)
                if result.get("release_commands"):
                    self.route_commands(result["release_commands"], message["at"])
        return responses

    def redeliver_pending(self, at: int) -> None:
        commands = self.coordinator.scan(at=at).get("pending_release_commands", [])
        # scan 已把报告写入返回值；这里只做 at-least-once 投递，不创建新决定。
        self.route_commands(commands, at)


def _msg(at: int, kind: str, record: dict[str, Any]) -> dict[str, Any]:
    return {"at": at, "kind": kind, "record": record}


def run_parallel_demo() -> dict[str, Any]:
    """三组并行联调：一组破环继续、一组无环推进、一组证据不足继续等待。"""
    cluster = LocalAdminCluster(Coordinator(slow_threshold=2))
    cluster.register("ra", epoch=1, release_delay=1)
    cluster.register("rb", epoch=1, release_delay=3)  # 慢网络/慢管理员
    cluster.register("fa", epoch=1)
    cluster.register("fb", epoch=1)
    cluster.register("sa", epoch=1)
    cluster.register("sb", epoch=1)

    messages: list[dict[str, Any]] = []
    for owner in ("ra", "rb", "fa", "fb", "sa", "sb"):
        messages.append(_msg(0, "owner", {"owner": owner, "epoch": 1}))

    specs = [
        ("ok-x", 5, 50, ["ra/cal", "rb/ref"]),
        ("ok-y", 1, 60, ["ra/cal", "rb/ref"]),
        ("flow-p", 3, 55, ["fa/u", "fb/v"]),
        ("flow-q", 4, 56, ["fb/v"]),
        ("slow-u", 2, 90, ["sa/r", "sb/s"]),
        ("slow-v", 1, 95, ["sa/r", "sb/s"]),
    ]
    for tx, priority, deadline, resources in specs:
        messages.append(_msg(1, "request", {
            "tx": tx, "attempt": 1, "priority": priority,
            "deadline": deadline, "resources": resources,
        }))

    holds = [
        ("ra", "cal", "ok-x", "reversible"),
        ("rb", "ref", "ok-y", "reversible"),
        ("fa", "u", "flow-p", "reversible"),
        ("fb", "v", "flow-q", "reversible"),
        ("sa", "r", "slow-u", "reversible"),
        ("sb", "s", "slow-v", "reversible"),
    ]
    for owner, resource, tx, phase in holds:
        messages.append(_msg(2, "hold", {
            "owner": owner, "epoch": 1, "resource": resource,
            "tx": tx, "attempt": 1, "phase": phase,
        }))

    messages.extend([
        _msg(3, "wait", {"owner": "ra", "epoch": 1, "resource": "cal", "tx": "ok-y", "attempt": 1, "on": "ok-x"}),
        _msg(3, "wait", {"owner": "rb", "epoch": 1, "resource": "ref", "tx": "ok-x", "attempt": 1, "on": "ok-y"}),
        _msg(3, "wait", {"owner": "fb", "epoch": 1, "resource": "v", "tx": "flow-p", "attempt": 1, "on": "flow-q"}),
        _msg(3, "wait", {"owner": "sa", "epoch": 1, "resource": "r", "tx": "slow-v", "attempt": 1, "on": "slow-u"}),
        _msg(4, "owner", {"owner": "sb", "epoch": 2}),
        _msg(5, "wait", {"owner": "sb", "epoch": 1, "resource": "s", "tx": "slow-u", "attempt": 1, "on": "slow-v"}),
    ])
    for message in messages:
        cluster.handle(message)

    first_scan = cluster.handle(_msg(6, "scan", {}))
    cluster.advance(7)  # ra 已确认，rb 仍因慢网络未回执
    ok_after_fast_owner = cluster.coordinator.inspect("ok-x", at=7)
    cluster.advance(9)  # rb 的 released 回执到达，决定才真正完成
    cluster.handle(_msg(10, "hold", {
        "owner": "rb", "epoch": 1, "resource": "ref",
        "tx": "ok-x", "attempt": 1, "phase": "reversible",
    }))

    # 无环的 flow 组只是正常释放、授予，不做任何中止。
    cluster.handle(_msg(9, "release", {
        "owner": "fb", "epoch": 1, "tx": "flow-q", "attempt": 1, "result": "released",
    }))
    cluster.handle(_msg(10, "hold", {
        "owner": "fb", "epoch": 1, "resource": "v",
        "tx": "flow-p", "attempt": 1, "phase": "reversible",
    }))

    slow_scan = cluster.handle(_msg(12, "scan", {}))
    return {
        "first_scan": first_scan,
        "ok_after_fast_owner": ok_after_fast_owner,
        "inspections": {
            "resolved_ok_x": cluster.coordinator.inspect("ok-x", at=12),
            "continued_flow_p": cluster.coordinator.inspect("flow-p", at=12),
            "waiting_slow_u": cluster.coordinator.inspect("slow-u", at=12),
        },
        "slow_scan": slow_scan,
        "admin_sent_commands": {
            owner: len(admin.sent_commands) for owner, admin in cluster.admins.items()
        },
    }


def run_builtin_demo() -> dict[str, Any]:
    """运行一个由本地模拟管理员异步回执的小型死锁解除演示。"""
    cluster = LocalAdminCluster()
    cluster.register("da", epoch=1, release_delay=1)
    cluster.register("db", epoch=1, release_delay=3)

    messages = [
        {"at": 0, "kind": "owner", "record": {"owner": "da", "epoch": 1}},
        {"at": 0, "kind": "owner", "record": {"owner": "db", "epoch": 1}},
        {
            "at": 1,
            "kind": "request",
            "record": {"tx": "demo-x", "attempt": 1, "priority": 5, "deadline": 30,
                       "resources": ["da/cal", "db/ref"]},
        },
        {
            "at": 1,
            "kind": "request",
            "record": {"tx": "demo-y", "attempt": 1, "priority": 1, "deadline": 40,
                       "resources": ["da/cal", "db/ref"]},
        },
        {"at": 2, "kind": "hold", "record": {"owner": "da", "epoch": 1, "resource": "cal",
                                             "tx": "demo-x", "attempt": 1, "phase": "reversible"}},
        {"at": 2, "kind": "hold", "record": {"owner": "db", "epoch": 1, "resource": "ref",
                                             "tx": "demo-y", "attempt": 1, "phase": "reversible"}},
        {"at": 3, "kind": "wait", "record": {"owner": "da", "epoch": 1, "resource": "cal",
                                             "tx": "demo-y", "attempt": 1, "on": "demo-x"}},
        {"at": 3, "kind": "wait", "record": {"owner": "db", "epoch": 1, "resource": "ref",
                                             "tx": "demo-x", "attempt": 1, "on": "demo-y"}},
    ]
    for message in messages:
        cluster.handle(message)

    scan = cluster.handle({"at": 4, "kind": "scan", "record": {}})
    cluster.advance(5)
    first_scan_after_fast_ack = cluster.coordinator.inspect("demo-x", at=5)
    cluster.advance(7)
    # 模拟 db 释放后把 ref 授予幸存的 demo-x。
    cluster.handle(
        {"at": 8, "kind": "hold", "record": {"owner": "db", "epoch": 1, "resource": "ref",
                                              "tx": "demo-x", "attempt": 1, "phase": "reversible"}}
    )
    ready = cluster.coordinator.inspect("demo-x", at=8)
    return {
        "scan": scan,
        "inspect_after_fast_ack": first_scan_after_fast_ack,
        "ready_inspect": ready,
        "transcript_size": len(cluster.transcript),
    }
