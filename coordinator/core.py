"""分布式联合标定事务协调器核心。

设计要点：

* ``(tx, attempt)`` 才是一轮有效申请；
* 管理员 ``epoch`` 决定报告是否属于当前代次；
* 只有当前代次的 wait/hold 能组成真实等待边；
* 扫描只对真实环选择一个可安全中止对象，中止单不等于释放完成；
* 所有相关管理员确认后才结束旧轮次；prepared 晚报会转人工安全状态；
* 状态可 JSON 持久化，restart 通过快照往返模拟进程重启，决定不丢失。
"""

from __future__ import annotations

import json
import os
import tempfile
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable

from .graph import build_wait_graph, cycle_nodes as extract_cycle_nodes, find_cycles, parse_resource_ref, round_id, select_victim

KNOWN_KINDS = {
    "hold",
    "inspect",
    "owner",
    "release",
    "request",
    "restart",
    "scan",
    "wait",
    "safety_confirm",
    "case_resolve",
}

OPEN_DECISION_STATUSES = {
    "release_pending",
    "release_unknown",
    "manual_safety_verification_required",
}


class ProtocolError(ValueError):
    """输入消息缺少必要字段或轮次/身份冲突。"""


def _fresh_state() -> dict[str, Any]:
    return {
        "version": 1,
        "now": 0,
        "epochs": {},
        "epoch_history": {},
        "transactions": {},
        "attempts": {},
        "holds": {},
        "waits": {},
        "release_reports": {},
        "decisions": {},
        "decision_order": [],
        "latest_decision_by_round": {},
        "operator_cases": {},
        "operator_case_order": [],
        "rescue_history": {},
        "rejected_reports": [],
        "restart_count": 0,
        "restarts": [],
        "counters": {"decision": 0, "operator_case": 0},
    }


class Coordinator:
    """接收规范化消息并维护协调状态。

    典型用法::

        coordinator = Coordinator()
        coordinator.handle({"at": 1, "kind": "request", "record": {...}})
        report = coordinator.scan(at=4)
        view = coordinator.inspect("x")
    """

    def __init__(
        self,
        state_path: str | os.PathLike[str] | None = None,
        *,
        slow_threshold: int | None = 2,
        recent_window: int | None = None,
        victim_key: Callable[..., tuple[Any, ...]] | None = None,
        **options: Any,
    ) -> None:
        # 兼容不同调用方对同一配置的命名。
        if state_path is None:
            state_path = (
                options.pop("state_file", None)
                or options.pop("persist_path", None)
                or options.pop("snapshot_path", None)
                or options.pop("path", None)
            )
        if recent_window is None:
            recent_window = options.pop("rescue_window", None) or options.pop("recent_rescue_window", None)
        if slow_threshold == 2 and options.get("slow_after") is not None:
            slow_threshold = options.pop("slow_after")
        options.pop("durable", None)
        options.pop("persist", None)
        self.extra_options = options
        if state_path == ":memory:":
            state_path = None
        self.state_path = Path(state_path) if state_path is not None else None
        self.slow_threshold = slow_threshold
        self.recent_window = recent_window
        self.victim_key = victim_key
        if self.state_path is not None and self.state_path.exists():
            self.state = self._read_state(self.state_path)
        else:
            self.state = _fresh_state()
            self._save()

    # ------------------------------------------------------------------
    # 输入入口
    # ------------------------------------------------------------------
    def handle(
        self,
        message: dict[str, Any] | str | None = None,
        record: dict[str, Any] | None = None,
        *,
        kind: str | None = None,
        at: int | None = None,
    ) -> dict[str, Any]:
        """处理一条信封消息，也兼容 ``handle("scan", {...}, at=4)``。"""
        if isinstance(record, str) and kind is None:
            # 兼容 handle(record_dict, "request") 这种参数顺序。
            kind = record
            record = None
        envelope = self._normalize_message(message, record, kind=kind, at=at)
        kind = envelope["kind"]
        at = envelope["at"]
        record = envelope["record"]

        if kind == "request":
            result = self._receive_request(record, at)
        elif kind == "owner":
            result = self._receive_owner(record, at)
        elif kind == "hold":
            result = self._receive_hold(record, at)
        elif kind == "wait":
            result = self._receive_wait(record, at)
        elif kind == "release":
            result = self._receive_release(record, at)
        elif kind == "scan":
            result = self.scan(at=at, record=record)
        elif kind == "restart":
            result = self.restart(at=at, record=record)
        elif kind == "inspect":
            tx = record.get("tx")
            result = self.inspect(tx, attempt=record.get("attempt"), at=at)
        elif kind == "safety_confirm":
            result = self.confirm_manual_safety(
                decision_id=record.get("decision_id"),
                tx=record.get("tx"),
                attempt=record.get("attempt"),
                owner=record.get("owner"),
                confirmed_by=record.get("confirmed_by", record.get("operator", "responsible-owner")),
                isolation_ref=record.get("isolation_ref", record.get("isolated_by")),
                note=record.get("note"),
                at=at,
            )
        elif kind == "case_resolve":
            self._require_fields(record, ["case_id"])
            result = self.resolve_operator_case(
                record["case_id"],
                resolution=record.get("resolution", "resolved_by_operations"),
                resolved_by=record.get("resolved_by", "operations"),
                note=record.get("note"),
                at=at,
            )
        else:
            raise ProtocolError(f"未登记的消息名称：{kind}")

        result.setdefault("at", at)
        result.setdefault("kind", f"{kind}_result" if kind not in {"scan", "inspect"} else kind)
        self.state["now"] = at
        self._save()
        return result

    def receive(self, kind: str, record: dict[str, Any] | None = None, at: int | None = None) -> dict[str, Any]:
        return self.handle(kind=kind, record=record or {}, at=at)

    def _normalize_message(
        self,
        message: dict[str, Any] | str | None,
        record: dict[str, Any] | None,
        *,
        kind: str | None,
        at: int | None,
    ) -> dict[str, Any]:
        if isinstance(message, str):
            kind = message
            message = None
        if isinstance(message, (bytes, bytearray)):
            message = json.loads(message.decode("utf-8"))
        if isinstance(message, dict):
            if "kind" in message and "record" in message:
                envelope = message
                kind = envelope.get("kind")
                record = envelope.get("record")
                at = envelope.get("at", at)
            else:
                # 允许直接把 record 作为第一个参数。
                record = message
        if not kind:
            raise ProtocolError("消息缺少 kind")
        if record is None:
            record = {}
        if not isinstance(record, dict):
            raise ProtocolError("record 应为对象")
        if at is None:
            at = self.state.get("now", 0)
        if type(at) is not int or at < 0:
            raise ProtocolError("到达秒点必须为非负整数")
        return {"at": at, "kind": kind, "record": record}

    # ------------------------------------------------------------------
    # 持久化与重启
    # ------------------------------------------------------------------
    @staticmethod
    def _read_state(path: Path) -> dict[str, Any]:
        with path.open("r", encoding="utf-8") as source:
            state = json.load(source)
        # 简单兼容未来补字段。
        fresh = _fresh_state()
        fresh.update(state)
        return fresh

    def _save(self) -> None:
        if self.state_path is None:
            return
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        fd, temporary_name = tempfile.mkstemp(
            prefix=f".{self.state_path.name}.", suffix=".tmp", dir=str(self.state_path.parent)
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as target:
                json.dump(self.state, target, ensure_ascii=False, sort_keys=True, indent=2)
                target.flush()
                os.fsync(target.fileno())
            os.replace(temporary_name, self.state_path)
        except BaseException:
            try:
                os.unlink(temporary_name)
            except FileNotFoundError:
                pass
            raise

    def restart(self, at: int | None = None, record: dict[str, Any] | None = None) -> dict[str, Any]:
        """模拟协调端进程重启；已下发和未落实决定原样保留。"""
        at = self.state["now"] if at is None else at
        self.state["restart_count"] += 1
        self.state["restarts"].append({"at": at, "record": record or {}})
        self._save()

        if self.state_path is not None:
            self.state = self._read_state(self.state_path)
            recovery = "durable_snapshot_reloaded"
        else:
            # 即使没有显式状态文件，也经过完整 JSON 序列化边界，
            # 验证内存状态全部可持久化且重启后不丢决定。
            self.state = json.loads(json.dumps(self.state, ensure_ascii=False))
            recovery = "json_checkpoint_reloaded"

        open_decisions = [
            self._decision_summary(did)
            for did in self.state["decision_order"]
            if self.state["decisions"][did].get("open")
        ]
        return {
            "kind": "restart_result",
            "recovered": True,
            "recovery": recovery,
            "restart_count": self.state["restart_count"],
            "open_decision_ids": [item["decision_id"] for item in open_decisions],
            "open_decisions": open_decisions,
            "note_zh": "重启不改变已下发或仍未落实的决定；待确认释放单继续有效。",
        }

    # ------------------------------------------------------------------
    # 输入处理
    # ------------------------------------------------------------------
    def _require_fields(self, record: dict[str, Any], fields: list[str]) -> None:
        missing = [name for name in fields if name not in record]
        if missing:
            raise ProtocolError(f"记录缺少字段：{', '.join(missing)}")

    def _receive_request(self, record: dict[str, Any], at: int) -> dict[str, Any]:
        self._require_fields(record, ["tx", "attempt", "priority", "deadline", "resources"])
        tx = record["tx"]
        attempt = record["attempt"]
        priority = record["priority"]
        deadline = record["deadline"]
        resources = record["resources"]
        if not isinstance(tx, str) or not tx:
            raise ProtocolError("tx 应为非空字符串")
        if type(attempt) is not int or attempt < 1:
            raise ProtocolError("attempt 应为正整数")
        if type(priority) is not int:
            raise ProtocolError("priority 应为整数")
        if type(deadline) is not int:
            raise ProtocolError("deadline 应为整数秒点")
        if not isinstance(resources, list) or not resources or not all(isinstance(r, str) for r in resources):
            raise ProtocolError("resources 应为非空字符串数组")

        parsed = [parse_resource_ref(ref) for ref in resources]
        owners = list(dict.fromkeys(owner for owner, _ in parsed))
        refs = [f"{owner}/{resource}" for owner, resource in parsed]
        key = round_id(tx, attempt)
        signature = {
            "priority": priority,
            "deadline": deadline,
            "resources": refs,
            "owners": owners,
        }

        transaction = self.state["transactions"].setdefault(
            tx, {"tx": tx, "attempts": [], "current_attempt": attempt, "created_at": at}
        )
        existing = self.state["attempts"].get(key)
        if existing is not None:
            same = all(existing.get(field) == signature[field] for field in signature)
            if not same:
                raise ProtocolError(f"申请轮次 {key} 已存在，但提交内容不一致")
            return {"accepted": True, "duplicate": True, "round": key}

        previous_max = max(transaction["attempts"], default=0)
        if attempt < previous_max:
            raise ProtocolError(f"{key} 是旧轮次；当前最大轮次为 {tx}#{previous_max}")

        attempt_state = {
            "round": key,
            "tx": tx,
            "attempt": attempt,
            **signature,
            "received_at": at,
            "lifecycle": "active",
            "ended_at": None,
            "decision_ids": [],
            "events": [{"at": at, "event": "request_received", "round": key}],
        }
        self.state["attempts"][key] = attempt_state
        transaction["attempts"].append(attempt)
        transaction["current_attempt"] = attempt
        return {"accepted": True, "duplicate": False, "round": key, "owners": owners}

    def _ensure_epoch(self, owner: str, epoch: Any, at: int, source: str) -> int | None:
        current = self.state["epochs"].get(owner)
        if current is None and epoch is not None:
            # 对缺少独立 owner 消息的直接报告保持容错。
            self.state["epochs"][owner] = epoch
            self.state["epoch_history"].setdefault(owner, []).append(
                {"epoch": epoch, "at": at, "source": source, "implicit": True}
            )
            return epoch
        return current

    def _receive_owner(self, record: dict[str, Any], at: int) -> dict[str, Any]:
        self._require_fields(record, ["owner", "epoch"])
        owner = record["owner"]
        epoch = record["epoch"]
        if not isinstance(owner, str) or not owner or type(epoch) is not int:
            raise ProtocolError("owner/epoch 不合法")
        previous = self.state["epochs"].get(owner)
        self.state["epochs"][owner] = epoch
        self.state["epoch_history"].setdefault(owner, []).append({"epoch": epoch, "at": at})
        return {"accepted": True, "owner": owner, "epoch": epoch, "previous_epoch": previous}

    def _stale_report(self, kind: str, record: dict[str, Any], at: int, reason: str) -> dict[str, Any]:
        item = {"at": at, "kind": kind, "reason": reason, "record": record}
        self.state["rejected_reports"].append(item)
        return {"accepted": False, "stale": True, "reason": reason, "at": at}

    def _wait_key(self, tx: str, attempt: int, ref: str) -> str:
        return f"{round_id(tx, attempt)}@{ref}"

    def _receive_hold(self, record: dict[str, Any], at: int) -> dict[str, Any]:
        self._require_fields(record, ["owner", "epoch", "resource", "tx", "attempt"])
        owner = record["owner"]
        epoch = record["epoch"]
        resource = record["resource"]
        tx = record["tx"]
        attempt = record["attempt"]
        phase = record.get("phase", "unknown")
        current_epoch = self._ensure_epoch(owner, epoch, at, "hold")
        if epoch != current_epoch:
            return self._stale_report("hold", record, at, "stale_epoch")

        ref = f"{owner}/{resource}"
        old = self.state["holds"].get(ref)
        hold = {
            "ref": ref,
            "owner": owner,
            "resource": resource,
            "epoch": epoch,
            "tx": tx,
            "attempt": attempt,
            "round": round_id(tx, attempt),
            "phase": phase,
            "received_at": at,
            "updated_at": at,
        }
        if old is not None and (old.get("tx"), old.get("attempt"), old.get("phase")) != (tx, attempt, phase):
            hold["previous_holder"] = {"tx": old.get("tx"), "attempt": old.get("attempt"), "phase": old.get("phase")}
        self.state["holds"][ref] = hold

        # 拿到该资源后，同一轮次针对它的 wait 报告不再是当前等待。
        self.state["waits"].pop(self._wait_key(tx, attempt, ref), None)

        # 中止决定下发后若收到 prepared/未知阶段晚报，必须立即冻结在
        # 人工安全核实状态，不能等 release 回执才发现风险。
        decision_id = self.state["latest_decision_by_round"].get(hold["round"])
        decision = self.state["decisions"].get(decision_id) if decision_id else None
        if decision is not None and owner in decision.get("required_owners", []):
            self._refresh_decision(decision_id, at=at, trigger=f"hold_report_phase_{phase}")

        return {"accepted": True, "ref": ref, "holder": hold["round"], "phase": phase,
                "decision_id": decision_id,
                "decision_status": self.state["decisions"][decision_id]["status"] if decision_id else None}

    def _receive_wait(self, record: dict[str, Any], at: int) -> dict[str, Any]:
        self._require_fields(record, ["owner", "epoch", "resource", "tx", "attempt", "on"])
        owner = record["owner"]
        epoch = record["epoch"]
        resource = record["resource"]
        tx = record["tx"]
        attempt = record["attempt"]
        on_tx = record["on"]
        current_epoch = self._ensure_epoch(owner, epoch, at, "wait")
        if epoch != current_epoch:
            return self._stale_report("wait", record, at, "stale_epoch")

        ref = f"{owner}/{resource}"
        key = self._wait_key(tx, attempt, ref)
        old = self.state["waits"].get(key)
        first_seen = at
        if old is not None and old.get("on_tx") == on_tx:
            first_seen = old.get("first_seen_at", at)
        wait = {
            "key": key,
            "owner": owner,
            "resource": resource,
            "ref": ref,
            "epoch": epoch,
            "tx": tx,
            "attempt": attempt,
            "round": round_id(tx, attempt),
            "on_tx": on_tx,
            "first_seen_at": first_seen,
            "last_seen_at": at,
        }
        self.state["waits"][key] = wait
        return {"accepted": True, "waiter": wait["round"], "ref": ref, "on": on_tx}

    # ------------------------------------------------------------------
    # 释放回执与决定状态
    # ------------------------------------------------------------------
    def _receive_release(self, record: dict[str, Any], at: int) -> dict[str, Any]:
        self._require_fields(record, ["owner", "tx", "attempt", "result"])
        owner = record["owner"]
        tx = record["tx"]
        attempt = record["attempt"]
        result = record["result"]
        if result not in {"released", "unknown", "prepared"}:
            raise ProtocolError("release result 必须是 released、unknown 或 prepared")
        epoch = record.get("epoch", self.state["epochs"].get(owner))
        if epoch is not None:
            current_epoch = self._ensure_epoch(owner, epoch, at, "release")
        else:
            current_epoch = self.state["epochs"].get(owner)
        epoch_current = epoch is None or current_epoch is None or epoch == current_epoch
        key = round_id(tx, attempt)

        round_reports = self.state["release_reports"].setdefault(key, {})
        owner_report = round_reports.setdefault(owner, {"reports": []})
        report_view = {
            "at": at,
            "result": result,
            "epoch": epoch,
            "current_epoch": current_epoch,
            "epoch_current": epoch_current,
            "decision_id": record.get("decision_id"),
            "note": record.get("note"),
        }
        owner_report["reports"].append(report_view)
        owner_report.update({k: v for k, v in report_view.items() if k != "decision_id" or v})
        owner_report["latest_at"] = at

        if result == "released" and epoch_current:
            self._delete_round_resources(key, owner=owner)
        elif result == "prepared" and epoch_current:
            for hold in self.state["holds"].values():
                if hold.get("round") == key and hold.get("owner") == owner:
                    hold["phase"] = "prepared"
                    hold["updated_at"] = at

        decision_id = record.get("decision_id") or self.state["latest_decision_by_round"].get(key)
        decision = self.state["decisions"].get(decision_id) if decision_id else None
        if decision is not None and owner in decision.get("required_owners", []):
            self._refresh_decision(decision["id"], at=at, trigger=f"release_{result}")
            decision = self.state["decisions"][decision["id"]]
        elif result == "prepared":
            self._create_operator_case(
                case_type="unsafe_prepared_release",
                at=at,
                title=f"{key} 在 {owner} 处报告已完成准备",
                cycle=[key],
                conflict={
                    "round": key,
                    "owner": owner,
                    "release": report_view,
                    "instruction_zh": "自动中止不能继续；必须由该管理员/负责人确认隔离或介入。",
                },
            )

        return {
            "accepted": True,
            "round": key,
            "owner": owner,
            "result": result,
            "epoch_current": epoch_current,
            "decision_id": decision["id"] if decision is not None else None,
            "decision_status": decision["status"] if decision is not None else None,
        }

    def _delete_round_resources(self, key: str, *, owner: str | None = None) -> None:
        for ref in list(self.state["holds"]):
            hold = self.state["holds"][ref]
            if hold.get("round") == key and (owner is None or hold.get("owner") == owner):
                del self.state["holds"][ref]
        for wait_key in list(self.state["waits"]):
            wait = self.state["waits"][wait_key]
            if wait.get("round") == key and (owner is None or wait.get("owner") == owner):
                del self.state["waits"][wait_key]

    def _ledger_owner_state(self, round_key: str, owner: str) -> dict[str, Any]:
        report = self.state["release_reports"].get(round_key, {}).get(owner)
        if not report:
            return {"state": "pending", "agreed_release": False}
        result = report.get("result")
        epoch_current = report.get("epoch_current", True)
        safety = report.get("safety_confirmation")
        base = {
            "state": "pending",
            "latest_result": result,
            "latest_at": report.get("latest_at") or report.get("at"),
            "epoch": report.get("epoch"),
            "current_epoch": report.get("current_epoch"),
            "epoch_current": epoch_current,
            "agreed_release": False,
            "safety_confirmation": safety,
        }
        if result == "released":
            if epoch_current:
                base["state"] = "released"
                base["agreed_release"] = True
            else:
                base["state"] = "released_stale_epoch"
        elif result == "prepared":
            if safety:
                base["state"] = "manually_safety_confirmed"
                base["manually_cleared"] = True
            else:
                base["state"] = "prepared"
        elif result == "unknown":
            base["state"] = "unknown"
        return base

    # ------------------------------------------------------------------
    # 扫描、环和决定
    # ------------------------------------------------------------------
    def graph(self) -> dict[str, Any]:
        return build_wait_graph(self.state)

    def cycles(self) -> list[list[str]]:
        return find_cycles(self.graph()["edges"])

    # 常见命名别名，便于不同调用方/测试习惯。
    detect_cycles = cycles
    detect_deadlocks = cycles

    def add_message(self, message: dict[str, Any]) -> dict[str, Any]:
        return self.handle(message)

    process = handle

    def query(self, tx: str, attempt: int | None = None, at: int | None = None) -> dict[str, Any]:
        return self.inspect(tx, attempt=attempt, at=at)

    def restart_coordinator(self, at: int | None = None, record: dict[str, Any] | None = None):
        return self.restart(at=at, record=record)

    def scan_now(self, at: int | None = None, record: dict[str, Any] | None = None):
        return self.scan(at=at, record=record)

    def scan(self, at: int | None = None, record: dict[str, Any] | None = None) -> dict[str, Any]:
        if at is None:
            at = self.state.get("now", 0)
        self.state["now"] = at
        graph = build_wait_graph(self.state)
        cycles = find_cycles(graph["edges"])
        cycle_reports: list[dict[str, Any]] = []
        new_commands: list[dict[str, Any]] = []
        decisions: list[dict[str, Any]] = []
        cases: list[dict[str, Any]] = []

        for cycle in cycles:
            nodes = extract_cycle_nodes(cycle)
            cycle_key = "->".join(cycle)
            edges = [
                edge
                for edge in graph["edges"]
                if edge["from"] in nodes and edge["to"] in nodes
            ]

            existing_decision = self._find_open_decision_for_nodes(nodes)
            existing_case = self._find_open_case_for_cycle(cycle_key)
            if existing_decision is not None:
                summary = self._decision_summary(existing_decision["id"])
                cycle_reports.append(
                    {
                        "cycle": cycle,
                        "cycle_key": cycle_key,
                        "status": summary["status"],
                        "existing_decision": True,
                        "decision": summary,
                        "edges": edges,
                    }
                )
                decisions.append(summary)
                continue
            if existing_case is not None:
                cycle_reports.append(
                    {
                        "cycle": cycle,
                        "cycle_key": cycle_key,
                        "status": "operator_case_open",
                        "operator_case": self._case_summary(existing_case["id"]),
                        "edges": edges,
                    }
                )
                cases.append(self._case_summary(existing_case["id"]))
                continue

            selection = select_victim(
                cycle,
                self.state,
                now=at,
                recent_window=self.recent_window,
                key=self.victim_key,
            )
            if selection["victim"] is None:
                case = self._create_operator_case(
                    case_type="deadlock_without_safe_victim",
                    at=at,
                    title="真实等待环中没有可安全自动中止的申请",
                    cycle=cycle,
                    conflict={
                        "cycle": cycle,
                        "cycle_key": cycle_key,
                        "edges": edges,
                        "candidates": selection["candidates"],
                        "selection_reason": selection["reason"],
                        "instruction_zh": "请运维核对 prepared 资源的隔离，或由负责人介入后再处理具体冲突。",
                    },
                    cycle_key=cycle_key,
                )
                cases.append(self._case_summary(case["id"]))
                cycle_reports.append(
                    {
                        "cycle": cycle,
                        "cycle_key": cycle_key,
                        "status": "operator_required",
                        "operator_case": self._case_summary(case["id"]),
                        "selection": selection,
                        "edges": edges,
                    }
                )
                continue

            decision = self._create_decision(cycle, edges, selection, at)
            decisions.append(self._decision_summary(decision["id"]))
            new_commands.extend(decision["pending_commands"])
            cycle_reports.append(
                {
                    "cycle": cycle,
                    "cycle_key": cycle_key,
                    "status": decision["status"],
                    "decision": self._decision_summary(decision["id"]),
                    "selection": selection,
                    "edges": edges,
                }
            )

        waiting = self._waiting_summary(graph, at)
        status = self._overall_scan_status(cycle_reports)
        open_decision_summaries = [
            self._decision_summary(decision_id)
            for decision_id in self.state["decision_order"]
            if self.state["decisions"][decision_id].get("open")
        ]
        open_case_summaries = [
            self._case_summary(case_id)
            for case_id in self.state["operator_case_order"]
            if self.state["operator_cases"][case_id].get("status") == "open"
        ]
        # 环可能因某一个管理员先 released 而不再完整，但中止单在所有管理员
        # 确认前仍然有效；prepared 晚报导致的人工状态也必须在 scan 中可见。
        if open_decision_summaries:
            if any(d["status"] == "manual_safety_verification_required" for d in open_decision_summaries):
                status = "operator_required_for_some_cycle"
            elif not cycles and status == "no_cycle_continue_waiting":
                status = "release_pending_for_open_decision"
        victim_rounds = [item["victim_round"] for item in decisions]
        return {
            "kind": "scan",
            "at": at,
            "status": status,
            "deadlock_detected": bool(cycles),
            "cycle_count": len(cycles),
            "cycles": cycles,
            "edges": graph["edges"],
            "valid_edges": graph["edges"],
            "evidence": graph["evidence"],
            "victims": victim_rounds,
            "victim": victim_rounds[0] if len(victim_rounds) == 1 else None,
            "cycle_reports": cycle_reports,
            "decisions": decisions,
            "abort_decisions": decisions,
            "open_decisions": open_decision_summaries,
            "operator_cases": open_case_summaries,
            "cycle_operator_cases": cases,
            "open_operator_cases": open_case_summaries,
            "all_operator_cases": open_case_summaries,
            "commands": new_commands,
            "release_commands": new_commands,
            "actions": new_commands,
            "pending_release_commands": self._all_pending_commands(),
            "waiting": waiting["all"],
            "slow_waiters": waiting["slow"],
            "invalid_evidence": [item for item in graph["evidence"] if not item["valid"]],
            "rejected_reports": self.state.get("rejected_reports", []),
            "guidance_zh": self._scan_guidance(status, cycles),
        }

    def _overall_scan_status(self, reports: list[dict[str, Any]]) -> str:
        if not reports:
            return "no_cycle_continue_waiting"
        statuses = {item["status"] for item in reports}
        if statuses == {"release_pending"} or statuses <= {"release_pending", "existing_decision_waiting"}:
            return "cycle_release_pending"
        if any("manual" in s or s == "operator_required" for s in statuses):
            return "operator_required_for_some_cycle"
        if any(s in {"release_pending", "release_unknown"} for s in statuses):
            return "cycle_release_pending"
        return "cycle_action_taken"

    def _scan_guidance(self, status: str, cycles: list[list[str]]) -> str:
        if not cycles:
            return "未发现真实等待环；持续慢的申请继续等待并展示阻塞证据，不能仅凭超时抛弃。"
        if status == "no_cycle_continue_waiting":
            return ""
        return "已按安全策略处理真实等待环；中止决定需等所有相关管理员确认释放，prepared 晚报必须转人工安全核实。"

    def _waiting_summary(self, graph: dict[str, Any], at: int) -> dict[str, Any]:
        by_key: dict[str, dict[str, Any]] = {}
        for evidence in graph["evidence"]:
            age = None if evidence.get("first_seen_at") is None else max(0, at - evidence["first_seen_at"])
            slow = self.slow_threshold is not None and age is not None and age >= self.slow_threshold
            item = {
                "round": evidence["from"],
                "ref": evidence["ref"],
                "owner": evidence["owner"],
                "resource": evidence["resource"],
                "on_tx": evidence["on_tx"],
                "on_attempt": evidence.get("on_attempt"),
                "valid_edge": evidence["valid"],
                "invalid_reasons": evidence["reasons"],
                "wait_age": age,
                "slow": slow,
                "disposition": "continue_waiting_do_not_kill_on_timeout_alone",
            }
            by_key[f"{evidence['from']}@{evidence['ref']}"] = item

        # 被代次检查拒绝的 wait 仍然是“申请方在等”的线索，应展示，
        # 但不能进入真实等待图或作为杀请求/成环证据。
        for rejected in self.state.get("rejected_reports", []):
            if rejected.get("kind") != "wait":
                continue
            record = rejected.get("record", {})
            owner = record.get("owner")
            resource = record.get("resource")
            tx = record.get("tx")
            attempt = record.get("attempt")
            if not owner or not resource or tx is None or attempt is None:
                continue
            ref = f"{owner}/{resource}"
            round_key = round_id(tx, attempt)
            key = f"{round_key}@{ref}"
            received_at = rejected.get("at", at)
            age = max(0, at - received_at)
            by_key[key] = {
                "round": round_key,
                "ref": ref,
                "owner": owner,
                "resource": resource,
                "on_tx": record.get("on"),
                "on_attempt": None,
                "valid_edge": False,
                "invalid_reasons": [rejected.get("reason", "rejected_evidence")],
                "wait_age": age,
                "slow": self.slow_threshold is not None and age >= self.slow_threshold,
                "disposition": "show_wait_but_evidence_insufficient_no_timeout_kill",
            }

        all_waiting = sorted(by_key.values(), key=lambda x: (x["round"], x["ref"]))
        slow_waiters = [item for item in all_waiting if item["slow"]]
        return {"all": all_waiting, "slow": slow_waiters}

    def _find_open_decision_for_nodes(self, nodes: list[str]) -> dict[str, Any] | None:
        node_set = set(nodes)
        for decision_id in reversed(self.state["decision_order"]):
            decision = self.state["decisions"][decision_id]
            if not decision.get("open"):
                continue
            if decision.get("victim_round") in node_set:
                return decision
            if node_set.intersection(decision.get("cycle_nodes", [])):
                return decision
        return None

    def _find_open_case_for_cycle(self, cycle_key: str) -> dict[str, Any] | None:
        for case_id in reversed(self.state["operator_case_order"]):
            case = self.state["operator_cases"][case_id]
            if case.get("status") == "open" and case.get("cycle_key") == cycle_key:
                return case
        return None

    def _create_decision(
        self,
        cycle: list[str],
        edges: list[dict[str, Any]],
        selection: dict[str, Any],
        at: int,
    ) -> dict[str, Any]:
        victim = selection["victim"]
        assert victim is not None
        tx, attempt = victim.rsplit("#", 1)
        attempt_int = int(attempt)
        attempt_state = self.state["attempts"][victim]
        required_owners = list(attempt_state["owners"])

        self.state["counters"]["decision"] += 1
        decision_id = f"D-{self.state['counters']['decision']}"
        cycle_nodes = extract_cycle_nodes(cycle)

        owner_states = {
            owner: self._ledger_owner_state(victim, owner) for owner in required_owners
        }
        commands = []
        for owner in required_owners:
            state_name = owner_states[owner]["state"]
            if state_name not in {"released", "manually_safety_confirmed"}:
                epoch = self.state["epochs"].get(owner)
                owner_resources = [ref for ref in attempt_state["resources"] if ref.startswith(f"{owner}/")]
                commands.append(
                    {
                        "command": "release",
                        "decision_id": decision_id,
                        "idempotency_key": f"{decision_id}:{owner}:{victim}",
                        "owner": owner,
                        "epoch": epoch,
                        "tx": tx,
                        "attempt": attempt_int,
                        "round": victim,
                        "resources": owner_resources,
                        "issued_at": at,
                    }
                )

        decision = {
            "id": decision_id,
            "type": "abort_round",
            "victim_round": victim,
            "tx": tx,
            "attempt": attempt_int,
            "cycle": cycle,
            "cycle_nodes": cycle_nodes,
            "cycle_edges": [
                {k: edge[k] for k in ("from", "to", "owner", "resource", "ref") if k in edge}
                for edge in edges
            ],
            "selection": {
                "reason": selection["reason"],
                "candidates": selection["candidates"],
            },
            "required_owners": required_owners,
            "owner_states": owner_states,
            "release_commands": commands,
            "pending_commands": commands,
            "status": "release_pending",
            "open": True,
            "created_at": at,
            "updated_at": at,
            "completed_at": None,
            "freeze": {
                "rounds": cycle_nodes,
                "resource_refs": list(attempt_state["resources"]),
                "reason_zh": "所有相关管理员确认前，环上幸存申请不得开始使用本轮冻结资源。",
            },
            "history": [{"at": at, "event": "abort_decision_issued_but_not_released"}],
        }
        self.state["decisions"][decision_id] = decision
        self.state["decision_order"].append(decision_id)
        self.state["latest_decision_by_round"][victim] = decision_id
        attempt_state["decision_ids"].append(decision_id)
        attempt_state["lifecycle"] = "abort_pending"
        attempt_state["events"].append({"at": at, "event": "abort_decision_issued", "decision_id": decision_id})

        rescue_list = self.state["rescue_history"].setdefault(tx, [])
        rescue_list.append({"at": at, "decision_id": decision_id, "round": victim, "cycle": cycle})

        self._refresh_decision(decision_id, at=at, trigger="decision_created")
        return self.state["decisions"][decision_id]

    def _refresh_decision(self, decision_id: str, *, at: int, trigger: str, mutate_attempt: bool = True) -> None:
        decision = self.state["decisions"][decision_id]
        key = decision["victim_round"]
        for owner in decision["required_owners"]:
            owner_state = self._ledger_owner_state(key, owner)
            # hold 阶段晚报也是安全证据：如果当前 hold 已 prepared，
            # 即使尚未收到 release=prepared，也不能继续自动收尾。
            current_holds = [
                hold
                for hold in self.state.get("holds", {}).values()
                if hold.get("round") == key and hold.get("owner") == owner
            ]
            if owner_state.get("state") != "manually_safety_confirmed":
                if any(hold.get("phase") == "prepared" for hold in current_holds):
                    owner_state["state"] = "prepared"
                    owner_state["agreed_release"] = False
                    owner_state["hold_phase"] = "prepared"
                elif any(hold.get("phase") not in {"reversible", "prepared"} for hold in current_holds):
                    owner_state["state"] = "unknown_phase"
                    owner_state["agreed_release"] = False
                    owner_state["hold_phase"] = "unknown"
            decision["owner_states"][owner] = owner_state

        states = {owner: item.get("state") for owner, item in decision["owner_states"].items()}
        manually_cleared = {
            owner
            for owner, item in decision["owner_states"].items()
            if item.get("state") == "manually_safety_confirmed"
        }
        released = {owner for owner, state in states.items() if state == "released"}
        prepared = {owner for owner, state in states.items() if state == "prepared"}
        unknown = {owner for owner, state in states.items() if str(state).startswith("unknown")}
        valid_release_count = len(released | manually_cleared)

        old_status = decision.get("status")
        if prepared:
            new_status = "manual_safety_verification_required"
        elif valid_release_count == len(decision["required_owners"]):
            new_status = "aborted" if not manually_cleared else "aborted_after_manual_safety"
        elif unknown:
            new_status = "release_unknown"
        else:
            new_status = "release_pending"

        decision["status"] = new_status
        if prepared:
            decision["open"] = True
            decision["manual_reason"] = "prepared_irreversible_phase_reported"
            decision["instruction_zh"] = "同一管理员/负责人必须证实隔离安全；其他管理员的 released 不能代签。"
            attempt = self.state["attempts"].get(key)
            if attempt is not None:
                attempt["lifecycle"] = "manual_required"
        elif new_status.startswith("aborted"):
            decision["open"] = False
            decision["completed_at"] = at
            decision["manual_reason"] = None
            self._delete_round_resources(key)
            attempt = self.state["attempts"].get(key)
            if attempt is not None and mutate_attempt:
                attempt["lifecycle"] = "aborted"
                attempt["ended_at"] = at
        elif unknown:
            decision["open"] = True
            attempt = self.state["attempts"].get(key)
            if attempt is not None and mutate_attempt and attempt.get("lifecycle") != "manual_required":
                attempt["lifecycle"] = "abort_pending"
        else:
            decision["open"] = True
            attempt = self.state["attempts"].get(key)
            if attempt is not None and mutate_attempt and attempt.get("lifecycle") != "manual_required":
                attempt["lifecycle"] = "abort_pending"

        # 重新计算仍需发送/重投的命令。prepared/unknown_phase 已转入人工
        # 安全核实，不再把自动 release 命令当作可自行推进的待办。
        commandable_states = {"pending", "unknown", "released_stale_epoch"}
        decision["pending_commands"] = [
            command
            for command in decision.get("release_commands", [])
            if decision["owner_states"].get(command["owner"], {}).get("state") in commandable_states
        ]
        decision["updated_at"] = at
        if new_status != old_status:
            decision["history"].append(
                {"at": at, "event": f"status_{old_status}_to_{new_status}", "trigger": trigger, "owner_states": states}
            )
            attempt = self.state["attempts"].get(key)
            if attempt is not None:
                attempt["events"].append({"at": at, "event": new_status, "decision_id": decision_id, "trigger": trigger})
        else:
            decision["history"].append({"at": at, "event": trigger, "owner_states": states})

    def _all_pending_commands(self) -> list[dict[str, Any]]:
        commands: list[dict[str, Any]] = []
        for decision_id in self.state["decision_order"]:
            decision = self.state["decisions"][decision_id]
            if decision.get("open"):
                commands.extend(decision.get("pending_commands", []))
        return commands

    def confirm_manual_safety(
        self,
        *,
        decision_id: str | None = None,
        tx: str | None = None,
        attempt: int | None = None,
        owner: str | None = None,
        confirmed_by: str = "responsible-owner",
        isolation_ref: str | None = None,
        note: str | None = None,
        at: int | None = None,
    ) -> dict[str, Any]:
        """由 prepared 所属管理员/负责人证实已安全隔离。

        这是运维/负责人入口；其他管理员的释放同意不能调用它代签。
        """
        at = self.state["now"] if at is None else at
        if decision_id is None and tx is not None and attempt is not None:
            decision_id = self.state["latest_decision_by_round"].get(round_id(tx, attempt))
        if decision_id is None or decision_id not in self.state["decisions"]:
            raise ProtocolError("找不到需要人工安全确认的决定")
        decision = self.state["decisions"][decision_id]
        if owner is None:
            prepared_owners = [
                own for own, state in decision["owner_states"].items() if state.get("state") == "prepared"
            ]
            if len(prepared_owners) == 1:
                owner = prepared_owners[0]
        if owner not in decision.get("required_owners", []):
            raise ProtocolError("必须指定该决定涉及的管理员")
        ledger = self.state["release_reports"].setdefault(decision["victim_round"], {}).setdefault(owner, {})
        latest = ledger.setdefault("reports", [{}])[-1] if ledger.get("reports") else {}
        confirmation = {
            "confirmed_at": at,
            "confirmed_by": confirmed_by,
            "isolation_ref": isolation_ref,
            "note": note,
            "owner": owner,
        }
        ledger["safety_confirmation"] = confirmation
        if ledger.get("reports"):
            ledger["reports"][-1]["safety_confirmation"] = confirmation
        else:
            ledger["reports"].append({"result": "prepared", "at": at, "safety_confirmation": confirmation})
            ledger["result"] = "prepared"
        self._refresh_decision(decision_id, at=at, trigger="manual_safety_confirmed_by_same_owner")
        return {"accepted": True, "decision": self._decision_summary(decision_id)}

    def resolve_operator_case(
        self,
        case_id: str,
        *,
        resolution: str = "resolved_by_operations",
        resolved_by: str = "operations",
        note: str | None = None,
        at: int | None = None,
    ) -> dict[str, Any]:
        """运维处理完具体冲突后关闭案件。"""
        at = self.state["now"] if at is None else at
        case = self.state["operator_cases"].get(case_id)
        if case is None:
            raise ProtocolError(f"找不到运维案件：{case_id}")
        case["status"] = "resolved"
        case["resolved_at"] = at
        case["resolution"] = resolution
        case["resolved_by"] = resolved_by
        case["resolution_note"] = note
        case["updated_at"] = at
        return {"accepted": True, "case": self._case_summary(case_id)}

    # ------------------------------------------------------------------
    # 运维案件
    # ------------------------------------------------------------------
    def _create_operator_case(
        self,
        *,
        case_type: str,
        at: int,
        title: str,
        cycle: list[str],
        conflict: dict[str, Any],
        cycle_key: str | None = None,
    ) -> dict[str, Any]:
        self.state["counters"]["operator_case"] += 1
        case_id = f"O-{self.state['counters']['operator_case']}"
        case = {
            "id": case_id,
            "type": case_type,
            "title": title,
            "status": "open",
            "cycle": cycle,
            "cycle_key": cycle_key,
            "conflict": conflict,
            "created_at": at,
            "updated_at": at,
        }
        self.state["operator_cases"][case_id] = case
        self.state["operator_case_order"].append(case_id)
        return case

    def _case_summary(self, case_id: str) -> dict[str, Any]:
        case = self.state["operator_cases"][case_id]
        summary = {
            "case_id": case["id"],
            "type": case["type"],
            "status": case["status"],
            "title": case["title"],
            "cycle": case.get("cycle"),
            "cycle_key": case.get("cycle_key"),
            "created_at": case.get("created_at"),
            "conflict": case.get("conflict"),
        }
        return deepcopy(summary)

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------
    def inspect(self, tx: str | None, attempt: int | None = None, at: int | None = None) -> dict[str, Any]:
        if at is not None:
            self.state["now"] = at
        if not tx:
            raise ProtocolError("inspect 需要 tx")
        transaction = self.state["transactions"].get(tx)
        if transaction is None:
            return {
                "kind": "inspect",
                "found": False,
                "tx": tx,
                "status": "unknown_tx",
                "state": "unknown_tx",
                "can_start": False,
                "ready": False,
            }

        attempt = transaction["current_attempt"] if attempt is None else attempt
        key = round_id(tx, attempt)
        item = self.state["attempts"].get(key)
        if item is None:
            return {
                "kind": "inspect",
                "found": False,
                "tx": tx,
                "attempt": attempt,
                "status": "unknown_attempt",
                "state": "unknown_attempt",
                "available_attempts": transaction["attempts"],
                "can_start": False,
                "ready": False,
            }

        graph = build_wait_graph(self.state)
        cycles = find_cycles(graph["edges"])
        containing_cycles = [cycle for cycle in cycles if key in cycle]
        acquired: list[str] = []
        missing: list[dict[str, Any]] = []
        for ref in item["resources"]:
            hold = self.state["holds"].get(ref)
            if hold is not None and hold.get("round") == key and hold.get("epoch") == self.state["epochs"].get(hold["owner"]):
                acquired.append(ref)
            else:
                missing.append(
                    {
                        "ref": ref,
                        "owner": ref.split("/", 1)[0],
                        "resource": ref.split("/", 1)[1],
                        "current_holder": hold.get("round") if hold else None,
                        "holder_phase": hold.get("phase") if hold else None,
                        "holder_epoch": hold.get("epoch") if hold else None,
                        "current_owner_epoch": self.state["epochs"].get(ref.split("/", 1)[0]),
                    }
                )

        wait_evidence = [evidence for evidence in graph["evidence"] if evidence["from"] == key]
        for rejected in self.state.get("rejected_reports", []):
            record = rejected.get("record", {})
            if rejected.get("kind") != "wait" or record.get("tx") != tx or record.get("attempt") != attempt:
                continue
            owner = record.get("owner")
            resource = record.get("resource")
            wait_evidence.append(
                {
                    "from": key,
                    "to": None,
                    "tx": tx,
                    "attempt": attempt,
                    "owner": owner,
                    "resource": resource,
                    "ref": f"{owner}/{resource}" if owner and resource else None,
                    "on_tx": record.get("on"),
                    "on_attempt": None,
                    "epoch": record.get("epoch"),
                    "valid": False,
                    "reasons": [rejected.get("reason", "rejected_evidence")],
                    "received_at": rejected.get("at"),
                }
            )
        valid_edges = [edge for edge in graph["edges"] if edge["from"] == key]
        blockers = [
            {
                "round": edge["to"],
                "tx": edge.get("on_tx"),
                "attempt": edge.get("on_attempt"),
                "resource": edge.get("ref"),
                "owner": edge.get("owner"),
            }
            for edge in valid_edges
        ]
        own_decisions = [
            self._decision_summary(did)
            for did in item.get("decision_ids", [])
        ]
        blocking_decisions: list[dict[str, Any]] = []
        for decision_id in self.state["decision_order"]:
            decision = self.state["decisions"][decision_id]
            if not decision.get("open"):
                continue
            if key == decision.get("victim_round"):
                continue
            if key in decision.get("cycle_nodes", []):
                blocking_decisions.append(self._decision_summary(decision_id))

        operator_cases = [
            self._case_summary(case_id)
            for case_id in self.state["operator_case_order"]
            if self.state["operator_cases"][case_id].get("status") == "open"
            and key in self.state["operator_cases"][case_id].get("cycle", [])
        ]

        why: list[str] = []
        lifecycle = item.get("lifecycle", "active")
        state_name = "active"
        can_start = False

        if lifecycle == "aborted":
            state_name = "aborted_round_ended_reapply_with_new_attempt"
            why.append("本轮已经结束；如需继续必须以新的 attempt 重新申请，旧释放确认不能用于新轮。")
        elif lifecycle == "manual_required" or any(d["status"] == "manual_safety_verification_required" for d in own_decisions):
            state_name = "manual_safety_verification_required"
            why.append("某管理员报告 prepared/已完成不可逆准备；需该管理员或负责人证实隔离安全，其他管理员同意不能代签。")
        elif own_decisions and own_decisions[-1].get("open"):
            state_name = "abort_release_pending"
            why.append("中止决定已下发但不等于释放完成；需等待所有涉及管理员确认。")
        elif operator_cases:
            state_name = "operator_intervention_required"
            why.append("真实冲突没有安全自动解围对象，已将具体环和资源交给运维。")
        elif blocking_decisions:
            manual_block = any(d["status"] == "manual_safety_verification_required" for d in blocking_decisions)
            state_name = "blocked_manual_safety" if manual_block else "blocked_frozen_waiting_all_release_confirmations"
            why.append("幸存申请涉及本轮冻结资源；所有相关管理员确认释放前不能开始。")
        elif missing:
            state_name = "waiting_for_resources"
            why.append("尚未由当前代次 hold 报告证明拿齐全部资源；继续等待真实管理员状态，不能仅凭超时中止。")
            if any(not evidence["valid"] for evidence in wait_evidence):
                why.append("部分等待证据是旧代次或持有者已变化，不能据此认定死锁环。")
        elif containing_cycles:
            state_name = "deadlock_detected_scan_required"
            why.append("当前证据已成环；等待值班方 scan 触发安全解围。")
        else:
            state_name = "ready_all_resources_held"
            can_start = True
            why.append("当前代次 hold 证明资源已拿齐，且没有未完成冻结/人工冲突。")

        if at is not None and at > item["deadline"]:
            why.append(f"已过到期时间 deadline={item['deadline']}；系统未因此直接抛弃或释放，需业务/负责人决定是否延期。")

        release_confirmation = None
        relevant = None
        if own_decisions:
            relevant = own_decisions[-1]
        elif blocking_decisions:
            relevant = blocking_decisions[0]
        if relevant:
            release_confirmation = relevant.get("release_confirmation")

        return {
            "kind": "inspect",
            "at": at if at is not None else self.state.get("now"),
            "found": True,
            "tx": tx,
            "attempt": attempt,
            "round": key,
            "status": self._simple_inspect_status(state_name),
            "simple_status": self._simple_inspect_status(state_name),
            "detailed_status": state_name,
            "state": state_name,
            "state_zh": self._state_zh(state_name),
            "can_start": can_start,
            "ready": can_start,
            "all_resources_held": not missing,
            "acquired": acquired,
            "acquired_resources": acquired,
            "missing": [item["ref"] for item in missing],
            "missing_resources": missing,
            "priority": item["priority"],
            "deadline": item["deadline"],
            "deadline_exceeded": at is not None and at > item["deadline"],
            "wait_evidence": wait_evidence,
            "valid_wait_edges": valid_edges,
            "blockers": blockers,
            "in_cycles": containing_cycles,
            "own_decisions": own_decisions,
            "blocking_decisions": blocking_decisions,
            "operator_cases": operator_cases,
            "release_confirmation": release_confirmation,
            "why_not": why if not can_start else [],
            "guidance_zh": "查询反映当前这一轮是否真正拿齐资源、阻塞原因，以及各管理员释放确认状态。",
            "round_history": [
                {
                    "attempt": past_attempt,
                    "round": round_id(tx, past_attempt),
                    "lifecycle": self.state["attempts"].get(round_id(tx, past_attempt), {}).get("lifecycle"),
                    "decision_ids": self.state["attempts"].get(round_id(tx, past_attempt), {}).get("decision_ids", []),
                    "ended_at": self.state["attempts"].get(round_id(tx, past_attempt), {}).get("ended_at"),
                }
                for past_attempt in transaction["attempts"]
            ],
        }

    def _simple_inspect_status(self, state: str) -> str:
        if state.startswith("ready"):
            return "ready"
        if state.startswith("waiting"):
            return "waiting"
        if state == "abort_release_pending":
            return "abort_pending"
        if state.startswith("blocked_manual") or state == "manual_safety_verification_required":
            return "manual_confirmation_required"
        if state == "operator_intervention_required":
            return "operator_required"
        if state.startswith("aborted"):
            return "aborted"
        if state.startswith("deadlock"):
            return "deadlock_detected"
        if state.startswith("blocked"):
            return "blocked"
        if state.startswith("unknown"):
            return "unknown"
        return state

    def _state_zh(self, state: str) -> str:
        mapping = {
            "ready_all_resources_held": "已拿齐资源，可以开始",
            "waiting_for_resources": "等待资源中；无真实环时继续等待",
            "abort_release_pending": "已决定中止，等待所有管理员确认释放",
            "blocked_frozen_waiting_all_release_confirmations": "本轮资源冻结中，等待全部释放确认",
            "blocked_manual_safety": "因 prepared 晚报进入人工安全核实，幸存申请继续冻结",
            "manual_safety_verification_required": "待人工证实 prepared 资源隔离安全",
            "operator_intervention_required": "无安全自动中止对象，需运维/负责人介入",
            "aborted_round_ended_reapply_with_new_attempt": "旧轮已结束，需以新 attempt 重新申请",
            "deadlock_detected_scan_required": "已检测到等待环，等待 scan 解围",
        }
        return mapping.get(state, state)

    def _decision_summary(self, decision_id: str) -> dict[str, Any]:
        d = self.state["decisions"][decision_id]
        confirmation = self._release_confirmation(d)
        summary = {
            "decision_id": d["id"],
            "type": d["type"],
            "status": d["status"],
            "open": d["open"],
            "resolved": not d["open"],
            "victim_round": d["victim_round"],
            "tx": d["tx"],
            "attempt": d["attempt"],
            "cycle": d["cycle"],
            "cycle_nodes": d["cycle_nodes"],
            "required_owners": d["required_owners"],
            "owner_states": d["owner_states"],
            "release_confirmation": confirmation,
            "pending_commands": d.get("pending_commands", []),
            "created_at": d.get("created_at"),
            "updated_at": d.get("updated_at"),
            "completed_at": d.get("completed_at"),
            "manual_reason": d.get("manual_reason"),
            "instruction_zh": d.get("instruction_zh"),
            "freeze": d.get("freeze"),
            "history": d.get("history", []),
        }
        return deepcopy(summary)

    def _release_confirmation(self, decision: dict[str, Any]) -> dict[str, Any]:
        owner_states = decision.get("owner_states", {})
        agreed = [o for o, s in owner_states.items() if s.get("state") == "released"]
        manually = [o for o, s in owner_states.items() if s.get("state") == "manually_safety_confirmed"]
        prepared = [o for o, s in owner_states.items() if s.get("state") == "prepared"]
        unknown = [o for o, s in owner_states.items() if str(s.get("state", "")).startswith("unknown")]
        stale = [o for o, s in owner_states.items() if str(s.get("state", "")).startswith("released_stale")]
        pending = [
            o
            for o, s in owner_states.items()
            if s.get("state") not in {"released", "manually_safety_confirmed", "prepared"}
        ]
        awaiting = sorted(set(pending + unknown + stale))
        return {
            "decision_id": decision["id"],
            "status": decision.get("status"),
            "all_confirmed": not decision.get("open", True),
            "required_owners": decision["required_owners"],
            "agreed_release_owners": sorted(agreed),
            "released_owners": sorted(agreed),
            "confirmed_owners": sorted(agreed),
            "manually_safety_confirmed_owners": sorted(manually),
            "prepared_owners": sorted(prepared),
            "unknown_owners": sorted(unknown),
            "stale_release_owners": sorted(stale),
            "pending_owners": sorted(pending),
            "awaiting_owners": awaiting,
            "waiting_owners": awaiting,
            "owner_states": owner_states,
            "note_zh": "只有同一管理员的有效 released 或其负责人的安全确认才能消除该管理员的阻塞；其他管理员不能代签。",
        }

    def state_snapshot(self) -> dict[str, Any]:
        return json.loads(json.dumps(self.state, ensure_ascii=False))


def process_messages(messages: list[dict[str, Any]], **coordinator_kwargs: Any) -> tuple[Coordinator, list[dict[str, Any]]]:
    """便捷批量入口，返回协调器和每条消息的响应。"""
    coordinator = Coordinator(**coordinator_kwargs)
    responses = [coordinator.handle(message) for message in messages]
    return coordinator, responses
