"""等待图构造、环检测与解围对象选择。

本模块只包含无副作用的纯逻辑，方便单测直接调用。
轮次身份统一使用 ``tx#attempt``，避免旧申请轮次污染当前图。
"""

from __future__ import annotations

from typing import Any, Iterable

ROUND_SEP = "#"


def round_id(tx: str, attempt: int | str) -> str:
    return f"{tx}{ROUND_SEP}{attempt}"


def parse_round_id(value: str) -> tuple[str, int]:
    tx, raw_attempt = value.rsplit(ROUND_SEP, 1)
    return tx, int(raw_attempt)


def parse_resource_ref(ref: str) -> tuple[str, str]:
    """把 ``owner/resource`` 拆成管理员和资源名。"""
    if not isinstance(ref, str) or "/" not in ref:
        raise ValueError(f"资源标识应为 owner/resource 形式：{ref!r}")
    owner, resource = ref.split("/", 1)
    if not owner or not resource:
        raise ValueError(f"资源标识的管理员和资源名均不能为空：{ref!r}")
    return owner, resource


def build_wait_graph(state: dict[str, Any]) -> dict[str, Any]:
    """从持久状态构造当前有效等待边，同时保留无效证据及原因。

    返回 ``{"edges": [...], "evidence": [...]}``。只有管理员代次、
    持有者、申请轮次全部对得上的边才进入 ``edges``。
    """
    epochs = state.get("epochs", {})
    attempts = state.get("attempts", {})
    holds = state.get("holds", {})
    waits = state.get("waits", {})

    edges: list[dict[str, Any]] = []
    evidence: list[dict[str, Any]] = []

    for wait in waits.values():
        owner = wait["owner"]
        ref = wait["ref"]
        waiter = round_id(wait["tx"], wait["attempt"])
        current_epoch = epochs.get(owner)
        reasons: list[str] = []

        if current_epoch is None:
            reasons.append("owner_epoch_unknown")
        elif wait.get("epoch") != current_epoch:
            reasons.append("stale_epoch")

        waiter_state = attempts.get(waiter)
        if waiter_state is None:
            reasons.append("unknown_waiter_round")
        elif waiter_state.get("lifecycle") in {"aborted", "released", "superseded"}:
            reasons.append("waiter_round_ended")

        hold = holds.get(ref)
        holder_round: str | None = None
        if hold is None:
            reasons.append("holder_not_currently_reported")
        elif current_epoch is not None and hold.get("epoch") != current_epoch:
            reasons.append("stale_holder_epoch")
        elif hold.get("tx") != wait.get("on_tx"):
            reasons.append("holder_changed")
        else:
            holder_round = round_id(hold["tx"], hold["attempt"])
            holder_state = attempts.get(holder_round)
            if holder_state is None:
                reasons.append("unknown_holder_round")
            elif holder_state.get("lifecycle") in {"aborted", "released", "superseded"}:
                reasons.append("holder_round_ended")

        item = {
            "from": waiter,
            "to": holder_round,
            "tx": wait["tx"],
            "attempt": wait["attempt"],
            "owner": owner,
            "resource": wait["resource"],
            "ref": ref,
            "on_tx": wait.get("on_tx"),
            "on_attempt": hold.get("attempt") if hold and hold.get("tx") == wait.get("on_tx") else None,
            "epoch": wait.get("epoch"),
            "current_epoch": current_epoch,
            "first_seen_at": wait.get("first_seen_at"),
            "last_seen_at": wait.get("last_seen_at"),
            "valid": not reasons and holder_round is not None,
            "reasons": reasons,
        }
        evidence.append(item)
        if item["valid"] and holder_round is not None:
            edge = {k: item[k] for k in ("from", "to", "owner", "resource", "ref", "on_tx", "on_attempt")}
            edge["wait"] = item
            edges.append(edge)

    edges.sort(key=lambda e: (e["from"], e["to"], e["ref"]))
    evidence.sort(key=lambda e: (e["tx"], e["attempt"], e["ref"]))
    return {"edges": edges, "evidence": evidence}


def adjacency(edges: Iterable[dict[str, Any]]) -> dict[str, list[str]]:
    graph: dict[str, set[str]] = {}
    for edge in edges:
        graph.setdefault(edge["from"], set()).add(edge["to"])
        graph.setdefault(edge["to"], set())
    return {node: sorted(targets) for node, targets in sorted(graph.items())}


def find_cycles(edges: Iterable[dict[str, Any]] | dict[str, Any]) -> list[list[str]]:
    """枚举规范排序的简单环。

    每个环只保留以字典序最小节点为起点的一次表示，例如
    ``["x#1", "y#1", "x#1"]``。
    """
    if isinstance(edges, dict):
        edges = edges.get("edges", [])
    adj = adjacency(edges)
    nodes = sorted(adj)
    cycles: list[list[str]] = []
    seen_keys: set[tuple[str, ...]] = set()

    for start in nodes:
        stack = [start]
        in_stack = {start}

        def visit(node: str) -> None:
            for nxt in adj.get(node, []):
                # start 必须是环中字典序最小的节点，从而消除旋转重复。
                if nxt < start:
                    continue
                if nxt == start and len(stack) >= 1:
                    cycle = stack + [start]
                    key = tuple(cycle)
                    if key not in seen_keys:
                        seen_keys.add(key)
                        cycles.append(cycle)
                elif nxt >= start and nxt not in in_stack:
                    stack.append(nxt)
                    in_stack.add(nxt)
                    visit(nxt)
                    stack.pop()
                    in_stack.remove(nxt)

        visit(start)

    cycles.sort(key=lambda c: (len(c), c))
    return cycles


def cycle_nodes(cycle: list[str]) -> list[str]:
    if len(cycle) > 1 and cycle[0] == cycle[-1]:
        return list(dict.fromkeys(cycle[:-1]))
    return list(dict.fromkeys(cycle))


def edges_for_cycle(edges: list[dict[str, Any]], cycle: list[str]) -> list[dict[str, Any]]:
    positions = {node: i for i, node in enumerate(cycle)}
    result = []
    for edge in edges:
        u, v = edge["from"], edge["to"]
        if u in positions and v in positions:
            # 保留沿环方向以及 SCC 内可能存在的额外边。
            result.append(edge)
    return result


def default_victim_key(candidate: dict[str, Any]) -> tuple[Any, ...]:
    """默认解围对象排序键，元组越小越优先被中止。

    1. 只有全部持用均 reversible 的申请可自动中止；
    2. 业务等级低者优先（priority 数值越大越优先，所以升序先选小值）；
    3. 截止时间更晚、剩余时间更多者优先，保护临近截止的申请；
    4. 近期被选为解围对象次数更少者优先，避免反复牺牲同一单位；
    5. 身份字典序保证结果确定。
    """
    return (
        candidate["safety_rank"],
        candidate["priority"],
        -candidate["deadline"],
        candidate["rescue_count"],
        candidate["round"],
    )


def select_victim(
    cycle: list[str],
    state: dict[str, Any],
    *,
    now: int | None = None,
    recent_window: int | None = None,
    key: Any = None,
) -> dict[str, Any]:
    """返回环上每个候选的评估及默认选中对象。

    返回 ``{"victim": str|None, "candidates": [...], "reason": ...}``。
    存在 prepared 或阶段未知的候选不会被自动选中；若没有安全候选，
    ``victim`` 为 ``None``，调用方应转运维处理。
    """
    selector = key or default_victim_key
    rescue_history = state.get("rescue_history", {})
    candidates: list[dict[str, Any]] = []

    for node in cycle_nodes(cycle):
        tx, attempt = parse_round_id(node)
        item = state.get("attempts", {}).get(node)
        if item is None:
            continue
        held: list[dict[str, Any]] = []
        for hold in state.get("holds", {}).values():
            if hold.get("tx") == tx and hold.get("attempt") == attempt:
                held.append(hold)
        phases = sorted({hold.get("phase", "unknown") for hold in held})
        prepared = [hold["ref"] for hold in held if hold.get("phase") == "prepared"]
        uncertain = [hold["ref"] for hold in held if hold.get("phase") not in {"reversible", "prepared"}]

        history = rescue_history.get(tx, [])
        if recent_window is not None and now is not None:
            count = sum(1 for event in history if now - int(event.get("at", now)) <= recent_window)
        else:
            count = len(history)

        if prepared or uncertain:
            safety_rank = 2 if prepared else 1
            abortable = False
            block_reasons = []
            if prepared:
                block_reasons.append("prepared_irreversible_phase")
            if uncertain:
                block_reasons.append("unknown_rollback_phase")
        else:
            safety_rank = 0
            abortable = item.get("lifecycle") not in {"aborted", "released", "superseded"}
            block_reasons = [] if abortable else ["round_not_active"]

        candidates.append(
            {
                "round": node,
                "tx": tx,
                "attempt": attempt,
                "priority": int(item.get("priority", 0)),
                "deadline": int(item.get("deadline", 0)),
                "deadline_slack": None if now is None else int(item.get("deadline", 0)) - now,
                "rescue_count": count,
                "phases": phases,
                "held_resources": sorted(hold["ref"] for hold in held),
                "prepared_resources": sorted(prepared),
                "uncertain_resources": sorted(uncertain),
                "abortable": abortable,
                "safety_rank": safety_rank,
                "block_reasons": block_reasons,
            }
        )

    abortable = [c for c in candidates if c["abortable"]]
    victim = min(abortable, key=selector)["round"] if abortable else None
    if victim is None:
        reason = "no_safe_auto_abort_candidate"
    else:
        chosen = next(c for c in candidates if c["round"] == victim)
        reason = {
            "rule": "lower_priority_then_more_deadline_slack_then_fewer_recent_rescues",
            "selected": victim,
            "priority": chosen["priority"],
            "deadline": chosen["deadline"],
            "rescue_count": chosen["rescue_count"],
        }
    return {"victim": victim, "candidates": candidates, "reason": reason}
