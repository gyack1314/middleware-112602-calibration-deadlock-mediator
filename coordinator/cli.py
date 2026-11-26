"""命令行/标准输入入口：反复接收 JSON 消息并输出协调结果。"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Iterable

from .core import Coordinator
from .simulator import run_builtin_demo, run_parallel_demo


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as source:
        for line_number, raw in enumerate(source, 1):
            stripped = raw.strip()
            if not stripped:
                continue
            try:
                row = json.loads(stripped)
            except json.JSONDecodeError as error:
                raise ValueError(f"{path.name} 第 {line_number} 行不是合法 JSON：{error}") from error
            if not isinstance(row, dict):
                raise ValueError(f"{path.name} 第 {line_number} 行应为 JSON 对象")
            rows.append(row)
    return rows


def iter_scenario_files(paths: list[Path]) -> Iterable[Path]:
    for path in paths:
        if path.is_dir():
            yield from sorted(path.glob("*.jsonl"))
        else:
            yield path


def replay_file(
    path: Path,
    *,
    state_path: Path | None = None,
    slow_threshold: int | None = 2,
) -> tuple[Coordinator, list[dict[str, Any]]]:
    """重放一个 JSONL 场景。

    若提供 ``state_path``，协调器在每条消息后原子落盘；场景中的 restart
    会从该快照真正重新加载。
    """
    coordinator = Coordinator(state_path=state_path, slow_threshold=slow_threshold)
    responses: list[dict[str, Any]] = []
    for message in read_jsonl(path):
        responses.append(coordinator.handle(message))
    return coordinator, responses


def _print_json(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, sort_keys=True))


def _text_line(response: dict[str, Any]) -> str:
    kind = response.get("kind")
    at = response.get("at")
    if kind == "scan":
        open_decisions = response.get("open_decisions", [])
        if not response.get("deadlock_detected") and not open_decisions:
            slow = len(response.get("slow_waiters", []))
            return f"[t={at}] scan：无真实等待环；持续慢等待 {slow} 个，继续展示等待而不抛弃。"
        bits = [f"[t={at}] scan：发现 {response.get('cycle_count')} 个当前真实环"]
        if not response.get("deadlock_detected"):
            bits = [f"[t={at}] scan：当前无完整真实环，但有 {len(open_decisions)} 个未落实/人工决定"]
        decisions = response.get("decisions", [])
        if not decisions:
            decisions = open_decisions
        cases = response.get("operator_cases", [])
        for decision in decisions:
            conf = decision["release_confirmation"]
            bits.append(
                "决定中止 "
                + decision["victim_round"]
                + f"（{decision['decision_id']}/{decision['status']}），已同意="
                + ",".join(conf["agreed_release_owners"])
                + "，待确认="
                + ",".join(conf["awaiting_owners"])
            )
        for case in cases:
            bits.append(f"运维案件 {case['case_id']}：{case['title']}")
        return "；".join(bits)
    if kind == "inspect":
        if not response.get("found"):
            return f"[t={at}] inspect {response.get('tx')}：{response.get('state')}"
        conf = response.get("release_confirmation")
        suffix = ""
        if conf:
            suffix = (
                f"；释放确认 {conf['decision_id']} status={conf.get('status')} "
                f"agreed={conf['agreed_release_owners']} prepared={conf['prepared_owners']} "
                f"awaiting={conf['awaiting_owners']}"
            )
        return (
            f"[t={at}] inspect {response.get('round')}：{response.get('state_zh')} "
            f"can_start={response.get('can_start')} acquired={response.get('acquired_resources')} "
            f"missing={[item['ref'] for item in response.get('missing_resources', [])]}{suffix}"
        )
    if kind == "restart_result":
        return f"[t={at}] restart：{response.get('recovery')}；未落实决定 {response.get('open_decision_ids')}"
    if kind == "release_result":
        return (
            f"[t={at}] release {response.get('round')}@{response.get('owner')}="
            f"{response.get('result')} decision={response.get('decision_id')}/{response.get('decision_status')}"
        )
    return f"[t={at}] {kind}: accepted={response.get('accepted', True)}"


def main(argv: list[str] | None = None) -> int:
    if argv is None:
        argv = sys.argv[1:]
    # 兼容常见的 ``python main.py replay <path>`` 调用方式。
    if argv and argv[0] in {"replay", "feed", "process", "run"}:
        argv = argv[1:]
    if argv and argv[0] == "serve":
        from .server import main as server_main

        return server_main(argv[1:])

    parser = argparse.ArgumentParser(description="分布式联合标定事务等待环协调中间件")
    parser.add_argument("paths", nargs="*", type=Path, help="JSONL 文件或包含 JSONL 的目录；为空时读取 stdin")
    parser.add_argument("--state", type=Path, default=None, help="持久化快照路径；restart 时从磁盘恢复")
    parser.add_argument("--format", choices=["jsonl", "text"], default="jsonl", help="输出格式")
    parser.add_argument("--slow-threshold", type=int, default=2, help="等待多少秒点标记为 slow；不影响安全策略")
    parser.add_argument("--demo", action="store_true", help="运行本地模拟管理员异步回执演示")
    parser.add_argument("--parallel-demo", action="store_true", help="运行三组并行联调差异演示：解除、继续、证据不足等待")
    args = parser.parse_args(argv)

    if args.parallel_demo:
        demo = run_parallel_demo()
        if args.format == "text":
            print(_text_line(demo["first_scan"]))
            print(_text_line(demo["ok_after_fast_owner"]))
            for key, view in demo["inspections"].items():
                print(f"[{key}] " + _text_line(view))
        else:
            _print_json(demo)
        return 0

    if args.demo:
        demo = run_builtin_demo()
        if args.format == "text":
            print(_text_line(demo["scan"]))
            print(_text_line(demo["inspect_after_fast_ack"]))
            print(_text_line(demo["ready_inspect"]))
        else:
            _print_json(demo)
        return 0

    if not args.paths:
        coordinator = Coordinator(state_path=args.state, slow_threshold=args.slow_threshold)
        for line in sys.stdin:
            if not line.strip():
                continue
            message = json.loads(line)
            response = coordinator.handle(message)
            if args.format == "text":
                print(_text_line(response))
            else:
                _print_json(response)
        return 0

    for path in iter_scenario_files(args.paths):
        coordinator, responses = replay_file(path, state_path=args.state, slow_threshold=args.slow_threshold)
        if args.format == "text":
            print(f"# {path}")
            for response in responses:
                if response.get("kind") in {"scan", "inspect", "restart_result", "release_result"}:
                    print(_text_line(response))
        else:
            for response in responses:
                _print_json(response)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
