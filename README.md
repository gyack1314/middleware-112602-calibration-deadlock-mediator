# 联合标定事务的等待成环检测与安全解围

多个训练单位并行申请工位、校准样本和参考版本时，同一联调必须拿齐多个管理员控制的资源。本仓库提供一个仅依赖 Python 3.11 标准库的分布式事务协调中间件：

- 用 `tx + attempt` 标识一轮有效申请，重新申请产生新身份并保留历史；
- 用管理员当前 `epoch` 过滤旧代次报告；
- 只把“当前代次 wait + 当前代次 hold + 同一未结束轮次”绑定成真实等待边；
- 枚举真实等待环，并按业务优先级、可回滚阶段、截止时间和近期解围次数选择一个安全中止对象；
- `prepared` 资源受保护；没有安全对象时生成具体运维案件；
- 中止决定不是释放完成：所有相关管理员确认前，冻结轮的幸存申请不能开始；
- `prepared` 晚报会把自动中止转为“待人工证实安全”，其他管理员的同意不能代签；
- 支持 JSON 快照持久化和 restart 恢复，决定 ID/状态不变；
- inspect 可查询本轮是否拿齐资源、阻塞原因、各管理员释放确认状态和历史轮次。

## 输入消息

每个 JSONL 文件是一段独立输入记录。每行包括整数秒点 `at`、消息名称 `kind` 与对象 `record`。`at` 代表中间件收到消息的时刻，同一秒内按行序提交。业务时间和对应回执引用写在 `record` 中。

- **request**　提交 `tx/attempt`、`priority`、`deadline` 和 `resources`。`priority` 数值越大越优先。
- **owner**　管理员从此刻使用 `epoch`。
- **hold**　管理员回报持有者及 `phase`，phase 为 `reversible` 或 `prepared`；携带 `epoch`。
- **wait**　管理员表明指定 `tx/attempt` 当前在等本服务的 resource 和持有者。`on` 为同一资源当前所等持有者的 tx，具体轮次按对应 hold 绑定。
- **scan**　值班方触发按当前真实证据的解围判断。
- **release**　管理员回报止用结果 `released`、`unknown` 或 `prepared`。
- **restart**　协调器重启。
- **inspect**　按 tx 查询；可在 record 中指定 `attempt`。

资源标识使用 `owner/resource`，例如 `a/r`、`b/s`。

## 快速运行

```bash
# 原有资料格式检查
python3 scenario_reader.py examples
python3 -m unittest discover -v

# 文本方式重放两个契约示例
python3 main.py examples/a-victim-is-not-release.jsonl --format text
python3 main.py examples/stale-wait-edges.jsonl --format text

# 三组并行联调：一组破环后继续、一组无环推进、一组证据不足仍等待
python3 main.py examples/parallel-groups.jsonl --format text

# 真实经过 LocalAdminCluster 下发 release 命令、模拟管理员异步回执
python3 main.py --parallel-demo --format text
python3 main.py --demo --format text

# JSONL 入口：每行输入一条消息，每行输出一个结果；也可接管道
cat examples/stale-wait-edges.jsonl | python3 main.py

# 真正落盘的重启恢复快照
python3 main.py examples/a-victim-is-not-release.jsonl --state /tmp/coordinator-state.json --format jsonl
```

也可以运行模块入口：

```bash
python3 -m coordinator examples/parallel-groups.jsonl --format text
```

可选本机 HTTP 入口：

```bash
python3 -m coordinator.server --host 127.0.0.1 --port 8765
curl -sS 'http://127.0.0.1:8765/inspect?tx=ok-x'
```

## 作为中间件 API 使用

```python
from coordinator import Coordinator

c = Coordinator(state_path="/tmp/coordinator-state.json")  # 不传则使用可 JSON 序列化的内存快照
c.handle({"at": 1, "kind": "request", "record": {
    "tx": "x",
    "attempt": 1,
    "priority": 5,
    "deadline": 20,
    "resources": ["a/r", "b/s"],
}})
# 继续 handle owner/hold/wait ...

scan = c.scan(at=4)
print(scan["deadlock_detected"], scan["cycles"], scan["victim"])

view = c.inspect("x", at=7)
print(view["can_start"], view["missing_resources"], view["why_not"])
print(view["release_confirmation"])

c.restart(at=8)       # 无 state_path 时也会做 JSON checkpoint 往返
```

兼容入口/别名包括：

- `Coordinator`、`DeadlockCoordinator`、`TransactionCoordinator`、`Middleware`；
- `from coordinator.core import ...`；
- `from coordinator.coordinator import Coordinator`；
- `from deadlock_coordinator import Coordinator`；
- `from middleware import DistributedTransactionMiddleware`；
- `Coordinator.handle/receive/add_message/process`、`scan/scan_now/detect_cycles`、`inspect/query`、`restart/restart_coordinator`。

## scan/inspect 如何表达结果

### 真实环与旧证据

协调器不会只看到两条 wait 文本就判定死锁。一条边有效必须满足：

1. wait 的 `epoch` 等于该 owner 当前 `epoch`；
2. waiter 的 `(tx, attempt)` 存在且尚未结束；
3. 同一 `owner/resource` 存在当前代次 hold；
4. hold 的 tx 等于 wait 的 `on`，具体 attempt 绑定到该 hold；
5. holder 轮次也尚未结束。

旧代次 wait 会进入 `rejected_reports` 和 inspect 的 `wait_evidence`，原因通常是 `stale_epoch`；当前代次但持有者已变化/释放的证据会标记 `holder_changed`、`holder_not_currently_reported` 等。它们会被展示为等待线索，但不会进入等待环，也不会导致自动中止。

### 安全 victim 策略

环上候选先经过硬安全检查：

- 任一当前 hold 为 `prepared`：不自动中止；
- phase 未知：不自动中止；
- 轮次已结束/不活动：不作为候选。

可中止候选默认按以下顺序确定优先 victim：

1. 全部资源仍为 `reversible`；
2. `priority` 数值更小（业务等级更低）；
3. deadline 更晚/剩余 slack 更多，优先保护临近截止的申请；
4. 该 tx 近期被选为解围对象的次数更少；
5. 身份字典序，保证结果确定。

如果环上没有安全对象，scan 不创建 abort 决定，而是创建 `O-n` 运维案件，包含完整环、候选阶段、prepared 资源和处理建议。重复 scan 不会重复创建同一环的未结案件或决定。多个独立 SCC/环分别处理，一个转人工不影响另一个自动破环，也不影响无关申请。

### 决定、释放确认与人工安全

scan 创建 `D-n` 后状态为 `release_pending`，并为 victim 申请中的每个资源管理员生成幂等 release 命令：

```json
{
  "command": "release",
  "decision_id": "D-1",
  "idempotency_key": "D-1:a:y#1",
  "owner": "a",
  "epoch": 1,
  "tx": "y",
  "attempt": 1,
  "resources": ["a/r"]
}
```

决定状态包括：

- `release_pending`：仍有管理员未确认；
- `release_unknown`：某管理员回报 unknown，不能视为释放；
- `manual_safety_verification_required`：出现 prepared 晚报或安全冲突；
- `aborted` / `aborted_after_manual_safety`：所有必需管理员均已确认。

`release_confirmation` 中可查看：

- `required_owners`
- `agreed_release_owners` / `released_owners` / `confirmed_owners`
- `prepared_owners`
- `unknown_owners`
- `stale_release_owners`
- `pending_owners` / `awaiting_owners` / `waiting_owners`

只有同一 prepared owner/负责人的显式安全确认才能消除该 owner 的阻塞。API 入口为：

```python
c.confirm_manual_safety(
    decision_id="D-1",
    owner="b",
    confirmed_by="owner-b-oncall",
    isolation_ref="sandbox-42",
    at=11,
)
```

### inspect 状态

常见 `state`/`status`：

- `ready_all_resources_held`：当前代次 hold 证明资源拿齐且无冻结；
- `waiting_for_resources`：无环或证据不足，继续等待；
- `abort_release_pending`：本轮的中止决定等待全部释放确认；
- `blocked_frozen_waiting_all_release_confirmations`：幸存方被冻结；
- `manual_safety_verification_required` / `blocked_manual_safety`：prepared 需人工安全核实；
- `operator_intervention_required`：无安全 victim；
- `aborted_round_ended_reapply_with_new_attempt`：旧轮结束，需新 attempt；
- `deadlock_detected_scan_required`：inspect 只读发现环，尚未 scan。

返回中同时有 `can_start`、`ready`、`acquired`、`missing`、`why_not`、`wait_evidence`、`in_cycles`、`own_decisions`、`blocking_decisions` 和 `round_history`。

## 本地模拟管理员

`coordinator.simulator.LocalResourceAdmin` 与 `LocalAdminCluster` 会：

1. 接收 coordinator 输出的 release 命令；
2. 按管理员自己的延迟安排 release 回执，模拟慢网络；
3. 根据本地 hold phase 或 `late_prepared` 脚本返回 `released` / `prepared`；
4. 把回执重新交给 coordinator。

`python main.py --parallel-demo --format text` 中可观察到：

- `ok-x/ok-y`：真实环，低优先级且 reversible 的 `ok-y#1` 被选为 victim；慢管理员 `rb` 未确认前 `ok-x` 不能开始，确认并授予 hold 后 `ok-x` ready；
- `flow-p/flow-q`：只有单向等待，`flow-q` 正常释放后 `flow-p` 拿齐资源继续；
- `slow-u/slow-v`：反向 wait 使用旧 epoch，不构成真实环；scan/inspect 持续展示等待和 stale 原因，不杀使用方。

`examples/parallel-groups.jsonl` 还演示了 `ok-y` 以 `attempt=2` 重新申请后，迟到的 `attempt=1` release 不会删除新轮次 hold。

## 目录

```text
coordinator/
  core.py        # Coordinator、状态机、消息入口、scan/inspect/restart
  graph.py       # 等待图、真实边过滤、环枚举、victim 策略
  simulator.py   # 本地模拟管理员和并行演示
  cli.py         # JSONL/stdin/文件/目录入口
  server.py      # 可选的本机 HTTP JSON 入口
examples/
  a-victim-is-not-release.jsonl
  stale-wait-edges.jsonl
  parallel-groups.jsonl
scenario_reader.py
test_coordinator.py
```
