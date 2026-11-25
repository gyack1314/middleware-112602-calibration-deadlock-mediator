# 联合标定事务的等待成环检测与安全解围

申请 tx 使用 attempt 区分重新申请。资源管理员的 epoch 独立，资源由 owner 与 resource 标识。

## 输入消息

每个 JSONL 文件是一段独立输入记录。每行包括整数秒点 at、消息名称 kind 与对象 record。at 代表中间件收到消息的时刻，同一秒内按行序提交。业务时间和对应回执引用写在 record 中。

- **request**　提交 tx/attempt、priority、deadline 和 resources。priority 数值越大越优先。
- **owner**　管理员从此刻使用 epoch。
- **hold**　管理员回报持有者及 phase，phase 为 reversible 或 prepared；携带 epoch。
- **wait**　管理员表明指定 tx/attempt 当前在等本服务的 resource 和持有者。 on 为同一资源当前所等持有者的 tx，具体轮次按对应 hold 绑定。
- **scan**　值班方触发按当前真实证据的解围判断。
- **release**　管理员回报止用结果 released、unknown 或 prepared。
- **restart**　协调器重启。
- **inspect**　按 tx 查询。

## 示例

- a-victim-is-not-release.jsonl
- stale-wait-edges.jsonl

## 读取演练资料

运行环境为 Python 3.11，只使用标准库。在根目录运行以下命令可核对文件格式及读取接口。

    python3 scenario_reader.py examples
    python3 -m unittest discover -v

资料读取器会按文件统计各类参与方消息的数量。演练记录只描述各参与方交给系统的输入。
