---
title: 查询网关操作
description: 查询尚未结束的 MCP 工作，区分调用处理结果与后端 RPC 状态，并通过现有 Clio 服务保存有界元数据。
---

# 查询网关操作

操作日志在 `warn` 日志级别下也可查询，不依赖 Clio 是否可用。它不改变请求路由、准入、取消或重试策略。完整字段说明见英文版[操作查询指南](/guide/advanced/gateway-operations)。

## 查询运行中的网关

在源码目录中运行以下脚本，或使用家庭网关发布包自带的脚本。将端口 `3000` 替换为实际监听端口，将凭据路径替换为已有的网关凭据 JSON 文件。支持的字段为 `token`、`bearer` 或 `access_token`。命令行只传文件路径，不传令牌值。

```sh
node scripts/gateway-observability.mjs --url http://127.0.0.1:3000 --credential-file /path/to/gateway-credential.json --active
node scripts/gateway-observability.mjs --url http://127.0.0.1:3000 --credential-file /path/to/gateway-credential.json --limit 100 --json
```

仅当目标网关已禁用认证时，才省略 `--credential-file`。脚本本身不会禁用认证。URL 只包含协议、主机和端口，不包含 `/mcp`、查询参数或凭据。

列表显示本地覆盖情况、未结束或状态不确定的操作、尚未结束的调用处理器、回调、私有连接和导出状态。脚本还读取现有 `/health/mcp` 接口，显示后端加载状态和重试次数。读取失败显示为不可用，不会当作空的健康列表。

从列表复制一个操作 ID，替换下面的 `OPERATION_ID`：

```sh
node scripts/gateway-observability.mjs --url http://127.0.0.1:3000 --credential-file /path/to/gateway-credential.json --operation OPERATION_ID
```

未知或已过期的 ID、认证失败、HTTP 错误、无效响应和无效参数都会返回非零退出码。退出码零仅表示查询成功，不表示每个后端或操作都健康。脚本只发送 GET 请求，不调用 MCP 工具。

## 区分不同的结束状态

`callerOutcome` 表示网关处理器已知的结果。客户端可能先结束等待，网关随后才收到取消或检测到断连。

- `backendRequest: not_dispatched` 表示尚未记录后端分发。
- `in_flight` 表示仍有被跟踪的 RPC 未收到终结响应。
- `terminal_response_observed` 表示已观察到 RPC 终结响应。
- `unknown` 表示无法确认终结证据。

RPC 终结响应不证明外部副作用或脱离请求的业务工作已经结束。取消也不证明后端停止。调用处理器返回后，尚未结束的回调和私有连接关闭仍会保留。

`summary.active` 统计未结束或不确定的操作。`summary.callerActive` 统计尚未结束的调用处理器。`elapsedMs` 衡量调用处理器耗时；`ageMs` 在后端或私有连接工作未结束时继续增长，确认结束后停止增长。计数器属于当前进程，不是滑动窗口成功率。重启产生新的 `instanceId`，不会保留旧的内存记录。

覆盖范围以 `coverage.methods` 为准，包括旧协议工具调用、资源读取、提示词获取、补全和网关分发操作，不包含所有发现请求或通知。家庭采样器另外提供目录和实际读取证据。`coverage.complete` 表示本地观测是否丢失，不表示后端健康或历史数据已存储。

内存最多保留 512 个未结束的观测和 2,048 个已结束的观测，每个操作最多 32 个时间线点。正常历史轮换增加 `historyEvicted`；观测容量不足增加 `dropped`，但不拒绝业务请求。记录未知或已淘汰不证明工作停止。

## 启用现有 Clio 导出

在网关的进程管理器配置中设置全部三个变量。对于 systemd 管理的网关，将以下环境配置加入现有服务覆盖文件：

```ini
[Service]
Environment=MCP_GATEWAY_OBSERVABILITY_OMPHALOS_URL=https://omphalos.example.net
Environment=MCP_GATEWAY_OBSERVABILITY_SERVICE=gateway-operations
Environment=MCP_GATEWAY_OBSERVABILITY_TIER=production
```

将示例地址替换为现有 Omphalos 的地址，不是网关监听地址。服务标签保持稳定，层级明确选择 `production` 或 `staging`。重新加载进程管理器配置，并按现有发布流程只重启目标网关。若直接从终端启动网关，则在启动新进程前使用 `export` 设置变量。修改终端环境不会改变已运行或由进程管理器启动的网关。

配置不完整或无效时，导出保持禁用并报告 `invalid_config`，不会阻止网关启动。

导出器通过现有 `ingest_logs` 发送受限元数据，不发送工具参数、结果、回调内容、凭据、原始会话令牌或客户端追踪元数据。队列最多 256 个事件，每秒发送最多 32 个一批的事件，同时最多一个请求。请求截止时间为五秒，响应读取上限为 4,096 字节。溢出、失败和不确定的批次计入丢弃，不重放失败批次，也不在工具处理器内等待网络。

API 的 `export` 字段显示启用状态、队列、活动批次、丢弃数、已确认批次和有限的失败类别。确认只表示服务接受批次，不证明存储成功。通过现有 `query_logs` 查询指定服务、操作 UUID、明确时间范围和有限结果数，核实真实记录。返回的 `line` 包含受限事件 JSON。历史保留由 Clio 管理。

此功能不增加 OTLP 收集器、Span、新的事件管理器或消息发送器。Phylax 的事件确认和 Kerux 的通知投递需单独验证。
