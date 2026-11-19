# Codex CLI 0.170.0 兼容性验收报告

第一轮传输验收对应提交 `ac0cd7b80`。真实 CLI 经 sub2api 网关连接本机假上游，以下四项均通过。测试启动时校验 `codex --version` 的输出为 `codex-cli 0.170.0`。本次未执行 实际部署环境 真实账号测试。

| 场景 | 假上游收到的请求 |
| --- | --- |
| HTTP | 一次 HTTP 生成，CLI 正常完成 |
| WebSocket | WS 预热后，一次 WS 生成，CLI 正常完成 |
| 请求超过握手公布的上限 | 一次 WS 预热，一次 HTTP 生成 |
| 上游在本轮首次响应前关闭 WS，返回 1009 | 一次 WS 预热，一次被拒绝的 WS 生成请求，一次成功的 HTTP 生成 |

## 分支与协议改动

版本标志已对齐。默认 User-Agent 和 `version` 头共用 0.170.0 版本常量。四项端到端用例验证请求使用的传输方式、生成次数和 CLI 终态。这四项使用 API Key 测试账号。后续 OAuth 对照结果单列如下。

`codex/client-compat-0.170.0` 从手动降智检测提交 `bb7261b9c` 分出。内置客户端版本升到 0.170.0，版本自动同步默认关闭。对照的官方源码是 [rust-v0.170.0](https://github.com/openai/codex/tree/rust-v0.170.0)。

- Responses WebSocket 握手通过 `X-Codex-WebSocket-Max-Message-Bytes` 公布网关实际的消息上限。CLI 可以在发送过大的请求前改用 HTTP。[客户端实现](https://github.com/openai/codex/pull/60841)
- 如果上游在本轮发回任何响应帧前因消息过大而以 `1009` 关闭，网关将关闭码送回客户端。该轮已被上游拒绝，网关会结束本轮 WS 转发，由 CLI 切换 HTTP。已有响应帧的情况按流中断处理。[客户端错误处理](https://github.com/openai/codex/pull/60840)
- HTTP SSE 转发保留 `response.incomplete` 终态中上游提供的非空输出。普通 OAuth HTTP 上游先报告独立的临时错误、随后送达该终态时，下行 SSE 保留终态中的部分输出，CLI 报告上游给出的结束原因。[客户端实现](https://github.com/openai/codex/pull/58456)
- 本机使用独立配置、测试密钥和回环上游，通过真实网关 handler 和转发服务验证 HTTP、WebSocket 和超限后转 HTTP。超限用例将握手上限设为 8 KiB，检查假上游恰好收到一次 WS 预热和一次 HTTP 生成。端到端夹具使用 API Key 类型的测试账号。OAuth 常规转发和透传另以假凭据和内存 HTTP 上游验证可选的 `prompt_cache_options.ttl`。

从 `backend` 目录执行以下命令。找不到 CLI 时命令返回失败。夹具为 CLI 子进程分别设置独立的 `CODEX_HOME` 和 `TMPDIR`。

```sh
codex_bin=$(command -v codex) &&
  SUB2API_CODEX_SMOKE_BINARY="$codex_bin" go test -tags=unit ./internal/handler -run '^TestCodexCLIResponsesSmoke$' -count=1 -v
```

### 0.170.0 复审与验收

复审在连续两轮 WS 请求中复现了一处竞态。第一轮终态回调允许第二轮开始，随后第一轮才写入“已输出”标记，会把第二轮尚未输出时收到的 1009 错判为普通断流。修复将成功写入的标记提前至回调之前。受控测试让第二轮进入上游后才允许第一轮回调返回，连续运行 30 次通过。

真实 CLI 验收现在校验二进制版本。回环代理对客户端公布 8 KiB 上限，网关实际允许更大的消息，因此遗漏客户端预检会直接在假上游观察到 WS 生成请求。负对照移除握手头后，生成请求确实改走 WS，测试按预期失败。另一个独立用例验证真实网关公布的上限与它实际执行的上限一致。

从 `backend` 目录分别复跑网关读上限检查和跨回合 1009 竞态回归。

```sh
go test -tags=unit ./internal/handler -run '^TestOpenAIResponsesWebSocket_AdvertisesAndEnforcesMessageReadLimit$' -count=1
go test -tags=unit ./internal/service/openai_ws_v2 -run '^TestRelay_UpstreamMessageTooBigAfterPreviousTerminalWrite$' -count=30
```

## OAuth 转发对照与使用场景复验

真实 0.170.0 CLI 使用隔离配置和本机回环端点。常规 OAuth 与透传 HTTP 在上游传输接口处记录请求。WS 透传和大消息对照通过回环套接字连接假上游。取消用例中，CLI 通过回环 WS 连接网关，网关通过连接池取得可控制事件顺序的内存上游连接。固定 ID 回退用例使用构造请求。生产账号、TLS、插件及完整上游身份识别未经验收。

| 场景 | 观测与结论 |
| --- | --- |
| HTTP 常规 OAuth 和透传 | 对同一次 CLI 请求核对模型、输入、工具声明和流式参数。常规 OAuth 的 SSE 送回真实 CLI；透传模式的答案和终态由 Go 响应记录器核对。6.1 Sol 默认使用输入中的 `additional_tools`，5.4 样例使用顶层工具。客户端省略顶层 `instructions`，将指令放在 developer 消息中。常规 OAuth 原先额外合成默认指令，已修复；两种模式和两种样例均通过。 |
| HTTP 出站差异 | 采样日志中 CLI exec 和网关的 User-Agent 不同。两个网关转发模式均断言出站 `Version: 0.170.0`、JSON 内容类型和 SSE 接收类型。OAuth 还会规范化 `additional_tools` 声明 ID 的 UUID 拼写。 |
| WS OAuth passthrough | 对照直连和经网关的两次 CLI 运行。各收到一次预热和一次生成，二者均通过 `previous_response_id` 引用预热响应。比较 `type`、`model`、`stream`、`generate`、`previous_response_id`、`instructions`、`tools`、`tool_choice`、`parallel_tool_calls`、`reasoning`、`include`、`store`、`text`、`service_tier`、`prompt_cache_options` 的存在性和值。输入逐项比较内容和 ID 类型前缀。假上游在 CLI 通过增量事件读到完整文本后，才发送含完整答案的完成事件。 |
| OAuth ctx_pool 生成中取消 | 真实 0.170.0 app-server 在支持 Responses Lite 的模型上显式启用 `interrupt_on_cancel`，收到部分文本后取消。在假上游发送终态前，网关通过本轮原租约转发匹配 response ID 的 interrupt。在通往 CLI 的 WS 上捕获到匹配的 ACK 和终态，app-server 报告回合为 `interrupted`，网关回调报告终态用量。扣掉下行 ACK 的负对照在 WS 断言处失败。该 CLI 功能默认关闭。 |
| OAuth ctx_pool 生成中追加输入（增量续轮） | 真实 0.170.0 app-server 使用 `turn/steer`。假上游收到 interrupt 后等待 1.2 秒再发送终态，全程不发 ACK。CLI 在原 WS 上用同一个 `turn_id` 和刚结束的 `previous_response_id` 续发生成，追加的用户消息到达上游。将匹配的 `response.interrupt.failed` 放在终态之前或之后，两种顺序下 CLI 均沿原租约继续同一回合。 |
| OAuth ctx_pool 调整推理级别后追加输入（全量续轮） | 真实 CLI 在启用 `step_model_switching` 和实验 API 后，通过 `turn/settings/update` 将推理级别调为 `high`，再调用 `turn/steer`。续轮在原 WS 上省略 `previous_response_id`，原提示和追加提示按序作为用户消息到达假上游。工具用例先让 CLI 执行 `exec_command`，确认上一帧仅包含工具结果，再验证全量续轮先携带原 `exec_command` 调用，随后携带匹配其 `call_id` 的原工具输出，CLI 正常结束回合。 |
| ctx_pool 收到 1009 后转普通 OAuth HTTP | 用固定用户条目 ID 对照同一客户端的 WS 尝试与 HTTP 重发。无工具、无续链的测试请求在修复前丢失 ID，修复后两端均收到原 ID。本例采用构造请求以覆盖真实 CLI 默认工具配置未必触发的删除分支。 |
| ctx_pool 传输大消息 | 在 4 MiB 上限内发送大于 2 MiB 的 UTF-8 生成请求，无压缩。直连和经过网关都只生成一次，假上游收到的最后一条用户消息逐字匹配完整提示词，无 HTTP 回退。等长改写首行的负对照在完整文本断言处失败。一次采样中，CLI 发送 1,048,576、1,048,576、55,871 字节三帧；网关发送 2,153,451 字节的单帧。应用层消息传输通过，出站帧边界与 CLI 有差异。 |
| 远程压缩 | CLI app-server 发出的手动压缩经 API Key 测试网关进入流式 `POST /v1/responses`，携带 `compaction_trigger`。后续一轮的 `compaction` 项保留了返回的 `id` 和 `encrypted_content`。该用例覆盖 HTTP API Key 路径。 |

WS 增量负对照只在经过网关的连接上扣住第二段文本。直连完成，经过网关的 CLI 未能拼出完整文本，测试按预期失败。

取消回归覆盖以下场景：

- 确认与 `response.completed` 的两种顺序均有回归。`response.incomplete` 带有 `incomplete_details.reason=interrupted` 时也允许先送终态再等 ACK。`response.metadata`、`codex.response.metadata`、`responsesapi.websocket_timing` 可以夹在中间并送达下游。中间通知不会重置已有的取消期限。
- 对当前响应的拒绝，网关向下游转发 `response.interrupt.failed`。原生 Lite 的主动取消由 CLI 放弃该 WS，网关废弃租约。追加输入时 CLI 可以读完原响应，再沿原租约显式关联续发。缺少 Lite 回合标识时仍按取消拒绝结束。若网关已经读到终态，服务通过 `AfterTurn` 回调其用量一次；数据库入账未纳入本组测试。
- 官方 CLI 的 Drain（追加输入）和 Cancel（主动取消）共用同一种控制帧。带 `turn_id` 的 Responses Lite 请求由客户端执行 Cancel 的一秒期限，网关保留普通上游读超时。这里依赖原生 CLI 执行取消期限，其他客户端仅声明相同元数据但持续保持连接时，释放时间可能超过一秒。真实 CLI 在只收到终态、或只收到 ACK 时都会放弃该 WS，网关读到断连后废弃租约。若正在同步向上游写 interrupt，处理断连可能延迟到该次写入返回或触发配置的上游写超时。只有前一种情况收到生成终态并回调其用量。同一逻辑回合可通过响应 ID 增量续发，全量续发省略该字段时，网关在新请求的非末尾用户条目中查找上一请求最后一条用户消息的内容；上一请求只有工具输出等增量时，从已关联的完整重放输入取最近的用户消息进行相同比较。比较先匹配原始 JSON，原文不同时再按 JSON 内容比较，对象字段顺序可以不同，数组元素顺序必须一致。对无法关联的排队 create，仍在等 ACK 时网关恢复使用从原 interrupt 发出时起算的一秒期限；已经收到明确拒绝时，网关直接按拒绝结束。已确认 steering 续轮后，上一响应迟到的 accepted 或 failed 由网关按旧 response ID 消费，不会确认或拒绝当前响应；若归还租约时仍有未决 ACK，池会关闭该连接，下一位借用者重新拨号。
- 对缺少 Lite 回合标识的请求，网关仍从发送 interrupt 起使用原有的一秒期限，覆盖等待确认、终态和取消期间的写入。下游 TCP 停读造成终态写入超时时，网关释放租约并对已读终态成功回调一次。
- 新请求写出后的第一次上游读取获得完整读超时。如果这次读到上一响应迟到的 ACK，消费它后会重新开始一次读取，到新响应首帧的总等待时间可能更长。测试把读超时设为一秒，在新请求前后各等待 650 毫秒；关联续轮能收到终态，完全不返回首帧时仍会超时。
- 对正常 Lite steering，网关按配置的下行写超时发送帧。受控慢读用例让客户端停读约 1.35 秒，随后验证完整的 8 MiB 终态和原租约上的续轮。
- 网关会拒绝客户端发来的错误或过期 ID。对同一响应重复请求取消，只向上游转发一次。

从 `backend` 目录可单独复跑 HTTP 部分输出快照与 OAuth 缓存 TTL 回归。

```sh
go test -tags=unit ./internal/service -run '^Test(NormalizeResponsesStreamingTerminalOutputPreservesIncompleteSnapshot|OpenAIResponseFlush_BareErrorFollowedByIncompleteKeepsSnapshot|OpenAIGatewayService_CodexOAuthPreservesTurnCacheTTL)$' -count=1
```

在 `backend` 目录可重跑 OAuth 对照、流中取消与固定 ID 回归。

```sh
codex_bin=$(command -v codex) &&
  SUB2API_CODEX_SMOKE_BINARY="$codex_bin" go test -tags=unit ./internal/service -run '^Test(CodexCLI170OAuth|OpenAIWSOAuthCtxPoolInterrupt|OpenAIOAuthCtxPoolHTTPFallbackPreservesUserMessageID)' -count=1 -v
```

单独复跑取消状态机的竞态检测：

```sh
go test -race -tags=unit ./internal/service -run '^TestOpenAIWSOAuthCtxPoolInterrupt' -count=6
```

本轮在同一工作区联合复跑上述 CLI 用例、固定 ID 回退、网关读上限、连接池和 WS 生命周期回归，service 与 handler 两组均通过。跨回合 1009 回归通过 30 次，取消状态机通过 6 轮竞态检测。所有请求使用本机测试凭据和假上游。
