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
- HTTP SSE 转发保留 `response.incomplete` 终态中上游提供的非空输出。CLI 记录失败轨迹时可以读取最后生成的部分内容。[客户端实现](https://github.com/openai/codex/pull/58456)
- 本机使用独立配置、测试密钥和回环上游，通过真实网关 handler 和转发服务验证 HTTP、WebSocket 和超限后转 HTTP。超限用例将握手上限设为 8 KiB，检查假上游恰好收到一次 WS 预热和一次 HTTP 生成。端到端夹具使用 API Key 类型的测试账号。OAuth 常规转发和透传另以假凭据和内存 HTTP 上游验证可选的 `prompt_cache_options.ttl`。

安装 0.170.0 后，从 `backend` 目录执行：

```sh
SUB2API_CODEX_SMOKE_BINARY="$(command -v codex)" go test -tags=unit ./internal/handler -run '^TestCodexCLIResponsesSmoke$' -count=1 -v
```

### 0.170.0 复审与验收

复审在连续两轮 WS 请求中复现了一处竞态。第一轮终态回调允许第二轮开始，随后第一轮才写入“已输出”标记，会把第二轮尚未输出时收到的 1009 错判为普通断流。修复将成功写入的标记提前至回调之前。受控测试让第二轮进入上游后才允许第一轮回调返回，连续运行 30 次通过。

真实 CLI 验收现在校验二进制版本。回环代理对客户端公布 8 KiB 上限，网关实际允许更大的消息，因此遗漏客户端预检会直接在假上游观察到 WS 生成请求。负对照移除握手头后，生成请求确实改走 WS，测试按预期失败。另一个独立用例验证真实网关公布的上限与它实际执行的上限一致。

## OAuth 转发对照与使用场景复验

真实 0.170.0 CLI 使用隔离配置和本机回环端点。测试通过假凭据驱动网关的 OAuth 转发代码。HTTP 在上游传输接口处记录请求；WS 将实际拨号目标重定向至本机假上游。此结果覆盖下列协议字段和行为。生产账号、TLS、插件及完整上游身份识别未经验收。

| 场景 | 观测与结论 |
| --- | --- |
| HTTP 常规 OAuth 和透传 | 对同一次 CLI 请求核对模型、输入、工具声明、流式参数和终态。6.1 Sol 默认使用输入中的 `additional_tools`，5.4 样例使用顶层工具。客户端省略顶层 `instructions`，将指令放在 developer 消息中。常规 OAuth 原先额外合成默认指令，已修复；两种模式和两种样例均通过。 |
| HTTP 出站差异 | CLI exec 和网关使用不同的 User-Agent，网关另加 `Version: 0.170.0`；两边使用相同的 JSON 内容类型和 SSE 接收类型。OAuth 还会规范化 `additional_tools` 声明 ID 的 UUID 拼写。 |
| WS OAuth passthrough | 对照直连和经网关的两次 CLI 运行。各收到一次预热和一次生成，二者均通过 `previous_response_id` 引用预热响应。输入、工具结构与生成参数匹配，CLI 收到增量和完成事件。 |
| OAuth ctx_pool 生成中取消 | 真实 0.170.0 app-server 在支持 Responses Lite 的模型上显式启用 `interrupt_on_cancel`，收到部分文本后取消。在假上游发送终态前，网关通过本轮原租约转发匹配 response ID 的 interrupt。客户端收到确认和终态，回合为 `interrupted`，网关收到终态用量。该 CLI 功能默认关闭。 |
| ctx_pool 收到 1009 后转普通 OAuth HTTP | 用固定用户条目 ID 对照同一客户端的 WS 尝试与 HTTP 重发。无工具、无续链的测试请求在修复前丢失 ID，修复后两端均收到原 ID。本例采用构造请求以覆盖真实 CLI 默认工具配置未必触发的删除分支。 |
| ctx_pool 传输大消息 | 在 4 MiB 上限内发送大于 2 MiB 的 UTF-8 生成请求，无压缩。直连和经过网关都只生成一次，提示词完整，无 HTTP 回退。一次采样中，CLI 发送 1,048,576、1,048,576、55,871 字节三帧；网关发送 2,153,451 字节的单帧。应用层消息传输通过，出站帧边界与 CLI 有差异。 |
| 远程压缩 | CLI app-server 发出的手动压缩经 API Key 测试网关进入流式 `POST /v1/responses`，携带 `compaction_trigger`。后续一轮原样携带上游返回的 compaction 项。该用例覆盖 HTTP API Key 路径。 |

取消路径还验证了 ACK 与终态的两种顺序、拒绝错误或过期 ID、重复请求只发送一次、收到终态后仍缺少 ACK 时废弃连接并保留用量，以及后续回合可继续。缺失 ACK 的等待从控制帧发出起限为 1 秒。控制帧回归通过 6 轮 Go 竞态检测。

在 `backend` 目录可重跑 OAuth 对照、流中取消与固定 ID 回归。

```sh
SUB2API_CODEX_SMOKE_BINARY="$(command -v codex)" go test -tags=unit ./internal/service -run '^Test(CodexCLI170OAuth|OpenAIWSOAuthCtxPoolInterrupt|OpenAIOAuthCtxPoolHTTPFallbackPreservesUserMessageID)' -count=1 -v
```
