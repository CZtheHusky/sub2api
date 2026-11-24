# Codex CLI 0.172.0 的 Responses WebSocket 跟踪计划

2026-11-23（洛杉矶）。目标是在 sub2api 的 WebSocket 入口验证正式版 CLI 的请求、续轮和结束行为。比较端点为 `GET /v1/responses` 升级后的双向连接。所有实测使用本机假上游、临时客户端目录及测试凭据。

## 起点

| 项目 | 依据 |
| --- | --- |
| 目标客户端 | GitHub 正式版与 npm 的 `latest` 均为 `0.172.0`。使用 Linux x86_64 发布包复验。[OpenAI 发布页](https://github.com/openai/codex/releases/tag/rust-v0.172.0) · [npm 发行标签](https://registry.npmjs.org/-/package/@openai/codex/dist-tags) |
| 比较客户端 | [0.170.0 到 0.172.0 的源码差异](https://github.com/openai/codex/compare/rust-v0.170.0...rust-v0.172.0)，其中包含 0.171.0 |
| 分支起点 | `codex/client-compat-0.172.0` 基于 `upstream/main@348997267d756eab5729bb6b78a275a15d18796a`。初次分支时上游为 `00c51f07d`，提交前快进并重新验证。本地 `main` 跟踪同一远端，核对时 fork 的 `origin/main` 为 `2af3f1e15` |
| 参考工作 | `codex/client-compat-0.170.0@b547c82cd` 的真实 CLI、回环上游及逐轮 close 测试，以及 `sub4api@4ae6b9fa8` 的 `backend/internal/service/openai_codex_ws_socket_test.go`。后者分别在握手与应用消息上观察连接复用后的字段 |

## 执行顺序与验收

1. **核对发布源码。** 比较 Responses WS 握手和序列化、预热与续轮、错误解码。逐项标出适用的提供商和触发条件。把客户端输入的字段和网关生成的字段分开记录。
2. **建立直连对照。** 在相同 Linux 环境中运行两个正式 CLI，对同一本机 WS 服务记录 Upgrade、帧类型、字段存在性、生成次数及终态。请求标识按连接和轮次比较关联关系。对握手、正文和帧顺序分别给出可观察的变化；应用层记录不能用于判断 TLS 指纹。
3. **经过网关复验。** 用 0.172.0 比较直连、透传和连接池路径。检查预热后的首条生成、服务端保持套接字开启时的错误、生成是否被重复提交。根据旧分支的回归重点，单独检查握手公布的读上限和首帧前 1009 关闭码。HTTP 请求只能作为离开 WS 的边界信号。
4. **验证差异与修复。** 区分官方客户端新增行为、两版共有但新 main 遗漏的兼容行为，以及当前工具无法观察的条件行为。只对有失败证据的 WS 路径改代码，以同一个用例在修复前后核对结果。保留复跑命令、实际覆盖面和待验证项。

验收时应能回答：新版本的哪一项线上可见行为发生了变化，哪一项只在特定条件下出现；同一组输入直连和经网关能否得到一致的协议结果；若连接失效，上游收到多少次实际生成请求。

## 两版 CLI 的实测差异

在 Linux x86_64 上用两个正式二进制运行 `codex app-server`。它们通过同一个自定义 API-key provider 访问回环 WS 服务。每个场景有独立的客户端目录，mock 决定何时回帧、何时关闭连接。下表的“立即”指 mock 保持连接开启、CLI 在观测窗口内主动结束本轮。

| 场景 | 0.170.0 | 0.172.0 |
| --- | --- | --- |
| Upgrade 请求 | `GET /v1/responses`，User-Agent 版本为 0.170.0 | 同一路径，版本为 0.172.0。直连探针的十组实验中，完整请求头名称集合逐组相等 |
| 新会话的两个连续文本轮次 | 一次握手。三条 `response.create` 依次为预热、第一次生成、第二次生成。预热使用 `generate:false`，两次生成分别引用预热和第一次生成的响应 ID。最后一帧的 `input` 只含新增用户输入 | 相同 |
| 生成中取消，或追加指令（steer） | 已收到输出时，显式启用 `interrupt_on_cancel` 再取消，会发 `response.interrupt` 并结束该轮。steer 场景在该开关关闭时也发 interrupt，在同一连接上引用刚结束的生成并继续 | 相同。两种场景的 interrupt 都带当前响应 ID 和 `discard_partial_items` |
| 首个生成响应前，上游发 WS close 1009 | 一次握手，一次预热，一次生成。没有第二次 WS 拨号 | 相同。若握手提前声明单帧上限为 1 字节，两版均只握手，WS 应用帧数为零 |
| 无顶层 `status`，只有 `error.code=invalid_prompt` | 连接保持开启的 10 秒内未出现错误通知或轮次结束 | 立即结束为 `failed`。app-server 对外报告 `other` |
| 无顶层 `status`，错误代码指向 API key、项目或组织的费用上限 | 三个场景在 10 秒内均未出现错误通知或轮次结束 | 立即结束，分别报告 `apiKeySpendLimitExceeded`、`projectSpendLimitExceeded`、`organizationSpendLimitExceeded` |
| 无顶层 `status`，只有 `error.code=context_length_exceeded` | 立即报告 `contextWindowExceeded` | 相同。这项作为检验错误夹具的阳性对照 |

这次逐组比较了握手字段名、User-Agent 的版本和应用帧的因果顺序。字段值依赖进程、连接与运行环境。回环使用明文 WS，TLS ClientHello、JA3/JA4 及生产链路未测量。官方错误解码和费用类别可分别查阅 [0.172.0 的 WebSocket 错误用例](https://github.com/openai/codex/blob/rust-v0.172.0/codex-rs/core/tests/suite/client_websockets.rs#L2736) 和 [额度错误变更](https://github.com/openai/codex/pull/62329)。

## 需要特定条件才出现的变化

| 来源 | 触发条件和可观察结果 | 本分支的验证 |
| --- | --- | --- |
| [0.171.0 引入逐轮 request_country](https://github.com/openai/codex/blob/rust-v0.172.0/codex-rs/core/tests/suite/agent_websocket.rs#L931) | 官方 Codex 后端的合格认证路径可在同一连接上逐轮发送国家码、显式 `null` 或省略字段。普通自定义 API-key provider 不会自动产生这个字段 | 在 API-key 连接池测试中输入三种合成帧；同一上游连接依次收到原值、`null`、无该字段 |
| [0.171.0 的历史图片预热](https://github.com/openai/codex/blob/rust-v0.172.0/codex-rs/core/tests/suite/scenarios_restored_history_prewarm.rs#L258) | 启用恢复预热、恢复含历史文件图片的线程时，首次真实生成可复用预热连接并重发完整历史，省略预热响应 ID。后续轮再引用真实生成 ID | 已核对官方源码与用例。本地二进制实测覆盖新建文本会话 |
| [0.171.0 的启动预热和 W3C trace](https://github.com/openai/codex/blob/rust-v0.172.0/codex-rs/core/tests/websocket_startup_prewarm_tracing.rs#L25) | 调用方提供追踪上下文时，预热与生成可使用各自的 trace。在同一连接上也不能假定 trace 永远沿用握手值 | 已核对官方用例，未连接外部追踪系统 |
| [0.172.0 的连接器工具结果](https://github.com/openai/codex/blob/rust-v0.172.0/codex-rs/codex-api/src/common.rs#L37) | 满足授权条件时，客户端可给指定的 executed tool call 附加 `tool_result` 和可选的 `tool_result_call_id`。添加结果时整条请求受 15 MiB 预算限制。客户端序列化观察器看到的是添加前的数据，带私有结果的发送过程也抑制底层帧日志 | 本地 API-key 池用合成结果验证两个字段一起转发。仓库的 OAuth 整理逻辑会移除所在的 `input[].internal_chat_message_metadata_passthrough`，本次没有用 OAuth 账户或真实连接器复测。[官方发送实现](https://github.com/openai/codex/blob/rust-v0.172.0/codex-rs/codex-api/src/endpoint/responses_websocket.rs#L354) |

## 对最新 main 的修复

1. 在未修复的起点，网关配置 128 字节的读上限，握手返回的 `X-Codex-WebSocket-Max-Message-Bytes` 为空。现从同一配置值设置 Upgrade 响应头和实际读上限。128 字节的测试帧进入正文校验，129 字节触发 WS close 1009。
2. 原始 `348997267` 上，透传、连接池和专用连接都把本轮首输出前的上游 1009 变成了下游 1011。透传另有一个时序问题：上一轮的终态写出后，新一轮已开始时仍可能被标为“已输出”。现在按轮判断是否已有输出。当前轮尚无输出时，网关把 1009 带回客户端，并停止重放该轮。已经有输出的透传轮次按中途断流处理。

两项修复取自 0.170.0 分支的 `05a903e58` 和 `ac0cd7b80`。原分支对 SSE 和全局客户端版本设置的改动超出本次 WS 跟踪范围。

## 网关实测与已知差异

用两个正式版各跑六例。客户端分别直连假上游、经网关 `passthrough`、经网关 `ctx_pool`。每条路径运行一次正常生成、一次无顶层状态的项目额度错误。六例中的每一例只出现一次上游握手、一次预热、一次真实生成，HTTP 请求数为零。正常轮次均返回指定文本。额度错误在三条路径上的对照结果一致：0.172.0 得到结构化错误，0.170.0 在发出错误后的两秒内仍无终态。另用受控 WS 客户端验证 `passthrough`、`ctx_pool`、`dedicated` 对本轮首输出前的 1009 均只提交一次请求。

**连接池仍有一个会影响交互的旧差异。** 用合成 WS 客户端建立真实网关连接，在收到首个输出增量后，发送带当前响应 ID 的 `response.interrupt`。三个重复实验中，直连及透传都在生成结束前将中断送到上游，随后客户端收到上游的接受通知。`ctx_pool` 在一秒观察窗内始终没有转发中断。放行上游终态后，客户端先收到 `response.completed`，再收到 1008，原因是 `unsupported websocket request type: response.interrupt`。当前池化路径在轮次结束后才读下一条客户端消息，两个正式 CLI 都会发送这个控制帧。0.170.0 旧分支的 `5013ed943` 可作为后续处理并发取消的起点。本次提交记录此差异，未移植旧分支的中断状态机。

## 复跑

在仓库根目录将 `CODEX_170` 和 `CODEX_172` 设为对应官方可执行文件的绝对路径。直连对照需要 Python 3.10+ 和 `websockets==10.4`。Go 网关测试独立运行，需要在本机监听回环端口。模型名和凭据由夹具提供。

```sh
python3 tools/codex_ws_compare.py "$CODEX_170" "$CODEX_172"
# 单独观察最小错误帧
python3 tools/codex_ws_compare.py "$CODEX_170" "$CODEX_172" --scenario bare-invalid-prompt

cd backend
SUB2API_CODEX_SMOKE_BINARY="$CODEX_170" go test -tags=unit ./internal/handler -run '^TestCodexCLIWebSocketTracking$' -count=1 -v
SUB2API_CODEX_SMOKE_BINARY="$CODEX_172" go test -tags=unit ./internal/handler ./internal/service ./internal/service/openai_ws_v2 \
  -run '^(TestCodexCLIWebSocketTracking|TestOpenAIResponsesWebSocket_.*|TestOpenAIGatewayService_ProxyResponsesWebSocketFromClient.*|TestRelay_.*|TestPassthroughLifecycle_.*|TestOAuthInputInternalMetadata)$' -count=1 -v
go test -race -tags=unit ./internal/service/openai_ws_v2 -run '^TestRelay_UpstreamMessageTooBigAfterPreviousTerminalWrite$' -count=15
```

Python 探针逐场景输出 JSON Lines，包含连接次数、帧顺序、轮次终态和完整握手字段名集合的比较结果。`observation_timeout` 指在开启的连接上等到观察窗口截止。探针自身启动或通信失败时以非零状态退出。Go 的真实 CLI 测试需要显式设置 `SUB2API_CODEX_SMOKE_BINARY`，省略时会标为跳过。

本次使用 Go 1.27.4。以 `348997267` 的原始生产源码运行同一组测试，握手读上限、三种模式的 1009 传播、透传跨轮输出计数均能复现失败。应用修复后，上面的 0.172.0 定向命令通过 148 个顶层测试，跳过数为零。0.170.0 的六个真实 CLI 场景另外复跑通过。跨轮时序用例在 `-race` 下连续通过 15 次。
