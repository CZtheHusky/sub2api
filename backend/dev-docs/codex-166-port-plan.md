# Codex CLI 0.166.0 兼容与降智检测移植准备

- 目标仓库：`CZtheHusky/sub2api`。旧 fork 主线保留在 `backup/main-2026-11-17`（`f13a96fc19fbbe08386940db23801e4a23fb006f`）。
- 上游基线：`Wei-Shaw/sub2api` 的 `main`，冻结在 `2af3f1e15c98c893a89255a037f551046883b8c0`（0.2.23）。
- 功能分支：`codex/client-compat-degradation`。来源：sub4api `4ae6b9fa8f79c3c6992189b181b55b6d59adea6d`。
- 后续只在功能分支承载补丁。更新时先取官方上游，再在 main 上执行快进更新。把功能分支移到新基线前，先核对已经被上游吸收的功能与回归差异。

```sh
git fetch upstream main
git switch main
git merge --ff-only upstream/main
git switch codex/client-compat-degradation
```

## 第一批：正常 Codex HTTP / WebSocket 协议

目标上游已有 Codex 版本集中配置、账号隔离、出站 transform 和 WS 池。按行为差异适配，避免用 sub4api 整文件覆盖。目标基线已有 `OpenAICodexVersionSyncService`。本分支默认关闭自动同步，并固定内置客户端版本为 0.166.0；管理员手填优先，明确启用同步后才使用同步值。0.166.0 是本机协议验收基线。

从 sub4api `4bb4a3b59`（0.164 基础）和 `4ae6b9fa8`（0.166 Guardian）的测试契约提取以下验证，不直接 cherry-pick 依赖其他扩展的整组提交：

| 行为 | 在新版上核对 | 验收 |
| --- | --- | --- |
| Responses HTTP 与 Lite 入参 | 出站 header、标准 turn metadata、额外工具、事件终态、请求体顺序和引用关系 | 同版本 CLI 直连 fake upstream 与经过新网关的常规流，检查语义等价及正常错误回传 |
| WS 首轮、追加输入、取消与恢复 | 现有池、连接生命周期、已有完整历史保护和各协议模式 | 用两轮 CLI 请求对照，检查连接重开时当前轮语义，统计发给假上游的次数；断线或信息不足时不误重放 |
| Guardian reviewer | 上游已有的 capability、身份和池隔离规则；仅传递已存在且与当前请求相关的标准协议字段 | 父子角色、metadata 省略语义、池隔离，使用假上游和假凭据；不使用第三方访问控制绕过、路由票据注入或跨账号复用 |

## 第二批：独立能力检测

sub4api 的检测链是 `modeltrace_challenge.go`、分类数据与许可证、`codex_ticket_diagnostic.go` 的现有 verdict 逻辑、`codex_diagnostic_gateway.go` 的网关诊断运行器和 `codex_auto_degradation_check.go`。旧 fork 也有较早的 ModelTrace 管理端和历史页，但移植以 sub4api 当前实现为语义来源，保留来源与许可，按新版入口接入。

1. 先接纯分类、模板、固定参考样本与结果类型。返回正常、疑似降级、不确定或失败；未知模型参考保持不确定。
2. 接管理员手动检测、账号明确选择与历史。复用当前网关正式传输和权限，记录结果、模型、观测时间、错误、HTTP/WS 协议信号；检测不会绕过上游限制。协议不支持、超时和网络错误不能当作降级；探针结果也不能保证识别实际运行模型。
3. 再接默认关闭的自动检测、任务生命周期、并发上限和只管理自身标记的临时停调。失败不加封也不解除封禁；复查仅解除自动检测建立的停调，保持其他原因与模型级限流。调度和结算规则按新版实现对照。测试用假上游验证可恢复与不可恢复案例。
4. 历史存储按新上游迁移序列新增文件，不能复用 sub4api 的旧 `252_codex_attempt_ws_probe.sql` 序号；现网数据迁移以后用备份副本另测。

范围只保留正常 Codex 客户端兼容和独立能力检测。支付、BPS、审计、自动用重置卡、点数接管、Antigravity、自更新及旧 fork 的 CI 规则不进入本分支。旧打票、风控或路由绕过不属于协议兼容和检测。

## 当前验证

- 本机 `codex --version` 为 0.166.0。使用独立测试配置与假凭据，回环 HTTP fake Responses 收到一次 `POST /v1/responses`、`stream=true`，CLI 获得 `LOOPBACK_OK` 并退出 0。这个结果验证 CLI 与夹具可用，尚未经过 sub2api 网关。
- 可重跑 `python3 backend/scripts/codex-166-loopback-smoke.py`。脚本只启动短时本地服务器，使用隔离的 Codex 配置。
- 已用 Go 1.27.4 运行版本同步、版本优先级和规范 UA 的定向回归，全部通过。WS 和网关端到端仍待移植后测试。真实账号验证按约定仅在 实际部署环境 执行。

## 首批移植进度

- 新上游已保留 turn metadata 中 `guardian_recorded_items` 的 `true`、`false` 和字段省略含义，补上回归用例。
- 管理员可在标准 OpenAI OAuth 账号的测试面板运行一次降智检测。HTTP 探针复用现有账号测试传输，使用内置 ModelTrace 参考库，展示判定与最接近的参考模型。断流、限流等请求错误与降智判定分开处理，探针不修改账号的限流和调度状态。
- 自动检测、历史、WebSocket 探针和 CLI 经网关的端到端验证尚待实现或执行。
