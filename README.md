# Concurrent Task Runner (Go)

一个本地可运行的**有界并发任务执行服务**。它保证任务在并发提交、过载、取消与服务关闭
等情形下，对外状态与结果始终**一致且可解释**：

- 每个任务有一条单调的状态机，终态**只结算一次**，不会重复结算、不会结果翻转；
- 每类失败都有可机器区分的错误分类（参数非法 / 幂等冲突 / 不存在 / 过载 / 服务关闭 / 取消）；
- 被拒绝的任务不占用执行资源、不改变已有任务状态；
- 取消后永远观察不到“完成结果”；
- 关闭在宽限期内收敛在途任务，超时任务被明确标记为 `UNFINISHED`。

模块路径：`github.com/highcumontoa/concurrent-task-runner-go`，包名 `taskrunner`，
仅依赖 Go 标准库（Go 1.25+）。

## 1. 对外接口

```go
// 构造与启动（New 内部已自动启动工作池，Start 可重复调用且幂等）
r, err := taskrunner.New(cfg)

// 提交任务（支持幂等重放）
res, err := r.Submit(ctx, taskrunner.SubmitRequest{
    IdempotencyKey: "order-123", // 必填，非空
    Payload:        []byte(`{...}`), // 参与冲突判定（按 SHA-256 摘要）
    Func:           doWork,          // 必填，任务执行体
})
// res.Replay == true  表示命中幂等重放，未触发执行，返回原任务快照
// res.Replay == false 表示首次受理

// 查询（按幂等键）
info, err := r.Get(ctx, "order-123")

// 取消（排队中或执行中；终态为幂等 no-op）
err = r.Cancel(ctx, "order-123")

// 优雅关闭（可并发、可重复调用，返回与第一次相同的结果）
err = r.Shutdown(ctx)

// 运行时统计
s := r.Stats()
```

任务执行体签名：

```go
type TaskFunc func(ctx context.Context, payload []byte) ([]byte, error)
```

`ctx` 在任务被取消或服务关闭宽限期到期时取消，任务体应监听 `ctx.Done()`
做**协作式**退出。

### 任务状态机

```
PENDING ──worker领取──▶ RUNNING ──正常返回──▶ SUCCEEDED（结果可读）
   │                      │
   │                      ├──返回错误──────▶ FAILED（错误信息可读）
   ├──取消(排队)──────────┤
   │                      ├──取消──────────▶ CANCELLED（无结果）
   └──────────────────────┴──关闭宽限期到期─▶ UNFINISHED（无结果）
```

`SUCCEEDED / FAILED / CANCELLED / UNFINISHED` 均为**终态**，不可逆。
状态迁移全部通过互斥锁 + CAS 式“仅当处于预期前置状态才迁移”实现，
因此取消、完成、关闭三者竞争时**先到的终态生效，其余结算被丢弃**。

## 2. 失败原因分类（可相互区分）

所有错误均为 `*taskrunner.Error`，可用 `errors.Is` 或 `taskrunner.KindOf(err)` 判定：

| Kind（ErrorKind） | 触发场景 | 哨兵错误 |
|---|---|---|
| `invalid_argument` | 幂等键为空、`Func` 为 nil、配置非法 | `ErrInvalidArgument` |
| `idempotency_conflict` | 同一幂等键复用，但 Payload 摘要不同 | `ErrIdempotencyConflict` |
| `not_found` | 查询/取消一个从未登记（或被过载拒绝）的键 | `ErrNotFound` |
| `overloaded` | 在途+排队已满，且 `QueueWaitTimeout` 内未出现空位 | `ErrOverloaded` |
| `runner_closed` | 服务关闭中/已关闭后提交；关闭宽限期到期 | `ErrRunnerClosed` |
| `task_canceled` | 任务已取消（执行体从 ctx 观察到的取消） | `ErrTaskCanceled` |

```go
switch {
case errors.Is(err, taskrunner.ErrOverloaded):
    // 退避后重试或降级
case errors.Is(err, taskrunner.ErrIdempotencyConflict):
    // 键被不同内容占用，属于调用方 bug
case errors.Is(err, taskrunner.ErrNotFound):
    // 任务不存在
}
```

## 3. 并发、幂等与过载语义

### 幂等提交与重放

- 以 `IdempotencyKey` 去重，按 `Payload` 的 SHA-256 摘要判定内容是否一致。
- **同键 + 同载荷**：直接返回原任务快照，`Replay=true`，**绝不重复执行**。
  即使原任务仍在排队/运行，也返回同一记录。
- **同键 + 不同载荷**：返回 `ErrIdempotencyConflict`，冲突请求不登记、不执行，
  也不影响原任务状态。
- 并发重复提交在互斥锁内串行判定，并在登记前二次复查，保证 N 个并发相同提交
  恰好只有一次真实执行（见 `TestConcurrentDuplicateSubmit`）。
- 重放与冲突判定**不占用容量槽位**，重放流量不会挤占真实新任务。

### 有界并发与过载保护

容量上限 = `Workers + QueueSize`，由一个带缓冲的容量信号量精确约束：

- 提交时先非阻塞尝试拿槽位；拿不到时最多等待 `QueueWaitTimeout`。
- 超时仍无空位 → 立即返回 `ErrOverloaded`。**被拒任务在登记之前失败**，
  因此不占执行资源、不留记录、不改变已有任务状态。
- `QueueWaitTimeout == 0` 表示不等待、立即拒绝。
- 提交方的 `ctx` 在等待空位期间被取消时，立即返回 `ctx.Err()`，同样不登记。

### 取消语义

- **排队中取消**：任务立即变 `CANCELLED`，worker 即使之后取到也直接跳过，**绝不执行**。
- **执行中取消**：取消任务的 `ctx`，由任务体协作退出。若任务体无视取消、
  在取消后才返回成功结果，该迟到结果会被状态机丢弃——**取消之后不存在任何
  可被观察到的完成结果**。
- **已终态**：取消是幂等 no-op，返回 nil，状态与结果不变。
- 取消与“正常完成”竞争时，先落定的终态胜出，不会出现重复结算。

## 4. 关闭（Shutdown）语义

`Shutdown(ctx)` 按以下顺序执行，且并发安全、可重复调用：

1. 置关闭标志、关闭内部通道：此后新提交一律 `ErrRunnerClosed`；
2. 关闭任务队列：已在队列中的任务仍会被 worker 取出处理（不会静默丢弃在途工作）；
3. 在 `ShutdownGrace` 宽限期内等待在途与排队任务自然收敛；
4. 宽限期到期（或传入的 `ctx` 到期）后，所有仍非终态的任务统一标记为
   `UNFINISHED` 并取消其执行上下文，随后返回 `ErrRunnerClosed`；
5. 全部任务在宽限期内自然收敛时，等待所有 worker 退出后返回 `nil`。
   此时状态绝不会再变化，保证**无重复结算、无结果丢失**。

> 关于不协作任务：Go 无法强制 kill goroutine。若任务体既不监听 `ctx` 又永不返回，
> 宽限期后它仍会被标记为 `UNFINISHED`，其迟到结果被丢弃；该 goroutine 只能随进程
> 结束而回收，`Shutdown` 不会因此无限等待。这是 Go 协作式取消的固有约束。

## 5. 配置项

`taskrunner.Config{}`（零值字段会用 `DefaultConfig` 补齐）：

| 字段 | 含义 | 默认值 |
|---|---|---|
| `Workers` | 最大并发执行数（必须 >0） | `4` |
| `QueueSize` | 等待执行的排队容量（>=0）；容量上限 = `Workers + QueueSize` | `128` |
| `QueueWaitTimeout` | 队列满时提交方等待空位的最长时间；`0` 立即拒绝 | `200ms` |
| `ShutdownGrace` | 关闭时等待在途任务收敛的宽限期；`0` 不等待，`<0` 无限等待（仍受 Shutdown 的 ctx 约束） | `5s` |

`Stats()` 提供：`Queued / Running / SubmittedTotal / ReplayedTotal /
RejectedTotal / CancelledTotal / UnfinishedTotal`。

## 6. 本地验证方法

要求 Go 1.25+。全部命令在仓库根目录执行：

```bash
# 1) 全部单元测试（含并发竞态检测，CI 同款）
go test -race -count=1 ./...

# 2) 查看带“输入与判定依据”的用例日志（-v）
go test -race -v ./...

# 3) 固定负载压测：32 worker / 256 队列 / 3000 个 ~200µs 任务，
#    打印吞吐与提交/端到端延迟的 p50/p95/p99/max
go test -run TestStressLoad -v -count=1 .

# 4) 并发基准（固定工作量，便于本机重复对比）
go test -run '^$' -bench BenchmarkSubmitThroughput -benchtime=20000x .
```

可运行的接口示例见 `example_test.go` 的 `Example`（输出由测试锁定）。

### 测试覆盖一览

| 文件 | 覆盖内容 |
|---|---|
| `config_test.go` | 配置校验、六类错误分类可区分 |
| `idempotency_test.go` | 提交/重放、同键不同载荷冲突、参数非法/不存在、**64 路并发重复提交恰好执行一次** |
| `overload_test.go` | 立即拒绝、排队等待超时拒绝；被拒任务不登记、不占资源、不影响已有任务 |
| `cancel_test.go` | 排队取消不执行、执行中取消迟到结果被丢弃、协作式取消、终态幂等、不存在 |
| `shutdown_test.go` | 宽限期内全部收敛、宽限期到期标记 UNFINISHED 且迟到结果丢弃、关闭后拒绝新提交、5 路并发重复 Shutdown 结果一致 |
| `race_test.go` | 20 轮“取消风暴 × Shutdown”交叉竞态（终态唯一/不翻转/只有成功携带结果）、同键 Submit/Cancel/Get 高并发交叉 |
| `benchmark_test.go` | 吞吐基准 + 固定负载压测（含尾延迟分位数） |

## 7. 给定负载下的性能表现（参考）

在 `Intel Xeon 6982P-C / 4 逻辑核 / Linux` 上的一次实测（结果随机器波动，可用上面
命令在本机复现）：

- **基准** `BenchmarkSubmitThroughput`（32 worker / 1024 队列 / 每任务 200µs，
  并行提交 20000 个）：约 **31µs/op** 的提交侧开销。
- **压测** `TestStressLoad`（32 worker / 256 队列 / 3000 任务 / 每任务 200µs）：
  - 吞吐 ≈ **25,000 tasks/s**（理论上限 32/200µs = 160k，此处受 4 核与提交/调度开销限制）；
  - 提交延迟：p50 ≈ 2µs，p95 ≈ 16µs，p99 ≈ 1.0ms，max ≈ 7ms；
  - 端到端延迟（提交→完成）：p50 ≈ 10ms，p95 ≈ 19ms，p99 ≈ 21ms，max ≈ 37ms；
  - 零过载拒绝、零取消、零 UNFINISHED——容量充足时结果与状态全部正确收敛。

## 8. 目录结构

```
errors.go       错误分类与哨兵
config.go       配置、默认值与校验
task.go         任务记录与状态机（CAS 式迁移、快照、摘要）
runner.go       提交/幂等、有界并发、过载拒绝、取消、优雅关闭、统计
*_test.go       单元测试、竞态测试、基准/压测、可运行 Example
```
