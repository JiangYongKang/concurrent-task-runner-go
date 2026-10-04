# concurrent-task-runner-go

一个本地可运行的内存并发任务执行服务。目标是让任务在**并发提交、过载、取消、服务关闭**等情形下，
状态与结果始终一致、只结算一次、且每个失败结局都可用机器可判定的错误码区分。

- 语言：Go（go.mod 声明 1.25）
- 无外部依赖，仅标准库
- 包路径：`github.com/highcumontoa/concurrent-task-runner-go/runner`

---

## 1. 对外接口

```go
import "github.com/highcumontoa/concurrent-task-runner-go/runner"

// 创建并启动
r, err := runner.New(runner.Config{
    MaxWorkers:    8,
    MaxQueue:      1024,
    MaxQueueWait:  200 * time.Millisecond,
    ShutdownGrace: 5 * time.Second,
})

// 提交（idemKey 为幂等标识；Payload 参与冲突判定；fn 为任务体）
state, err := r.Submit(ctx, "order-123",
    runner.Payload{Name: "charge", Params: map[string]any{"amount": 100}},
    func(ctx context.Context) (any, error) {
        // 业务逻辑；请监听 ctx.Done() 以响应取消/关闭
        return "done", nil
    })

// 提交并指定自动重试策略（不指定时的行为与 Submit 完全一致）
state, err = r.SubmitWithRetry(ctx, "order-456",
    runner.Payload{Name: "charge", Params: map[string]any{"amount": 200}},
    func(ctx context.Context) (any, error) {
        if err := maybeTransient(); err != nil {
            return nil, runner.Retryable(err) // 标记为“值得重试”的失败
        }
        return "done", nil
    },
    runner.RetryPolicy{
        MaxAttempts: 3,                          // 最多共尝试 3 次
        Interval:    100 * time.Millisecond,     // 首次重试间隔
        Backoff:     runner.BackoffExponential,  // 间隔逐次翻倍
        MaxInterval: time.Second,                // 间隔封顶
    })

// 查询
state, ok := r.Get(ctx, "order-123")

// 取消
state, err = r.Cancel(ctx, "order-123", "user-abort")

// 优雅关闭
_ = r.Shutdown(ctx)

// 计数快照
_ = r.Stats()
```

### 任务状态 `runner.Status`

| 状态 | 含义 | 是否终态 |
| --- | --- | --- |
| `queued` | 已受理并在 FIFO 队列等待执行 | 否 |
| `running` | 正在执行 | 否 |
| `retry_wait` | 两次尝试之间等待下一轮重试（不占执行并发，仍属进行中） | 否 |
| `succeeded` | 执行成功，结果在 `TaskState.Result` | 是 |
| `failed` | 任务函数返回错误或 panic（已 recover），含重试次数用尽 | 是 |
| `canceled` | 被 `Cancel` 或关闭流程取消（排队/在途/重试等待） | 是 |
| `rejected` | 因过载或关闭被拒绝受理执行 | 是 |
| `aborted` | 关闭宽限期结束仍未结束 | 是 |

终态由 `status.IsTerminal()` 判定；终态任务的结果与状态不再变化。

### 失败原因码 `runner.Code`（`*runner.TaskError.Code`，也可用 `errors.Is` 配哨兵错误）

| Code | 哨兵错误 | 触发入口 | 含义 |
| --- | --- | --- | --- |
| `invalid_argument` | `ErrInvalidArgument` | Submit / Config | 幂等键为空、`fn==nil`、配置非法 |
| `idempotent_conflict` | `ErrIdempotentConflict` | Submit | 同幂等键复用但 `Payload`（Name/Params）不同 |
| `not_found` | `ErrNotFound` | Cancel（Get 用 `ok=false` 表达） | 任务不存在 |
| `queue_full` | `ErrQueueFull` | Submit | 在途容量已满且 `MaxQueueWait<=0`，立即拒绝 |
| `queue_timeout` | `ErrQueueTimeout` | Submit | 容量满，等待 `MaxQueueWait` 仍无空位 |
| `shutting_down` | `ErrShuttingDown` | Submit | 关闭开始后的新提交 |
| `task_canceled` | `ErrTaskCanceled` | Cancel / 等待结果 / 协作退出 | 任务被取消 |
| `terminal` | `ErrTerminal` | Cancel | 已 succeeded/failed，不能再取消 |
| `aborted` | `ErrAborted` | 关闭 | 宽限期结束仍未结束 |
| `task_failed` | — | 任务执行业务错误 | succeeded 之外的业务失败，体现在 `failed` 终态 |

三类“找不到/参数错/键冲突”严格区分，可直接按 `Code` 分支处理。

---

## 2. 关键语义

### 2.1 提交与幂等重放

- 以 `idemKey` 为唯一标识登记；提交内容用 `Payload{Name, Params}` 的确定性哈希作为**指纹**。
- **同键 + 同内容**：不重复执行。已终结则立即重放同一份最终状态/结果；仍在执行（含 `retry_wait`）则阻塞到终结再重放。
- **同键 + 不同内容**：返回 `idempotent_conflict`，已有任务完全不受影响，本次也不会执行。
- 对 `canceled/aborted/failed` 终态的同内容重复提交：重放当时的失败原因（不重新执行）。
- 对因**瞬时容量不足**（`queue_full`/`queue_timeout`）被拒的任务：同内容再次提交会**重新登记执行**，
  该幂等键不会因一次瞬时拒绝而永久不可用；容量恢复后即可正常提交成功。
  （`shutting_down` 拒绝不在此列：关闭后的一切提交仍被拒绝。）
- 重试策略不参与指纹：同一幂等键的重试策略以**首次受理的提交**为准，重放时忽略。
- 高并发下同键重复提交只有一个任务真正执行（见 `TestConcurrentDuplicateSubmit`）。

### 2.2 有界并发与过载保护

- 在途总容量 = `MaxWorkers + MaxQueue`（在跑 + 排队）。容量由单锁下的计数严格约束。
- `MaxWorkers` 个常驻 worker 从 FIFO 队列取任务，实际并发绝不超过该值。
- 容量已满：
  - `MaxQueueWait <= 0`：立即返回 `queue_full`；
  - `MaxQueueWait > 0`：有界等待，超时返回 `queue_timeout`。
- 被拒任务**从未进入队列**，不占用执行资源，也不改变已有任务；其终态保留为 `rejected` 可查。
- 因此系统不会无限堆积：内存中任务数受“已登记表”约束，活动在途量受容量约束。

### 2.3 取消

- 排队中取消：立即结算 `canceled` 并移出队列，**绝不执行**。
- 重试等待（`retry_wait`）中取消：立即停止等待定时器并结算 `canceled`，之后**不再发起新的尝试**。
- 执行中取消：立即结算 `canceled`（从此刻起不再有任何可观测的成功/失败结果），
  同时取消任务 `ctx`；任务函数应协作退出。任务函数若在取消后才返回，其返回值会被丢弃。
- 取消已 `succeeded/failed` 的任务返回 `terminal`；对 `canceled/rejected/aborted` 重复取消幂等返回。
- 若任务体因监听 `ctx.Done()` 返回 `context.Canceled`，终态记为 `canceled`（而非 `failed`）。

### 2.4 优雅关闭（`Shutdown`，幂等可重复调用）

1. 置关闭标志，此后所有新提交返回 `shutting_down`；
2. 从未开始执行的排队任务统一结算为 `canceled`，不会执行；
3. 已进入重试周期的任务（`retry_wait` 等待中或重试再入队）**立即停止等待、视为到点**，
   在 `ShutdownGrace` 宽限期内随在跑任务一起尽量执行完，保留其 `succeeded/failed` 结果；
   宽限期内不再安排新的重试轮次（可重试失败直接按 `failed` 结算）；
4. 对在跑任务取消其 `ctx`，并在 `ShutdownGrace`（或传入 `ctx`）宽限期内等待收敛；
5. 宽限期结束仍在跑或未收敛的任务明确标记为 `aborted`，其晚到返回值被丢弃。

每个任务**只结算一次**：所有终态转移都走唯一入口，在单锁内以“仅当非终态才转移”（CAS 语义）完成。
`Stats()` 满足 `succeeded+failed+canceled+rejected+aborted == submitted`，压测程序会强制校验该恒等式。

> 说明：无视取消信号、永不让出的任务其 goroutine 可能在关闭后残留（Go 无法强杀 goroutine），
> 但其状态已被明确标记、晚到结果不会覆盖，服务对外语义保持一致。协作式任务的 worker 会被回收。

### 2.5 失败自动重试（`SubmitWithRetry` + `RetryPolicy`）

- **策略配置**：见下文 `RetryPolicy` 表。零值等价于不重试，`Submit` 行为完全不变。
- **可重试判定**：只有任务函数返回被 `runner.Retryable(err)` 标记的错误才安排下一轮尝试；
  成功、未标记的失败（如参数/数据错误）、panic、`context.Canceled` 都不会重试。
  可用 `runner.IsRetryable(err)` 判定（支持包装链）。
- **等待不占并发**：两次尝试之间任务处于 `retry_wait`，由定时器驱动，不占用 worker 与队列容量；
  对外仍是同一次提交的进行中状态，`Get` 可见 `Attempts`（已尝试次数）、
  `NextRetryAt`（下次尝试时间）与 `Err`（上一次失败原因）。
- **次数用尽的结局**：达到 `MaxAttempts` 后以 `failed` 终结，`TaskState.Attempts` 为总尝试次数，
  `TaskState.Err` 为最后一次失败原因（`errors.Is` 可追溯底层错误）。
- **重新执行受容量约束**：重试到点后按当前在途容量重新入队；容量紧张时在 `retry_wait` 中等待晋升，
  容量随在途任务结算释放后即被调度，不会长期卡住，也不会超过 `MaxAttempts` 无限重试。
- **与幂等的关系**：`retry_wait` 期间同键同内容的提交复用同一任务（阻塞等待最终结局），
  不会另起并行执行；同键不同内容仍返回 `idempotent_conflict`。
- **与取消/关闭的关系**：见 2.3 / 2.4——取消或关闭会立即打断等待中的重试，
  之后不再发起新尝试；某次旧尝试很晚才返回的结果不会覆盖已确定的终态。

---

## 3. 配置项 `runner.Config`

| 字段 | 默认值 | 含义 / 约束 |
| --- | --- | --- |
| `MaxWorkers` | 8 | 最大并发执行数，必须 `>= 1` |
| `MaxQueue` | 1024 | 最大排队长度（在跑之外的在途量），必须 `>= 0`；为 0 时只允许恰好 `MaxWorkers` 个在途 |
| `MaxQueueWait` | 200ms | 队列满时等待空位的最长时间；`<=0` 表示立即拒绝 |
| `ShutdownGrace` | 5s | 关闭时在途任务收敛宽限期；`<=0` 表示不等待，在跑任务立即 `aborted` |

`New` 内部调用 `Config.Validate()`，非法配置返回 `invalid_argument`。

### 重试策略 `runner.RetryPolicy`（按任务指定，见 `SubmitWithRetry`）

| 字段 | 默认值 | 含义 / 约束 |
| --- | --- | --- |
| `MaxAttempts` | 1（即不重试） | 总尝试次数上限（含首次），取值 `[1, 16]`；`0` 归一化为 1，超过硬上限 `MaxRetryAttempts=16` 返回 `invalid_argument`，杜绝实质性无限重试 |
| `Interval` | 0 | 首次重试前的等待间隔，必须 `>= 0`；指数退避时作为基数 |
| `Backoff` | `fixed` | 间隔增长方式：`fixed` 每次相同；`exponential` 逐次翻倍 |
| `MaxInterval` | 30s | 指数退避的间隔封顶；`<=0` 时用默认值；`Interval` 超过它会被收敛到该值 |

非法取值在提交时即返回 `invalid_argument`，任务不会登记。

---

## 4. 本地验证

### 单元测试 / 竞态检测

```bash
go test ./...
go test ./... -race -count=5        # 重复 + 竞态检测
go test ./runner/ -v                # 每个用例打印“输入 + 判定依据”
```

覆盖的关键场景（测试日志中均打印输入与判定依据）：

- 并发同键重复提交只执行一次、同键不同内容 `idempotent_conflict`；
- `queue_full` 立即拒绝、`queue_timeout` 超时拒绝、空位释放后等待者被接纳、并发上限；
- 排队取消绝不执行、运行中取消丢弃晚到结果、取消已终结任务返回 `terminal`；
- 关闭：宽限期内成功收敛、宽限期后 `aborted`、排队任务批量取消、关闭与新提交竞态的多种合法结局；
- 重试：固定/指数退避的间隔与次数、不可重试失败立即终结、次数用尽 `failed` 且保留最后失败原因、
  `retry_wait` 状态可观测（`Attempts`/`NextRetryAt`/上次失败原因）；
- 重试等待期间同键同内容提交复用同一任务、不同内容冲突拒绝；
- 取消打断重试等待后不再发起新尝试；关闭打断重试等待并在宽限期内尽量执行完、
  未收敛则 `aborted` 且晚到结果不覆盖；
- 重试再入队受容量约束（容量被占时留在 `retry_wait`，释放后晋升执行）；
- 瞬时容量拒绝后同键同内容可重新提交并执行成功，统计恒等式保持成立；
- 500 个不同键高并发计数自洽、取消与重复提交竞态下终态唯一。

### 基准测试（吞吐 / 尾延迟 / 过载拒绝）

```bash
go test ./runner -run=^$ -bench=. -benchtime=1s -benchmem
```

参考机器（`Intel Xeon 6982P-C`, GOMAXPROCS=4）一次实测：

| Benchmark | 结果 |
| --- | --- |
| `BenchmarkSubmitThroughput`（16 workers / queue 4096，空任务） | ~2.5µs/op，约 39 万 ops/s，566k 全部成功 |
| `BenchmarkSubmitTailLatency`（8 workers，每任务 20µs） | 受理延迟 p50≈2µs，p95≈60µs，p99≈1.0ms |
| `BenchmarkOverloadedReject`（4 workers + queue16 全占满，wait=0） | ~1.6µs/op，admitted=0，全部即时 rejected |

### 压测程序（可重复运行，输出吞吐/尾延迟/结局计数并做一致性校验）

```bash
# 正常负载
go run ./cmd/stress -workers=8 -queue=256 -n=20000 -conc=64 -work=100µs -grace=1s

# 过载（自动收紧为 2 workers / queue 4 / 5ms 等待）
go run ./cmd/stress -overload -n=20000 -conc=64 -work=5ms -grace=200ms

# 取消 + 幂等复用/冲突混合
go run ./cmd/stress -workers=8 -queue=128 -n=20000 -conc=48 -work=300µs \
  -cancel-pct=20 -dup-pct=30 -grace=1s

# 重试混合（30% 任务首次可重试失败后成功）
go run ./cmd/stress -workers=8 -queue=256 -n=20000 -conc=64 -work=100µs -retry-pct=30 -grace=2s
```

实测示例（GOMAXPROCS=4）：

- 正常负载（20k，64 并发，100µs/任务）：约 **27k 提交/s**，受理 p99≈13ms，全部受理，无 aborted；
- 过载（20k，2 workers/queue4，5ms/任务）：仅 622 被接纳，**19378 个 `queue_timeout` 即时/短时拒绝**，
  服务不堆积，终态合计 == 登记数；
- 取消+幂等混合：成功/重放 14386、`idempotent_conflict` 5379、`queue_timeout` 235，
  服务端 `canceled=2333`，计数恒等式成立。

程序退出码非 0 表示终态计数与登记数不一致（用于回归检测结果丢失/重复结算）。

---

## 5. 代码结构

```
runner/
  config.go       配置与校验
  errors.go       错误码、TaskError、哨兵错误
  task.go         状态、Payload、TaskState、任务记录
  retry.go        重试策略、退避计算、可重试错误标记
  idempotency.go  提交内容指纹
  store.go        幂等登记表（由 Runner 单锁保护）
  runner.go       Runner：提交/查询/取消/关闭主流程与状态机
  dispatcher.go   准入（有界容量）、worker 调度、重试调度、唯一结算入口
  *_test.go       单元/竞态/基准测试
cmd/stress/       可重复运行的本机压测程序
```

设计要点：**单一互斥锁 + `sync.Cond` + FIFO 切片**保护登记表、队列与全部状态转移；
worker 执行用户函数期间不持锁。这样“登记—入队—结算”天然互斥，
避免了通道会合在 `queue=0` 等边界上的死锁，并使每个结局都可从状态与错误码直接解释。
重试等待通过 `time.AfterFunc` 定时器实现：等待期间任务不在队列、不占 worker，
到点后经统一的容量检查重新入队，取消/关闭可在任意时刻安全打断定时器。
