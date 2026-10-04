# concurrent-task-runner-go

一个本地可运行的内存并发任务执行服务。目标是让任务在**并发提交、过载、取消、服务关闭、失败重试**等情形下，
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

// 带重试策略的提交：最多尝试 3 次，间隔 100ms 起步、指数拉长、封顶 1s
state, err = r.Submit(ctx, "order-456",
    runner.Payload{
        Name:   "charge",
        Params: map[string]any{"amount": 200},
        Retry: &runner.RetryPolicy{
            MaxAttempts: 3,
            Interval:    100 * time.Millisecond,
            Backoff:     runner.BackoffExponential,
            MaxInterval: time.Second,
        },
    },
    func(ctx context.Context) (any, error) {
        // 返回普通 error => 视为瞬时失败，按策略重试；
        // 返回 runner.NonRetryable(err) => 永久失败，立即进入 failed 终态。
        return "done", nil
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
| `retry_wait` | 上一次尝试失败，等待下一次重试（不占执行并发） | 否 |
| `succeeded` | 执行成功，结果在 `TaskState.Result` | 是 |
| `failed` | 任务函数返回错误或 panic（已 recover），含重试耗尽 | 是 |
| `canceled` | 被 `Cancel` 或关闭流程取消（在途） | 是 |
| `rejected` | 因过载或关闭被拒绝受理执行 | 是 |
| `aborted` | 关闭宽限期结束仍未结束（在跑/排队/等待重试） | 是 |

终态由 `status.IsTerminal()` 判定；终态任务的结果与状态不再变化。
`retry_wait` 期间 `TaskState.Attempts` 为已尝试次数、`TaskState.Err` 为最近一次失败原因；
重试耗尽进入 `failed` 后，二者即“总共尝试了几次、最后一次为什么失败”。

### 失败原因码 `runner.Code`（`*runner.TaskError.Code`，也可用 `errors.Is` 配哨兵错误）

| Code | 哨兵错误 | 触发入口 | 含义 |
| --- | --- | --- | --- |
| `invalid_argument` | `ErrInvalidArgument` | Submit / Config | 幂等键为空、`fn==nil`、重试策略或配置非法 |
| `idempotent_conflict` | `ErrIdempotentConflict` | Submit | 同幂等键复用但 `Payload`（Name/Params/Retry）不同 |
| `not_found` | `ErrNotFound` | Cancel（Get 用 `ok=false` 表达） | 任务不存在 |
| `queue_full` | `ErrQueueFull` | Submit | 在途容量已满且 `MaxQueueWait<=0`，立即拒绝 |
| `queue_timeout` | `ErrQueueTimeout` | Submit / 重试再准入 | 容量满，等待 `MaxQueueWait` 仍无空位 |
| `shutting_down` | `ErrShuttingDown` | Submit | 关闭开始后的新提交 |
| `task_canceled` | `ErrTaskCanceled` | Cancel / 等待结果 / 协作退出 | 任务被取消 |
| `terminal` | `ErrTerminal` | Cancel | 已 succeeded/failed，不能再取消 |
| `aborted` | `ErrAborted` | 关闭 | 宽限期结束仍未结束 |
| `task_failed` | — | 任务执行业务错误 | succeeded 之外的业务失败，体现在 `failed` 终态 |

三类“找不到/参数错/键冲突”严格区分，可直接按 `Code` 分支处理。

---

## 2. 关键语义

### 2.1 提交与幂等重放

- 以 `idemKey` 为唯一标识登记；提交内容用 `Payload{Name, Params, Retry}` 的确定性哈希作为**指纹**。
- **同键 + 同内容**：不重复执行。已终结则立即重放同一份最终状态/结果；仍在执行（含 `retry_wait`）则阻塞到终结再重放。
- **同键 + 不同内容**（含重试策略不同）：返回 `idempotent_conflict`，已有任务完全不受影响，本次也不会执行。
- 对 `canceled/aborted/failed` 终态的同内容重复提交：重放当时的失败原因（不重新执行）。
- **对 `rejected` 终态的同内容重复提交**：瞬时容量/关闭拒绝不会“毒化”幂等键——
  会登记为一个新任务重新走准入，容量恢复后即可正常执行；旧任务的 `rejected` 终态已结算并计入统计，不受影响。
- 高并发下同键重复提交只有一个任务真正执行（见 `TestConcurrentDuplicateSubmit`）。

### 2.2 有界并发与过载保护

- 在途总容量 = `MaxWorkers + MaxQueue`（在跑 + 排队）。容量由单锁下的计数严格约束。
- `MaxWorkers` 个常驻 worker 从 FIFO 队列取任务，实际并发绝不超过该值。
- 容量已满：
  - `MaxQueueWait <= 0`：立即返回 `queue_full`；
  - `MaxQueueWait > 0`：有界等待，超时返回 `queue_timeout`。
- 被拒任务**从未进入队列**，不占用执行资源，也不改变已有任务；其终态保留为 `rejected` 可查。
- 因此系统不会无限堆积：内存中任务数受“已登记表”约束，活动在途量受容量约束。

### 2.3 失败重试（可选，默认关闭）

- 在 `Payload.Retry` 指定 `RetryPolicy` 即开启重试；**不指定（nil）或零值时行为与上一版完全一致**（只尝试一次）。
- 失败分类：
  - 返回普通 `error`：视为**瞬时失败**，值得重试；
  - 返回 `runner.NonRetryable(err)` 包装的错误：视为**永久失败**（参数/数据明显不对等），立即进入 `failed`，不再重试；
  - panic：recover 后直接 `failed`，不重试；
  - 因任务 ctx 被取消而退出：终态 `canceled`，不重试。
- 每次失败后的等待间隔由策略决定（见 §3.1），等待期间任务处于 `retry_wait`：
  - **不占用 worker 与队列容量**（不计入在途容量），但对外仍是同一次提交、处于进行中；
  - 同键同内容的重复提交复用同一任务（阻塞到终态重放），同键不同内容仍 `idempotent_conflict`；
  - `Cancel` 会立即停掉重试定时器并结算 `canceled`，之后不再发起任何新尝试。
- 重试到期后**重新走容量准入**：容量满时按 `MaxQueueWait` 有界等待空位；
  等待期间被取消/关闭结算则立即放弃；超时仍无空位则结算为 `failed`（原因码 `queue_timeout`）——
  任何情况下任务都有确定结局，不会卡在既不执行也不结束的状态。
- 重试次数有硬上限（`MaxRetryAttempts=32`），间隔有硬上限（`MaxRetryInterval=5min`），
  从机制上杜绝“实质性无限重试”。
- 重试耗尽后进入 `failed` 终态：`TaskState.Attempts` 为总尝试次数，`TaskState.Err` 为最后一次失败原因；
  同键同内容的重复提交重放该失败结局，不会重新执行。

### 2.4 取消

- 排队中取消：立即结算 `canceled` 并移出队列，**绝不执行**。
- 重试等待中取消：立即停掉重试定时器并结算 `canceled`，之后不再发起新尝试。
- 执行中取消：立即结算 `canceled`（从此刻起不再有任何可观测的成功/失败结果），
  同时取消任务 `ctx`；任务函数应协作退出。任务函数若在取消后才返回，其返回值会被丢弃，
  即使返回的是“值得重试”的错误也不会再触发重试。
- 取消已 `succeeded/failed` 的任务返回 `terminal`；对 `canceled/rejected/aborted` 重复取消幂等返回。
- 若任务体因监听 `ctx.Done()` 返回 `context.Canceled`，终态记为 `canceled`（而非 `failed`）。

### 2.5 优雅关闭（`Shutdown`，幂等可重复调用）

1. 置关闭标志，此后所有新提交返回 `shutting_down`；
2. 对在跑任务取消其 `ctx`，促使协作式任务尽快收尾；
3. **宽限期内排空**：已受理的排队任务继续被派发执行，`retry_wait` 中的任务到期后仍重新准入执行，
   成功/失败结果如实保留；
4. 宽限期（`ShutdownGrace` 或传入 `ctx`）结束仍未收敛的任务——无论在跑、排队还是等待重试——
   统一结算为 `aborted`，重试定时器被停掉，之后不再发起任何新尝试；
5. 无视取消信号、晚到的返回值一律丢弃，不会覆盖已确定的结局。

> 行为变更说明：上一版在关闭开始时会立即把排队任务结算为 `canceled`；
> 本轮起改为“宽限期内尽量执行完”，只有宽限期结束仍未收敛的才标记为 `aborted`。

每个任务**只结算一次**：所有终态转移都走唯一入口，在单锁内以“仅当非终态才转移”（CAS 语义）完成。
`Stats()` 满足 `succeeded+failed+canceled+rejected+aborted == submitted`，压测程序会强制校验该恒等式，
并额外校验关闭后 `retrying == 0`（无残留重试等待）。

> 说明：无视取消信号、永不让出的任务其 goroutine 可能在关闭后残留（Go 无法强杀 goroutine），
> 但其状态已被明确标记、晚到结果不会覆盖，服务对外语义保持一致。协作式任务的 worker 会被回收。

---

## 3. 配置项 `runner.Config`

| 字段 | 默认值 | 含义 / 约束 |
| --- | --- | --- |
| `MaxWorkers` | 8 | 最大并发执行数，必须 `>= 1` |
| `MaxQueue` | 1024 | 最大排队长度（在跑之外的在途量），必须 `>= 0`；为 0 时只允许恰好 `MaxWorkers` 个在途 |
| `MaxQueueWait` | 200ms | 队列满时等待空位的最长时间（重试再准入复用同一窗口）；`<=0` 表示立即拒绝 |
| `ShutdownGrace` | 5s | 关闭时在途任务收敛宽限期；`<=0` 表示不等待，未收敛任务立即 `aborted` |

`New` 内部调用 `Config.Validate()`，非法配置返回 `invalid_argument`。

### 3.1 重试策略 `runner.RetryPolicy`（按任务指定，放在 `Payload.Retry`）

| 字段 | 默认值 | 含义 / 约束 |
| --- | --- | --- |
| `MaxAttempts` | 1（不重试） | 总尝试次数上限（含首次），取值 `[1, MaxRetryAttempts=32]`；`0` 等价于 `1` |
| `Interval` | 0 | 重试基础间隔；`MaxAttempts>1` 时必须 `>0` 且 `<= MaxRetryInterval=5min` |
| `Backoff` | `BackoffFixed` | `BackoffFixed` 固定间隔 / `BackoffExponential` 每次翻倍逐步拉长 |
| `MaxInterval` | 0（不封顶） | 指数退避的间隔上限；`>0` 时必须 `>= Interval` 且 `<= MaxRetryInterval` |

- 策略随 `Payload` 参与幂等指纹：同键但策略不同视为冲突提交。
- 非法策略在 `Submit` 时返回 `invalid_argument`，不会登记任务。
- 指数退避第 k 次失败后的等待为 `Interval * 2^(k-1)`，受 `MaxInterval`（及硬上限 `MaxRetryInterval`）封顶。

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
- 重试：固定/指数间隔与封顶、重试耗尽后 `failed`（尝试次数+最后失败原因可查）、
  `NonRetryable` 不重试、`retry_wait` 期间同键复用与冲突、取消打断重试、
  运行中取消后晚到的可重试错误不触发新尝试；
- 容量与重试：重试再准入遇容量满且不可等待时确定结局 `failed(queue_timeout)`；
  瞬时 `queue_full` 拒绝后同键同内容可重新提交并成功（键不被毒化）；
- 关闭：宽限期内排队/重试等待任务排空执行并保留结果、宽限期后统一 `aborted`、
  关闭期间新提交拒绝、关闭与新提交竞态的多种合法结局；
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

# 重试混合：30% 任务带指数退避重试且前两次瞬时失败
go run ./cmd/stress -workers=8 -queue=256 -n=20000 -conc=64 -work=100µs -retry-pct=30 -grace=2s

# 过载 + 重试：观察重试再准入在容量紧张下的确定结局
go run ./cmd/stress -overload -n=10000 -conc=64 -work=2ms -retry-pct=40 -grace=500ms
```

实测示例（GOMAXPROCS=4）：

- 正常负载（20k，64 并发，100µs/任务）：约 **27k 提交/s**，受理 p99≈13ms，全部受理，无 aborted；
- 过载（20k，2 workers/queue4，5ms/任务）：仅 622 被接纳，**19378 个 `queue_timeout` 即时/短时拒绝**，
  服务不堆积，终态合计 == 登记数；
- 取消+幂等混合：成功/重放 14386、`idempotent_conflict` 5379、`queue_timeout` 235，
  服务端 `canceled=2333`，计数恒等式成立；
- 重试混合（20k，retry-pct=30）：全部受理，重试任务第三次尝试成功，
  终态合计 == 登记数且关闭后 `retrying=0`。

程序退出码非 0 表示终态计数与登记数不一致，或关闭后仍有残留的重试等待（用于回归检测结果丢失/重复结算）。

---

## 5. 代码结构

```
runner/
  config.go       配置与校验
  errors.go       错误码、TaskError、哨兵错误
  task.go         状态、Payload、TaskState、任务记录
  retry.go        重试策略、退避计算、可重试性判定（NonRetryable）
  idempotency.go  提交内容指纹
  store.go        幂等登记表（由 Runner 单锁保护）
  runner.go       Runner：提交/查询/取消/关闭主流程与状态机
  dispatcher.go   准入（有界容量）、worker 调度、重试调度、唯一结算入口
  *_test.go       单元/竞态/基准测试
cmd/stress/       可重复运行的本机压测程序
```

设计要点：**单一互斥锁 + `sync.Cond` + FIFO 切片**保护登记表、队列与全部状态转移；
worker 执行用户函数期间不持锁；重试等待由 `time.Timer` 驱动、到期后在同一把锁内重新准入。
这样“登记—入队—重试—结算”天然互斥，
避免了通道会合在 `queue=0` 等边界上的死锁，并使每个结局都可从状态与错误码直接解释。

