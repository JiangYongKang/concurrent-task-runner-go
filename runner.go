package taskrunner

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Runner 是有界并发的任务执行服务。零值不可用，请用 New 构造。
type Runner struct {
	cfg Config

	mu           sync.Mutex
	tasks        map[string]*taskEntry // 按幂等键索引的任务记录（保留全部历史用于重放/查询）
	closed       bool
	shutdownErr  error         // 第一次 Shutdown 的结果，供后续调用返回同一结果
	shutdownDone chan struct{} // 第一次 Shutdown 完全结束后关闭
	startOne     sync.Once

	jobs chan *taskEntry // 容量 = QueueSize；接收即代表已获得执行承诺

	slots   chan struct{} // 容量 = Workers + QueueSize，用于队列满时的空位等待
	closeCh chan struct{}
	wg      sync.WaitGroup // 跟踪 worker 生命周期

	// 统计计数器。
	submitted  atomic.Uint64
	replayed   atomic.Uint64
	rejected   atomic.Uint64
	cancelled  atomic.Uint64
	unfinished atomic.Uint64
	running    atomic.Int64
}

// Stats 是运行时统计快照。
type Stats struct {
	Queued          int
	Running         int
	SubmittedTotal  uint64
	ReplayedTotal   uint64
	RejectedTotal   uint64
	CancelledTotal  uint64
	UnfinishedTotal uint64
}

// New 校验配置并构造 Runner。零值字段先用 DefaultConfig 补齐。
func New(cfg Config) (*Runner, error) {
	if cfg.Workers == 0 {
		d := DefaultConfig()
		if cfg.QueueSize == 0 {
			cfg.QueueSize = d.QueueSize
		}
		if cfg.QueueWaitTimeout == 0 {
			cfg.QueueWaitTimeout = d.QueueWaitTimeout
		}
		if cfg.ShutdownGrace == 0 {
			cfg.ShutdownGrace = d.ShutdownGrace
		}
		cfg.Workers = d.Workers
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	capacity := cfg.Workers + cfg.QueueSize
	r := &Runner{
		cfg:          cfg,
		tasks:        make(map[string]*taskEntry),
		jobs:         make(chan *taskEntry, cfg.QueueSize),
		slots:        make(chan struct{}, capacity),
		closeCh:      make(chan struct{}),
		shutdownDone: make(chan struct{}),
	}
	// 构造后即启动工作池，避免“未 Start 即 Shutdown”时任务永远无人收尾。
	// Start 本身幂等，调用方再次调用不会产生额外 worker。
	r.Start()
	return r, nil
}

// Start 启动工作池，重复调用是幂等的。
func (r *Runner) Start() {
	r.startOne.Do(func() {
		for i := 0; i < r.cfg.Workers; i++ {
			r.wg.Add(1)
			go r.worker(i)
		}
	})
}

// Submit 提交任务。
//
// 语义：
//   - 参数非法：ErrInvalidArgument；
//   - 同一幂等键 + 相同载荷：幂等重放，Replay=true，绝不重复执行；
//   - 同一幂等键 + 不同载荷：ErrIdempotencyConflict，拒绝且不影响已有任务；
//   - 容量耗尽且 QueueWaitTimeout 内无空位：ErrOverloaded；
//   - 服务关闭中/已关闭：ErrRunnerClosed；
//   - 调用方 ctx 在等待空位期间结束：返回 ctx 错误，任务不会被登记。
func (r *Runner) Submit(ctx context.Context, req SubmitRequest) (*SubmitResult, error) {
	if req.IdempotencyKey == "" {
		return nil, NewError(KindInvalidArgument, "IdempotencyKey must not be empty")
	}
	if req.Func == nil {
		return nil, NewError(KindInvalidArgument, "Func must not be nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// 第一步：持锁做关闭判定与幂等判定。重放与冲突不占用容量槽位，
	// 因而重放/冲突流量既不会触发执行，也不会挤占真实新任务。
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil, ErrRunnerClosed
	}
	existing, dup := r.tasks[req.IdempotencyKey]
	r.mu.Unlock()
	if dup {
		if existing.payloadHash == hashPayload(req.Payload) {
			r.replayed.Add(1)
			return &SubmitResult{Replay: true, Task: existing.snapshot()}, nil
		}
		r.rejected.Add(1)
		return nil, NewError(KindIdempotencyConflict,
			"key %q already used with a different payload (existing hash=%s, submitted hash=%s)",
			req.IdempotencyKey, existing.payloadHash, hashPayload(req.Payload))
	}

	// 第二步：为“真正的新任务”尝试获得一个容量槽位。
	// 失败时任务尚未登记，不改变任何已有任务状态。
	if !r.acquireSlot(ctx) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if r.isClosed() {
			return nil, ErrRunnerClosed
		}
		r.rejected.Add(1)
		return nil, ErrOverloaded
	}

	// 第三步：再次持锁登记（等待期间服务可能关闭，或同键提交已抢先登记）。
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		r.releaseSlot()
		return nil, ErrRunnerClosed
	}
	if other, ok := r.tasks[req.IdempotencyKey]; ok {
		r.mu.Unlock()
		r.releaseSlot()
		if other.payloadHash == hashPayload(req.Payload) {
			r.replayed.Add(1)
			return &SubmitResult{Replay: true, Task: other.snapshot()}, nil
		}
		r.rejected.Add(1)
		return nil, NewError(KindIdempotencyConflict,
			"key %q already used with a different payload (existing hash=%s, submitted hash=%s)",
			req.IdempotencyKey, other.payloadHash, hashPayload(req.Payload))
	}
	entry, err := newTaskEntry(req)
	if err != nil {
		r.mu.Unlock()
		r.releaseSlot()
		return nil, err
	}
	// 容量槽位保证此刻有空位，发送不会阻塞；jobs 仅在关闭后被 close，
	// 而上面已经把 closed 挡在门外，因此此处不需要 default 分支。
	r.jobs <- entry
	r.tasks[req.IdempotencyKey] = entry
	r.mu.Unlock()

	r.submitted.Add(1)
	return &SubmitResult{Replay: false, Task: entry.snapshot()}, nil
}

// acquireSlot 尝试占用一个容量槽位；先做非阻塞尝试，再按配置等待。
// 返回 false 表示未取得槽位（ctx 取消、超时或关闭）。
func (r *Runner) acquireSlot(ctx context.Context) bool {
	// 非阻塞快速路径：立即拿到空位。
	select {
	case r.slots <- struct{}{}:
		return true
	default:
	}

	if r.cfg.QueueWaitTimeout <= 0 {
		// 0 表示不等待，直接拒绝（仍允许 ctx 立即就绪的极端情形）。
		select {
		case r.slots <- struct{}{}:
			return true
		case <-r.closeCh:
			return false
		case <-ctx.Done():
			return false
		default:
			return false
		}
	}

	timer := time.NewTimer(r.cfg.QueueWaitTimeout)
	defer timer.Stop()
	select {
	case r.slots <- struct{}{}:
		return true
	case <-timer.C:
		return false
	case <-r.closeCh:
		return false
	case <-ctx.Done():
		return false
	}
}

func (r *Runner) releaseSlot() {
	<-r.slots
}

func (r *Runner) isClosed() bool {
	select {
	case <-r.closeCh:
		return true
	default:
		return false
	}
}

// Get 按幂等键查询任务快照；不存在返回 ErrNotFound。
func (r *Runner) Get(ctx context.Context, key string) (*TaskInfo, error) {
	if key == "" {
		return nil, NewError(KindInvalidArgument, "key must not be empty")
	}
	r.mu.Lock()
	entry, ok := r.tasks[key]
	r.mu.Unlock()
	if !ok {
		return nil, NewError(KindNotFound, "no task with key %q", key)
	}
	return entry.snapshot(), nil
}

// Cancel 请求取消任务。
//
//   - 排队中：立即转为 CANCELLED，worker 取到时跳过，绝不执行；
//   - 执行中：取消其上下文，由任务协作式退出；若任务无视取消而返回结果，
//     该结果会被状态机丢弃（取消后不存在可观察到的完成结果）；
//   - 已终态：幂等 no-op，返回当前快照而不报错；
//   - 不存在：ErrNotFound。
func (r *Runner) Cancel(ctx context.Context, key string) error {
	if key == "" {
		return NewError(KindInvalidArgument, "key must not be empty")
	}
	r.mu.Lock()
	entry, ok := r.tasks[key]
	r.mu.Unlock()
	if !ok {
		return NewError(KindNotFound, "no task with key %q", key)
	}
	if entry.requestCancel() {
		r.cancelled.Add(1)
	}
	return nil
}

// Shutdown 优雅关闭。
//
// 顺序：
//  1. 标记关闭：停止接受新提交（Submit 返回 ErrRunnerClosed）；
//  2. 停止派发：关闭 jobs，已在队列中的任务仍会被 worker 取出处理；
//  3. 等待 ShutdownGrace：让在途与排队任务自然收敛；
//  4. 宽限期（或传入 ctx）结束后仍未终态的任务统一标记为 UNFINISHED，
//     并取消其执行上下文，随后按时返回错误；
//  5. 全部自然收敛时等待 worker 退出后返回 nil，返回后状态不再变化，
//     保证不重复结算、不丢失结果。
//
// 注意：Go 无法强制终止 goroutine。若任务函数无视 ctx 取消且永不返回，
// 宽限期结束后其状态仍会被标记为 UNFINISHED（迟到结果由状态机丢弃），
// 该 goroutine 将随进程生命周期结束，Shutdown 不会因此无限等待。
//
// ShutdownGrace < 0 表示无限等待（仍受传入 ctx 约束）；
// 多次调用并发安全，且返回与第一次调用相同的结果。
func (r *Runner) Shutdown(ctx context.Context) error {
	r.mu.Lock()
	if r.closed {
		done := r.shutdownDone
		r.mu.Unlock()
		<-done // 等待第一次 Shutdown 收敛，再返回其相同结果
		r.mu.Lock()
		err := r.shutdownErr
		r.mu.Unlock()
		return err
	}
	r.closed = true
	close(r.closeCh)
	close(r.jobs) // 缓冲中剩余任务仍可被 worker 取出处理
	pending := make([]*taskEntry, 0, len(r.tasks))
	for _, e := range r.tasks {
		pending = append(pending, e)
	}
	r.mu.Unlock()

	grace := r.cfg.ShutdownGrace
	var deadlineCh <-chan time.Time
	if grace >= 0 {
		timer := time.NewTimer(grace)
		defer timer.Stop()
		deadlineCh = timer.C
	}

	allDone := make(chan struct{})
	go func() {
		// 等待所有已登记任务完成（含排队任务被处理或跳过后关闭 done）。
		for _, e := range pending {
			<-e.done
		}
		close(allDone)
	}()

	var shutdownErr error
	select {
	case <-allDone:
		// 全部自然收敛：等待 worker 退出，此后状态绝不再变。
		r.wg.Wait()
	case <-deadlineCh:
		shutdownErr = NewError(KindRunnerClosed,
			"shutdown grace period %s elapsed, unfinished tasks marked UNFINISHED", grace)
	case <-ctx.Done():
		shutdownErr = ctx.Err()
	}

	// 宽限期/ctx 到期：所有未终态任务明确标记 UNFINISHED（状态 CAS 保证不覆盖终态）。
	if shutdownErr != nil {
		var n uint64
		for _, e := range pending {
			if e.markUnfinished() {
				n++
			}
		}
		if n > 0 {
			r.unfinished.Add(n)
		}
		// 不等待无视 ctx 的任务 goroutine，保证 Shutdown 按时返回；
		// 它们迟到的 settleResult 会因状态已非 Running 而被丢弃。
	}

	r.mu.Lock()
	r.shutdownErr = shutdownErr
	done := r.shutdownDone
	r.mu.Unlock()
	close(done) // 广播：第一次 Shutdown 已完全结束
	return shutdownErr
}

// Stats 返回统计快照。Queued 取“已提交但尚未被 worker 领取”的任务数。
func (r *Runner) Stats() Stats {
	s := Stats{
		Running:         int(r.running.Load()),
		SubmittedTotal:  r.submitted.Load(),
		ReplayedTotal:   r.replayed.Load(),
		RejectedTotal:   r.rejected.Load(),
		CancelledTotal:  r.cancelled.Load(),
		UnfinishedTotal: r.unfinished.Load(),
	}
	s.Queued = len(r.jobs)
	return s
}

// worker 从 jobs 领取任务并负责其完整生命周期收尾，包括释放容量槽位
// 与关闭 entry.done（恰好一次）。
func (r *Runner) worker(id int) {
	defer r.wg.Done()
	for {
		select {
		case <-r.closeCh:
			// 关闭后仍要排空已入队任务（jobs 已 close，range 语义）。
			for entry := range r.jobs {
				r.runOne(entry)
			}
			return
		case entry, ok := <-r.jobs:
			if !ok {
				return
			}
			r.runOne(entry)
		}
	}
}

// runOne 处理单个任务。无论执行、跳过还是 panic，都保证：
// 槽位被释放、entry.done 被关闭，且任务至多结算一次。
func (r *Runner) runOne(entry *taskEntry) {
	defer func() {
		entry.cancel() // 幂等：释放 ctx 子树，避免 WithCancel 泄漏；取消路径重复调用无害
		r.releaseSlot()
		entry.finishOnce()
	}()

	if !entry.beginRunning() {
		// 入队后、领取前已被取消/标记：跳过，绝不执行。
		entry.cancel()
		return
	}

	r.running.Add(1)
	defer r.running.Add(-1)

	result, err := r.invoke(entry)
	// settleResult 内部做状态 CAS：仅 Running 才能写入结果；
	// 若执行期间发生取消/UNFINISHED，结果被丢弃，状态以先到的终态为准。
	entry.settleResult(result, err)
}

// invoke 执行任务体，隔离 panic 并透传任务上下文取消。
func (r *Runner) invoke(entry *taskEntry) (result []byte, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("task function panicked: %v", rec)
			result = nil
		}
	}()
	return entry.fn(entry.runCtx, entry.payload)
}
