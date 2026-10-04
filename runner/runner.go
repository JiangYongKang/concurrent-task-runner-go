package runner

import (
	"context"
	"sync"
	"time"
)

// Stats 是 Runner 的计数快照。Submitted 为累计登记数，
// Queued/Running 为当前在队/在跑的瞬时值，其余为各终态累计数。
type Stats struct {
	Submitted int64
	Queued    int64
	Running   int64
	Succeeded int64
	Failed    int64
	Canceled  int64
	Rejected  int64
	Aborted   int64
}

// Runner 是并发任务执行服务。
//
// 并发模型：
//   - 单一互斥锁保护登记表、FIFO 队列与全部状态转移，
//     保证“登记—入队—结算”互斥、每任务恰好结算一次且可解释；
//   - MaxWorkers 个常驻工作协程通过 sync.Cond 等待队列；
//   - 在途容量为 MaxWorkers + MaxQueue，容量耗尽的新提交被
//     有界等待或明确拒绝，绝不无限堆积。
type Runner struct {
	cfg Config

	mu   sync.Mutex
	cond *sync.Cond

	store    *store
	queue    []*Task // 已接纳、等待派发的 FIFO 队列
	running  map[string]*Task
	nRunning int

	retryDue   []*Task // 重试定时已到点、等待容量晋升的任务（FIFO）
	nRetryWait int     // 处于 retry_wait 状态的任务数（含已到点待晋升者）

	stats    Stats
	shutdown bool
	stopCh   chan struct{} // 关闭开始时关闭，用于唤醒看门狗

	wg        sync.WaitGroup
	closeOnce sync.Once
}

// New 创建一个 Runner 并立即启动工作协程。
func New(cfg Config) (*Runner, error) {
	cfg, err := cfg.Validate()
	if err != nil {
		return nil, err
	}
	r := &Runner{
		cfg:     cfg,
		store:   newStore(),
		running: make(map[string]*Task),
		stopCh:  make(chan struct{}),
	}
	r.cond = sync.NewCond(&r.mu)
	r.startWorkers()
	return r, nil
}

// Submit 提交任务（不重试，等价于 SubmitWithRetry 传入零值 RetryPolicy）。
//
// 幂等语义：
//   - idemKey 首次出现：登记并尝试在有界容量内执行；
//   - 相同 idemKey + 相同 Payload：不重复执行，返回同一任务的最终/当前状态；
//   - 相同 idemKey + 不同 Payload：返回 CodeIdempotentConflict，原任务不受影响；
//   - 对已终结任务的重复提交直接重放其结果（成功无错；拒绝类终态返回当时的失败原因），
//     但因容量不足（queue_full/queue_timeout）被拒的任务允许同内容再次提交并重新执行。
//
// 并发重复提交且任务尚未终结时，调用会阻塞至任务终结或 ctx 取消。
func (r *Runner) Submit(ctx context.Context, idemKey string, p Payload, fn Func) (*TaskState, error) {
	return r.submit(ctx, idemKey, p, fn, RetryPolicy{})
}

// SubmitWithRetry 提交任务并指定自动重试策略。
//
// 除重试外语义与 Submit 完全一致（同一幂等登记表、同一容量约束）：
//   - 仅当任务函数返回被 Retryable 标记的错误时才安排下一轮尝试；
//   - 两次尝试之间的等待不占执行并发，任务对外处于 retry_wait（进行中）；
//   - 等待期间的同键同内容提交复用同一任务（阻塞等待最终结局）；
//   - 尝试次数耗尽后以 failed 终结，TaskState.Attempts 记录总尝试次数，
//     TaskState.Err 为最后一次失败原因。
func (r *Runner) SubmitWithRetry(ctx context.Context, idemKey string, p Payload, fn Func, policy RetryPolicy) (*TaskState, error) {
	np, err := policy.normalize()
	if err != nil {
		return nil, err
	}
	return r.submit(ctx, idemKey, p, fn, np)
}

func (r *Runner) submit(ctx context.Context, idemKey string, p Payload, fn Func, policy RetryPolicy) (*TaskState, error) {
	if idemKey == "" {
		return nil, newError("submit", CodeInvalidArgument, "idemKey must not be empty", nil)
	}
	if fn == nil {
		return nil, newError("submit", CodeInvalidArgument, "fn must not be nil", nil)
	}
	fp := fingerprint(p)

	r.mu.Lock()
	if existing, ok := r.store.get(idemKey); ok {
		if existing.fingerprint != fp {
			snap := existing.snapshot()
			r.mu.Unlock()
			return &snap, newError("submit", CodeIdempotentConflict,
				"idempotency key reused with a different payload", nil)
		}
		if existing.state.Status.IsTerminal() {
			if revivableReject(existing) {
				// 瞬时容量不足（queue_full/queue_timeout）的被拒任务：
				// 同内容再次提交时以新记录替换重新登记执行，
				// 该幂等键不会因一次瞬时拒绝而永久不可用。
				t := newTask(idemKey, fp, p, fn, policy)
				r.store.put(idemKey, t)
				r.stats.Submitted++
				if err := r.admitLocked(t); err != nil {
					snap := t.snapshot()
					r.mu.Unlock()
					return &snap, err
				}
				snap := t.snapshot()
				r.mu.Unlock()
				return &snap, nil
			}
			snap := existing.snapshot()
			err := terminalCause(existing)
			r.mu.Unlock()
			return &snap, err
		}
		done := existing.done
		r.mu.Unlock()

		select {
		case <-done:
		case <-ctx.Done():
			r.mu.Lock()
			snap := existing.snapshot()
			r.mu.Unlock()
			return &snap, ctx.Err()
		}
		r.mu.Lock()
		snap := existing.snapshot()
		err := terminalCause(existing)
		r.mu.Unlock()
		return &snap, err
	}

	t := newTask(idemKey, fp, p, fn, policy)
	r.store.putIfAbsent(idemKey, t)
	r.stats.Submitted++
	if err := r.admitLocked(t); err != nil {
		snap := t.snapshot()
		r.mu.Unlock()
		return &snap, err
	}
	snap := t.snapshot()
	r.mu.Unlock()
	return &snap, nil
}

// Get 查询任务状态；任务不存在时 ok=false（对应 CodeNotFound 语义）。
func (r *Runner) Get(ctx context.Context, idemKey string) (*TaskState, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	t, ok := r.store.get(idemKey)
	if !ok {
		return nil, false
	}
	snap := t.snapshot()
	return &snap, true
}

// Cancel 请求取消任务。
//
//   - 排队中：立即结算为 canceled 并移出队列，绝不执行；
//   - 重试等待中（retry_wait）：立即停止等待定时器并结算为 canceled，
//     之后不再发起新的尝试；
//   - 执行中：立即结算为 canceled（此后不再有可观测的成功/失败结果），
//     并通过任务 ctx 发出取消信号，业务函数应协作退出；
//   - 已 canceled/rejected/aborted：幂等返回当前状态；
//   - 已 succeeded/failed：返回 CodeTerminal；
//   - 不存在：返回 CodeNotFound。
func (r *Runner) Cancel(ctx context.Context, idemKey string, reason string) (*TaskState, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	t, ok := r.store.get(idemKey)
	if !ok {
		return nil, newError("cancel", CodeNotFound, "task not found", nil)
	}
	if t.state.Status.IsTerminal() {
		snap := t.snapshot()
		switch t.state.Status {
		case StatusSucceeded, StatusFailed:
			return &snap, newError("cancel", CodeTerminal,
				"task already finished: "+string(t.state.Status), nil)
		default:
			return &snap, nil
		}
	}

	cause := newError("cancel", CodeTaskCanceled, "canceled: "+reason, context.Canceled)
	if t.state.Status == StatusQueued {
		r.removeFromQueueLocked(t)
	} else {
		r.stopRetryWaitLocked(t) // retry_wait：停表并移出待晋升队列
	}
	r.settleLocked(t, StatusCanceled, nil, cause)
	if t.cancel != nil {
		t.cancel()
	}
	snap := t.snapshot()
	return &snap, nil
}

// Shutdown 优雅关闭（可重复调用，仅首次生效）。
//
//  1. 标记关闭，拒绝一切新提交；
//  2. 从未开始执行的排队任务立即结算为 canceled 并移出队列，不会执行；
//  3. 已进入重试周期的任务（retry_wait 等待中或重试再入队）立即停止等待、
//     视为到点，在 ShutdownGrace 宽限期内随在跑任务一起尽量执行完，
//     保留其成功/失败结果；宽限期内不再安排新的重试轮次；
//  4. 对在跑任务发出 ctx 取消信号，并在宽限期（或传入 ctx）内等待收敛；
//  5. 宽限期结束后仍在跑/仍未收敛的任务结算为 aborted；
//  6. 每个任务只结算一次，结果保留可查；无视取消信号的任务其协程可能残留，
//     但其晚到结果会被丢弃，不会再改变状态。
func (r *Runner) Shutdown(ctx context.Context) error {
	r.closeOnce.Do(func() {
		r.beginShutdown()
		r.waitConvergence(ctx)
		r.finishShutdown()
	})
	return nil
}

func (r *Runner) beginShutdown() {
	r.mu.Lock()
	r.shutdown = true
	close(r.stopCh)

	cause := newError("shutdown", CodeShuttingDown, "canceled while queued at shutdown", context.Canceled)
	// 复制当前队列后统一处理，避免边遍历边切片操作。
	queued := append([]*Task(nil), r.queue...)
	r.queue = r.queue[:0]
	for _, t := range queued {
		if t.state.Status.IsTerminal() {
			continue
		}
		if t.state.Attempts == 0 {
			// 从未开始执行的排队任务：保持原语义，立即取消。
			r.settleLocked(t, StatusCanceled, nil, cause)
			continue
		}
		// 重试再入队的任务（已至少尝试一次）：留在队首，宽限期内尽量执行完。
		r.queue = append(r.queue, t)
	}
	// 重试等待中的任务：停止等待定时器、立即到点入队，在宽限期内尽量执行完。
	for _, t := range r.store.all() {
		if t.state.Status == StatusRetryWait {
			r.stopRetryWaitLocked(t)
			t.state.Status = StatusQueued
			r.queue = append(r.queue, t)
		}
	}
	for _, t := range r.running {
		if t.cancel != nil {
			t.cancel()
		}
	}
	r.cond.Broadcast()
	r.mu.Unlock()
}

func (r *Runner) waitConvergence(ctx context.Context) {
	deadline := nowFunc().Add(r.cfg.ShutdownGrace)
	stopWatch := make(chan struct{})
	defer close(stopWatch)
	go func() {
		d := time.Until(deadline)
		if d < 0 {
			d = 0
		}
		timer := time.NewTimer(d)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
		case <-stopWatch:
		}
		r.cond.Broadcast()
	}()

	r.mu.Lock()
	for r.nRunning > 0 || len(r.queue) > 0 || r.nRetryWait > 0 {
		if !nowFunc().Before(deadline) || ctx.Err() != nil {
			break
		}
		r.cond.Wait()
	}
	r.mu.Unlock()
}

func (r *Runner) finishShutdown() {
	r.mu.Lock()
	cause := newError("shutdown", CodeAborted, "still running after shutdown grace", context.DeadlineExceeded)
	for _, t := range r.running {
		if t.state.Status == StatusRunning {
			r.settleLocked(t, StatusAborted, nil, cause)
			if t.cancel != nil {
				t.cancel()
			}
		}
	}
	// 宽限期内未能收敛的重试再入队/重试等待任务：统一标记为未完成。
	queuedCause := newError("shutdown", CodeAborted, "not finished within shutdown grace", context.DeadlineExceeded)
	for _, t := range r.queue {
		if !t.state.Status.IsTerminal() {
			r.settleLocked(t, StatusAborted, nil, queuedCause)
		}
	}
	r.queue = r.queue[:0]
	for _, t := range r.store.all() {
		if t.state.Status == StatusRetryWait {
			r.stopRetryWaitLocked(t)
			r.settleLocked(t, StatusAborted, nil, queuedCause)
		}
	}
	allDone := r.nRunning == 0
	r.cond.Broadcast()
	r.mu.Unlock()

	// 所有任务均在宽限期内收敛时，回收全部工作协程；
	// 若存在无视取消信号的失控任务，不无限等待，其协程稍后自行退出且结果被丢弃。
	if allDone {
		r.wg.Wait()
	}
}

// Stats 返回计数快照。
func (r *Runner) Stats() Stats {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.stats
	s.Queued = int64(len(r.queue))
	s.Running = int64(r.nRunning)
	return s
}

// inflightLocked 返回在途任务数（在跑 + 排队）。
func (r *Runner) inflightLocked() int { return r.nRunning + len(r.queue) }

// removeFromQueueLocked 从 FIFO 队列移除指定排队任务（调用方持锁）。
func (r *Runner) removeFromQueueLocked(target *Task) {
	q := r.queue
	for i, t := range q {
		if t == target {
			r.queue = append(q[:i], q[i+1:]...)
			return
		}
	}
}

// revivableReject 报告终态任务是否为“瞬时容量不足”导致的拒绝。
// 此类拒绝不代表业务结局，允许同内容再次提交时重新登记执行；
// 其余终态（含 shutting_down 拒绝）保持终态不变、仅重放结果。
func revivableReject(t *Task) bool {
	if t.state.Status != StatusRejected || t.state.Err == nil {
		return false
	}
	return t.state.Err.Code == CodeQueueFull || t.state.Err.Code == CodeQueueTimeout
}

// terminalCause 返回终态任务应重放给提交者的错误（成功为 nil）。
func terminalCause(t *Task) error {
	switch t.state.Status {
	case StatusSucceeded:
		return nil
	case StatusFailed, StatusCanceled, StatusRejected, StatusAborted:
		if t.state.Err != nil {
			return t.state.Err
		}
		return newError("submit", Code(t.state.Status), string(t.state.Status), nil)
	default:
		return nil
	}
}
