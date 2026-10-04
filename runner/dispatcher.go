package runner

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"time"
)

// CodeFailed 是业务失败码（非提交侧失败原因），用于 failed 终态。
const CodeFailed Code = "task_failed"

// capacity 返回在途总容量 = 并发数 + 排队长度。
func (r *Runner) capacity() int { return r.cfg.MaxWorkers + r.cfg.MaxQueue }

// admitLocked 尝试接纳新登记的任务（调用方持锁）。
//
// 在途总容量为 MaxWorkers + MaxQueue：
//   - 有空位：入 FIFO 队尾，广播唤醒空闲 worker；
//   - 容量满：按 MaxQueueWait 有界等待空位/关闭/超时；
//   - 等待超时或关闭：标记 rejected。被拒绝任务从未入队，不占执行资源。
func (r *Runner) admitLocked(t *Task) error {
	if r.shutdown {
		return r.rejectLocked(t, CodeShuttingDown, "new submissions are rejected during shutdown")
	}
	if r.inflightLocked() < r.capacity() {
		r.enqueueLocked(t)
		return nil
	}
	if r.cfg.MaxQueueWait <= 0 {
		return r.rejectLocked(t, CodeQueueFull,
			fmt.Sprintf("queue full (capacity=%d, running=%d)", r.capacity(), r.nRunning))
	}

	deadline := nowFunc().Add(r.cfg.MaxQueueWait)
	stopWatch := make(chan struct{})
	defer close(stopWatch)
	go func() {
		// cond.Broadcast 可在未持锁时调用；看门狗负责把时间流逝转为唤醒。
		d := time.Until(deadline)
		if d < 0 {
			d = 0
		}
		timer := time.NewTimer(d)
		defer timer.Stop()
		select {
		case <-timer.C:
			r.cond.Broadcast()
		case <-r.stopCh:
			return
		case <-stopWatch:
			return
		}
	}()

	for r.inflightLocked() >= r.capacity() {
		if r.shutdown {
			return r.rejectLocked(t, CodeShuttingDown, "new submissions are rejected during shutdown")
		}
		if !nowFunc().Before(deadline) {
			return r.rejectLocked(t, CodeQueueTimeout,
				fmt.Sprintf("timed out waiting for queue slot (capacity=%d, running=%d)",
					r.capacity(), r.nRunning))
		}
		r.cond.Wait()
	}
	r.enqueueLocked(t)
	return nil
}

// enqueueLocked 将任务放入 FIFO 队尾并唤醒空闲 worker。
func (r *Runner) enqueueLocked(t *Task) {
	r.queue = append(r.queue, t)
	r.cond.Broadcast()
}

// scheduleRetryLocked 把一次可重试失败安排为下一轮尝试（调用方持锁）。
// 等待期间任务处于 retry_wait：不占执行并发、不在队列，但仍是进行中的同一次提交。
func (r *Runner) scheduleRetryLocked(t *Task, cause error) {
	delay := t.policy.delay(t.state.Attempts)
	t.state.Status = StatusRetryWait
	t.state.Err = newError("execute", CodeFailed, "attempt failed, retry scheduled", cause)
	t.state.NextRetryAt = nowFunc().Add(delay)
	r.nRetryWait++
	t.retryTimer = time.AfterFunc(delay, func() { r.onRetryDue(t) })
}

// onRetryDue 由重试定时器触发：到点后把任务标记为待晋升并尝试按容量入队。
func (r *Runner) onRetryDue(t *Task) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if t.state.Status != StatusRetryWait || t.retryDue {
		return // 已被取消/关闭打断，或定时器重复触发
	}
	t.retryDue = true
	r.retryDue = append(r.retryDue, t)
	r.promoteDueLocked()
}

// promoteDueLocked 在容量允许时把到点的重试任务重新入队（调用方持锁）。
// 容量不足时任务留在 retry_wait，待容量释放后由 worker 循环再次晋升，
// 不会长期卡住：容量随在途任务结算而释放，关闭时由关闭流程统一处理。
func (r *Runner) promoteDueLocked() {
	for len(r.retryDue) > 0 && r.inflightLocked() < r.capacity() {
		t := r.retryDue[0]
		r.retryDue = r.retryDue[1:]
		if t.state.Status != StatusRetryWait {
			t.retryDue = false
			continue
		}
		t.retryDue = false
		t.state.Status = StatusQueued
		t.state.NextRetryAt = time.Time{}
		r.nRetryWait--
		r.enqueueLocked(t)
	}
}

// stopRetryWaitLocked 使任务退出 retry_wait 状态：停定时器并移出待晋升队列。
// 返回任务是否确实处于 retry_wait。
func (r *Runner) stopRetryWaitLocked(t *Task) bool {
	if t.state.Status != StatusRetryWait {
		return false
	}
	if t.retryTimer != nil {
		t.retryTimer.Stop()
		t.retryTimer = nil
	}
	if t.retryDue {
		t.retryDue = false
		for i, x := range r.retryDue {
			if x == t {
				r.retryDue = append(r.retryDue[:i], r.retryDue[i+1:]...)
				break
			}
		}
	}
	t.state.NextRetryAt = time.Time{}
	r.nRetryWait--
	return true
}

// rejectLocked 将任务结算为 rejected（仅登记不入队），不占用执行资源。
func (r *Runner) rejectLocked(t *Task, code Code, msg string) error {
	err := newError("submit", code, msg, nil)
	r.settleLocked(t, StatusRejected, nil, err)
	return err
}

// settleLocked 是唯一的终态结算入口（调用方持锁），保证每任务恰好结算一次。
// 已终结任务不会被重复结算，并保留首次结算的结果。
func (r *Runner) settleLocked(t *Task, st Status, result any, cause *TaskError) {
	if t.state.Status.IsTerminal() {
		return
	}
	t.state.Status = st
	t.state.FinishedAt = nowFunc()
	t.state.Result = result
	t.state.Err = cause
	switch st {
	case StatusSucceeded:
		r.stats.Succeeded++
	case StatusFailed:
		r.stats.Failed++
	case StatusCanceled:
		r.stats.Canceled++
	case StatusRejected:
		r.stats.Rejected++
	case StatusAborted:
		r.stats.Aborted++
	}
	close(t.done)
	r.cond.Broadcast()
}

// startWorkers 启动 MaxWorkers 个常驻工作协程。
func (r *Runner) startWorkers() {
	for i := 0; i < r.cfg.MaxWorkers; i++ {
		r.wg.Add(1)
		go r.worker(i)
	}
}

// worker 从 FIFO 队列取任务执行；关闭且队列清空后退出。
// 不变量：循环边界处 worker 始终持有 r.mu。
func (r *Runner) worker(id int) {
	defer r.wg.Done()
	r.mu.Lock()
	for {
		// 容量可能已于上次结算时释放：先晋升到点的重试任务。
		r.promoteDueLocked()
		for len(r.queue) == 0 && !r.shutdown {
			r.cond.Wait()
			r.promoteDueLocked()
		}
		if len(r.queue) == 0 {
			// shutdown 已开始且队列被清空：本 worker 退出。
			r.mu.Unlock()
			return
		}
		t := r.queue[0]
		r.queue = r.queue[1:]
		if t.state.Status.IsTerminal() {
			// 双保险：排队期间已结算的任务绝不执行。
			continue
		}
		t.state.Status = StatusRunning
		t.state.StartedAt = nowFunc()
		t.state.Attempts++
		t.state.NextRetryAt = time.Time{}
		ctx, cancel := context.WithCancel(context.Background())
		t.cancel = cancel
		r.running[t.idemKey] = t
		r.nRunning++
		r.cond.Broadcast()
		r.mu.Unlock()

		r.execute(ctx, t)
		cancel()

		r.mu.Lock()
		delete(r.running, t.idemKey)
		r.nRunning--
		r.cond.Broadcast()
	}
}

// execute 执行业务函数。取消/关闭先于完成发生时，业务结果不会成为可观测终态。
func (r *Runner) execute(ctx context.Context, t *Task) {
	defer func() {
		if rec := recover(); rec != nil {
			r.mu.Lock()
			if t.state.Status == StatusRunning {
				err := newError("execute", CodeFailed,
					fmt.Sprintf("task panicked: %v", rec),
					fmt.Errorf("%v\n%s", rec, debug.Stack()))
				r.settleLocked(t, StatusFailed, nil, err)
			}
			r.mu.Unlock()
		}
	}()

	result, err := t.fn(ctx)

	r.mu.Lock()
	defer r.mu.Unlock()
	if t.state.Status != StatusRunning {
		// 取消或关闭已先行结算：丢弃结果，保证取消后无完成结果可观测。
		return
	}
	if err != nil {
		// 业务函数因任务 ctx 被取消而退出：归类为 canceled，比 failed 更可解释。
		if ctx.Err() != nil && errors.Is(err, context.Canceled) {
			cause := newError("execute", CodeTaskCanceled,
				"task exited after cancel signal", context.Canceled)
			r.settleLocked(t, StatusCanceled, nil, cause)
			return
		}
		// 可重试失败且仍有剩余次数：安排下一轮尝试（关闭期间不再发起新尝试）。
		if !r.shutdown && t.policy.enabled() &&
			t.state.Attempts < t.policy.MaxAttempts && IsRetryable(err) {
			r.scheduleRetryLocked(t, err)
			return
		}
		terr := newError("execute", CodeFailed, "task returned error", err)
		r.settleLocked(t, StatusFailed, nil, terr)
		return
	}
	r.settleLocked(t, StatusSucceeded, result, nil)
}
