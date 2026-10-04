package runner

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// waitStatus 轮询直到任务进入指定状态或超时。
func waitStatus(t *testing.T, r *Runner, key string, st Status) TaskState {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if s, ok := r.Get(context.Background(), key); ok && s.Status == st {
			return *s
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("任务 %s 未在超时内进入 %s", key, st)
	return TaskState{}
}

func TestDuplicateSubmitDuringRetryWait(t *testing.T) {
	r := newTestRunner(t, Config{MaxWorkers: 2, MaxQueue: 4})
	ctx := context.Background()

	if _, err := r.SubmitWithRetry(ctx, "dup", Payload{Name: "d", Params: 1},
		flakyFunc(1, "final-ok", nil),
		RetryPolicy{MaxAttempts: 2, Interval: 120 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	// 等任务进入重试等待：此时同键同内容再次提交必须复用同一任务。
	waitStatus(t, r, "dup", StatusRetryWait)

	type outcome struct {
		state *TaskState
		err   error
	}
	done := make(chan outcome, 1)
	go func() {
		s, err := r.SubmitWithRetry(ctx, "dup", Payload{Name: "d", Params: 1},
			quickFunc("should-not-run"),
			RetryPolicy{MaxAttempts: 2, Interval: 120 * time.Millisecond})
		done <- outcome{s, err}
	}()

	select {
	case o := <-done:
		t.Fatalf("重试等待期间的重复提交不应立即返回, 得到 %+v err=%v", o.state, o.err)
	case <-time.After(30 * time.Millisecond):
		// 仍在阻塞等待同一任务的最终结局：符合预期。
	}

	o := <-done
	judge(t, "dup-submit-during-retry-wait", "retry_wait 期间同键同内容提交",
		fmt.Sprintf("status=%s attempts=%d result=%v err=%v", o.state.Status, o.state.Attempts, o.state.Result, o.err),
		"succeeded attempts=2 result=final-ok err=nil")
	if o.err != nil || o.state.Status != StatusSucceeded || o.state.Result != "final-ok" {
		t.Fatalf("重复提交应复用同一任务并重放最终成功, 实际 %+v err=%v", o.state, o.err)
	}
	if o.state.Attempts != 2 {
		t.Fatalf("同一任务只应被重试推进, attempts=%d", o.state.Attempts)
	}

	// 同键不同内容：仍然按冲突拒绝。
	_, err := r.SubmitWithRetry(ctx, "dup", Payload{Name: "d", Params: 2}, quickFunc(1),
		RetryPolicy{MaxAttempts: 2})
	judge(t, "conflict-during-retry", "同键不同内容", errCode(err), CodeIdempotentConflict)
	if errCode(err) != CodeIdempotentConflict {
		t.Fatalf("内容不一致应冲突拒绝, 实际 %v", err)
	}
}

func TestCancelDuringRetryWait(t *testing.T) {
	r := newTestRunner(t, Config{MaxWorkers: 1, MaxQueue: 2})
	ctx := context.Background()

	var runs int32
	fn := func(ctx context.Context) (any, error) {
		atomic.AddInt32(&runs, 1)
		return nil, Retryable(errors.New("transient"))
	}
	if _, err := r.SubmitWithRetry(ctx, "cancel-wait", Payload{Name: "c"}, fn,
		RetryPolicy{MaxAttempts: 5, Interval: 100 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, r, "cancel-wait", StatusRetryWait)

	s, err := r.Cancel(ctx, "cancel-wait", "user-abort")
	judge(t, "cancel-during-retry-wait", "retry_wait 中取消",
		fmt.Sprintf("status=%s err=%v", s.Status, err), "canceled")
	if err != nil || s.Status != StatusCanceled {
		t.Fatalf("取消应立即生效, 实际 %+v err=%v", s, err)
	}
	// 等待超过原计划的重试间隔：不得再发起新的尝试。
	time.Sleep(250 * time.Millisecond)
	if n := atomic.LoadInt32(&runs); n != 1 {
		t.Fatalf("取消后不得再执行, runs=%d", n)
	}
	fin, _ := r.Get(ctx, "cancel-wait")
	judge(t, "no-attempt-after-cancel", "取消后等待 250ms",
		fmt.Sprintf("status=%s attempts=%d runs=%d", fin.Status, fin.Attempts, atomic.LoadInt32(&runs)),
		"canceled attempts=1 runs=1")
	if fin.Status != StatusCanceled || fin.Attempts != 1 {
		t.Fatalf("终态应保持 canceled, 实际 %+v", fin)
	}
}

func TestShutdownDrainsRetryWaitTask(t *testing.T) {
	// 重试间隔 5s 远大于宽限期 500ms：关闭应立即打断等待并在宽限期内执行完。
	r := newManualRunner(t, Config{MaxWorkers: 1, MaxQueue: 2, ShutdownGrace: 500 * time.Millisecond})
	ctx := context.Background()

	var runs int32
	fn := func(ctx context.Context) (any, error) {
		n := atomic.AddInt32(&runs, 1)
		if n == 1 {
			return nil, Retryable(errors.New("transient"))
		}
		return "finished-during-grace", nil
	}
	if _, err := r.SubmitWithRetry(ctx, "drain", Payload{Name: "d"}, fn,
		RetryPolicy{MaxAttempts: 3, Interval: 5 * time.Second}); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, r, "drain", StatusRetryWait)

	begin := time.Now()
	if err := r.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(begin)

	s, _ := r.Get(ctx, "drain")
	judge(t, "shutdown-drains-retry", "retry_wait(5s) 中关闭, grace=500ms",
		fmt.Sprintf("status=%s attempts=%d result=%v elapsed=%v",
			s.Status, s.Attempts, s.Result, elapsed.Round(10*time.Millisecond)),
		"succeeded attempts=2 elapsed<<5s")
	if s.Status != StatusSucceeded || s.Result != "finished-during-grace" {
		t.Fatalf("宽限期内应尽量执行完并保留成功结果, 实际 %+v", s)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("关闭不应等待整个重试间隔, 实际 %v", elapsed)
	}
}

func TestShutdownAbortsUnconvergedRetryTask(t *testing.T) {
	// 重试任务在宽限期内被调度执行但不收敛：宽限期结束标记 aborted。
	r := newManualRunner(t, Config{MaxWorkers: 1, MaxQueue: 2, ShutdownGrace: 80 * time.Millisecond})
	ctx := context.Background()

	release := make(chan struct{})
	var runs int32
	fn := func(ctx context.Context) (any, error) {
		n := atomic.AddInt32(&runs, 1)
		if n == 1 {
			return nil, Retryable(errors.New("transient"))
		}
		<-release // 第二次尝试不收敛
		return "too-late", nil
	}
	if _, err := r.SubmitWithRetry(ctx, "stuck-retry", Payload{Name: "s"}, fn,
		RetryPolicy{MaxAttempts: 3, Interval: 5 * time.Second}); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, r, "stuck-retry", StatusRetryWait)

	if err := r.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	s, _ := r.Get(ctx, "stuck-retry")
	judge(t, "shutdown-abort-retry", "重试任务宽限期内未收敛",
		fmt.Sprintf("status=%s errCode=%s", s.Status, errCode(s.Err)), "aborted/aborted")
	if s.Status != StatusAborted || errCode(s.Err) != CodeAborted {
		t.Fatalf("未收敛任务应标记 aborted, 实际 %+v", s)
	}
	// 晚到结果不得覆盖已确定的结局。
	close(release)
	time.Sleep(30 * time.Millisecond)
	s2, _ := r.Get(ctx, "stuck-retry")
	if s2.Status != StatusAborted || s2.Result != nil {
		t.Fatalf("晚到结果不得覆盖 aborted, 实际 %+v", s2)
	}
}

func TestRetryReentryRespectsCapacity(t *testing.T) {
	// 1 worker、queue=0：重试到点时容量被占，任务应留在 retry_wait，
	// 容量释放后再入队执行，不能长期卡住。
	r := newTestRunner(t, Config{MaxWorkers: 1, MaxQueue: 0, MaxQueueWait: 0})
	ctx := context.Background()

	occupyStarted := make(chan struct{})
	occupyRelease := make(chan struct{})
	occupy := func(ctx context.Context) (any, error) {
		close(occupyStarted)
		<-occupyRelease
		return "occupy-done", nil
	}
	// 先让重试任务失败一次进入 retry_wait（间隔 20ms），再占满 worker。
	var runs int32
	retryFn := func(ctx context.Context) (any, error) {
		n := atomic.AddInt32(&runs, 1)
		if n == 1 {
			return nil, Retryable(errors.New("transient"))
		}
		return "retry-done", nil
	}
	if _, err := r.SubmitWithRetry(ctx, "retry-cap", Payload{Name: "r"}, retryFn,
		RetryPolicy{MaxAttempts: 2, Interval: 20 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, r, "retry-cap", StatusRetryWait)
	if _, err := r.Submit(ctx, "occupy", Payload{Name: "o"}, occupy); err != nil {
		t.Fatal(err)
	}
	<-occupyStarted

	// 重试到点但容量被占：任务不得执行，也不得终结。
	time.Sleep(80 * time.Millisecond)
	mid, _ := r.Get(ctx, "retry-cap")
	judge(t, "retry-waits-for-capacity", "容量被占时重试到点",
		fmt.Sprintf("status=%s runs=%d", mid.Status, atomic.LoadInt32(&runs)),
		"retry_wait runs=1")
	if mid.Status != StatusRetryWait || atomic.LoadInt32(&runs) != 1 {
		t.Fatalf("容量不足时重试应继续等待而非执行/终结, 实际 %+v runs=%d", mid, runs)
	}

	close(occupyRelease)
	fin := waitTerminal(t, r, "retry-cap")
	judge(t, "retry-promoted-after-capacity", "释放容量后",
		fmt.Sprintf("status=%s attempts=%d result=%v", fin.Status, fin.Attempts, fin.Result),
		"succeeded attempts=2")
	if fin.Status != StatusSucceeded || fin.Attempts != 2 || fin.Result != "retry-done" {
		t.Fatalf("容量释放后重试应执行成功, 实际 %+v", fin)
	}
}

func TestRejectedThenResubmitSucceeds(t *testing.T) {
	// 瞬时容量不足被拒后，容量恢复时同键同内容可重新提交并执行。
	r := newTestRunner(t, Config{MaxWorkers: 1, MaxQueue: 0, MaxQueueWait: 0})
	ctx := context.Background()

	release := make(chan struct{})
	started := make(chan struct{})
	occupy := func(ctx context.Context) (any, error) {
		close(started)
		<-release
		return "x", nil
	}
	if _, err := r.Submit(ctx, "occupy", Payload{Name: "o"}, occupy); err != nil {
		t.Fatal(err)
	}
	<-started

	var runs int32
	fn := func(ctx context.Context) (any, error) {
		atomic.AddInt32(&runs, 1)
		return "real-result", nil
	}
	_, err := r.Submit(ctx, "transient-key", Payload{Name: "t", Params: 1}, fn)
	judge(t, "transient-reject", "容量满时首次提交", errCode(err), CodeQueueFull)
	if errCode(err) != CodeQueueFull {
		t.Fatalf("应被瞬时拒绝, 实际 %v", err)
	}

	// 容量恢复：同键同内容重新提交，应正常执行而不是重放拒绝。
	close(release)
	waitTerminal(t, r, "occupy")
	s, err := r.Submit(ctx, "transient-key", Payload{Name: "t", Params: 1}, fn)
	judge(t, "resubmit-after-reject", "容量恢复后同键同内容再提交",
		fmt.Sprintf("status=%s err=%v", s.Status, err), "受理成功")
	if err != nil {
		t.Fatalf("瞬时拒绝后同键同内容应可重新提交, 实际 %v", err)
	}
	fin := waitTerminal(t, r, "transient-key")
	if fin.Status != StatusSucceeded || fin.Result != "real-result" || atomic.LoadInt32(&runs) != 1 {
		t.Fatalf("重新提交应执行成功, 实际 %+v runs=%d", fin, runs)
	}
	judge(t, "resubmit-executed", "重新提交的结局",
		fmt.Sprintf("status=%s result=%v runs=%d", fin.Status, fin.Result, atomic.LoadInt32(&runs)),
		"succeeded/real-result runs=1")

	// 同键不同内容：即使曾被拒绝也仍按冲突拒绝。
	_, err = r.Submit(ctx, "transient-key", Payload{Name: "t", Params: 2}, fn)
	if errCode(err) != CodeIdempotentConflict {
		t.Fatalf("内容不一致应冲突拒绝, 实际 %v", err)
	}

	// 统计自洽：登记 3 次（occupy + 首次被拒 + 重新提交），终态合计应相等。
	st := r.Stats()
	tracked := st.Succeeded + st.Failed + st.Canceled + st.Rejected + st.Aborted
	judge(t, "stats-consistent", "终态合计 vs 登记数",
		fmt.Sprintf("tracked=%d submitted=%d", tracked, st.Submitted), "相等")
	if tracked != st.Submitted {
		t.Fatalf("终态合计 %d != 登记数 %d", tracked, st.Submitted)
	}
}

func TestShutdownDrainsCapacityBlockedRetry(t *testing.T) {
	// 重试到点但容量被占（留在 retry_wait），随后关闭：
	// 关闭流程应将其视为到点任务入队，在宽限期内执行完。
	r := newManualRunner(t, Config{MaxWorkers: 1, MaxQueue: 0, MaxQueueWait: 0, ShutdownGrace: time.Second})
	ctx := context.Background()

	var runs int32
	retryFn := func(ctx context.Context) (any, error) {
		n := atomic.AddInt32(&runs, 1)
		if n == 1 {
			return nil, Retryable(errors.New("transient"))
		}
		return "drained", nil
	}
	if _, err := r.SubmitWithRetry(ctx, "blocked-retry", Payload{Name: "b"}, retryFn,
		RetryPolicy{MaxAttempts: 2, Interval: 20 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, r, "blocked-retry", StatusRetryWait)

	// 占满唯一 worker，使重试到点后无法晋升。
	occupyStarted := make(chan struct{})
	occupyRelease := make(chan struct{})
	occupy := func(ctx context.Context) (any, error) {
		close(occupyStarted)
		select {
		case <-occupyRelease:
			return "o", nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if _, err := r.Submit(ctx, "occupy", Payload{Name: "o"}, occupy); err != nil {
		t.Fatal(err)
	}
	<-occupyStarted
	time.Sleep(60 * time.Millisecond) // 确保重试早已到点且仍被容量阻塞
	if mid, _ := r.Get(ctx, "blocked-retry"); mid.Status != StatusRetryWait {
		t.Fatalf("关闭前重试应被容量阻塞在 retry_wait, 实际 %s", mid.Status)
	}

	// 关闭：occupy 协作退出后，被阻塞的重试任务应在宽限期内执行完。
	go func() {
		time.Sleep(30 * time.Millisecond)
		close(occupyRelease)
	}()
	if err := r.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	s, _ := r.Get(ctx, "blocked-retry")
	judge(t, "shutdown-drains-blocked-retry", "容量阻塞的重试任务 + 关闭",
		fmt.Sprintf("status=%s attempts=%d result=%v", s.Status, s.Attempts, s.Result),
		"succeeded attempts=2")
	if s.Status != StatusSucceeded || s.Attempts != 2 || s.Result != "drained" {
		t.Fatalf("宽限期内应尽量执行完被阻塞的重试任务, 实际 %+v", s)
	}
}
