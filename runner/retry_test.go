package runner

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestRetryPolicyValidate 校验重试策略的取值约束。
func TestRetryPolicyValidate(t *testing.T) {
	r := newTestRunner(t, Config{MaxWorkers: 1, MaxQueue: 1})
	ctx := context.Background()

	bad := []struct {
		name   string
		policy *RetryPolicy
	}{
		{"negative attempts", &RetryPolicy{MaxAttempts: -1}},
		{"too many attempts", &RetryPolicy{MaxAttempts: MaxRetryAttempts + 1, Interval: time.Millisecond}},
		{"missing interval", &RetryPolicy{MaxAttempts: 2}},
		{"negative interval", &RetryPolicy{MaxAttempts: 2, Interval: -time.Millisecond}},
		{"interval too large", &RetryPolicy{MaxAttempts: 2, Interval: MaxRetryInterval + time.Second}},
		{"bad backoff", &RetryPolicy{MaxAttempts: 2, Interval: time.Millisecond, Backoff: BackoffKind(9)}},
		{"maxinterval below interval", &RetryPolicy{MaxAttempts: 2, Interval: time.Second, MaxInterval: time.Millisecond}},
	}
	for _, c := range bad {
		_, err := r.Submit(ctx, "bad-"+c.name, Payload{Name: "n", Retry: c.policy}, quickFunc(1))
		judge(t, "retry-policy:"+c.name, fmt.Sprintf("policy=%+v", *c.policy), errCode(err), CodeInvalidArgument)
		if errCode(err) != CodeInvalidArgument {
			t.Fatalf("%s: 期望 invalid_argument, 实际 %v", c.name, err)
		}
		if _, ok := r.Get(ctx, "bad-"+c.name); ok {
			t.Fatalf("%s: 非法策略不得登记任务", c.name)
		}
	}

	// 零值策略等价于不重试；nil 策略保持原行为。
	for i, p := range []*RetryPolicy{nil, {}} {
		key := fmt.Sprintf("ok-%d", i)
		if _, err := r.Submit(ctx, key, Payload{Name: "n", Retry: p}, quickFunc(1)); err != nil {
			t.Fatalf("零值/nil 策略应合法: %v", err)
		}
		fin := waitTerminal(t, r, key)
		judge(t, "retry-policy-default", fmt.Sprintf("policy=%+v", p),
			fmt.Sprintf("status=%s attempts=%d", fin.Status, fin.Attempts), "succeeded attempts=1")
		if fin.Status != StatusSucceeded || fin.Attempts != 1 {
			t.Fatalf("默认策略应只尝试一次, 实际 %+v", fin)
		}
	}
}

// TestRetryFixedInterval 固定间隔重试：前两次失败、第三次成功。
func TestRetryFixedInterval(t *testing.T) {
	r := newTestRunner(t, Config{MaxWorkers: 2, MaxQueue: 4})
	ctx := context.Background()

	var mu sync.Mutex
	var starts []time.Time
	var calls int32
	fn := func(ctx context.Context) (any, error) {
		c := atomic.AddInt32(&calls, 1)
		mu.Lock()
		starts = append(starts, time.Now())
		mu.Unlock()
		if c < 3 {
			return nil, fmt.Errorf("transient-%d", c)
		}
		return "ok-after-retry", nil
	}
	policy := &RetryPolicy{MaxAttempts: 3, Interval: 40 * time.Millisecond, Backoff: BackoffFixed}
	if _, err := r.Submit(ctx, "fx", Payload{Name: "job", Retry: policy}, fn); err != nil {
		t.Fatal(err)
	}
	fin := waitTerminal(t, r, "fx")

	mu.Lock()
	defer mu.Unlock()
	judge(t, "retry-fixed", "MaxAttempts=3 Interval=40ms fixed, 前2次失败",
		fmt.Sprintf("status=%s attempts=%d calls=%d gaps=%v", fin.Status, fin.Attempts, atomic.LoadInt32(&calls), gaps(starts)),
		"succeeded attempts=3 calls=3 间隔约40ms")
	if fin.Status != StatusSucceeded || fin.Result != "ok-after-retry" {
		t.Fatalf("应重试后成功, 实际 %+v", fin)
	}
	if fin.Attempts != 3 || atomic.LoadInt32(&calls) != 3 {
		t.Fatalf("应尝试 3 次, attempts=%d calls=%d", fin.Attempts, calls)
	}
	if len(starts) != 3 {
		t.Fatalf("应记录 3 次开始时间, 实际 %d", len(starts))
	}
	for i := 1; i < 3; i++ {
		gap := starts[i].Sub(starts[i-1])
		if gap < 30*time.Millisecond {
			t.Fatalf("固定间隔应 >= ~40ms, 第 %d 次间隔仅 %v", i, gap)
		}
	}
}

// TestRetryExponentialBackoff 指数退避：间隔逐步拉长且受 MaxInterval 封顶。
func TestRetryExponentialBackoff(t *testing.T) {
	r := newTestRunner(t, Config{MaxWorkers: 2, MaxQueue: 4})
	ctx := context.Background()

	var mu sync.Mutex
	var starts []time.Time
	fn := func(ctx context.Context) (any, error) {
		mu.Lock()
		starts = append(starts, time.Now())
		mu.Unlock()
		return nil, errors.New("always-transient")
	}
	policy := &RetryPolicy{MaxAttempts: 4, Interval: 20 * time.Millisecond, Backoff: BackoffExponential}
	if _, err := r.Submit(ctx, "exp", Payload{Name: "job", Retry: policy}, fn); err != nil {
		t.Fatal(err)
	}
	fin := waitTerminal(t, r, "exp")

	mu.Lock()
	defer mu.Unlock()
	judge(t, "retry-exponential", "MaxAttempts=4 Interval=20ms exponential, 全部失败",
		fmt.Sprintf("status=%s attempts=%d gaps=%v", fin.Status, fin.Attempts, gaps(starts)),
		"failed attempts=4 间隔约 20/40/80ms")
	if fin.Status != StatusFailed || fin.Attempts != 4 {
		t.Fatalf("重试耗尽应为 failed 且 attempts=4, 实际 %+v", fin)
	}
	if len(starts) != 4 {
		t.Fatalf("应记录 4 次开始时间, 实际 %d", len(starts))
	}
	g1 := starts[1].Sub(starts[0])
	g2 := starts[2].Sub(starts[1])
	g3 := starts[3].Sub(starts[2])
	if g1 < 15*time.Millisecond {
		t.Fatalf("首次重试间隔应约 20ms, 实际 %v", g1)
	}
	if g2 < g1*3/2 {
		t.Fatalf("指数退避应逐步拉长: g1=%v g2=%v", g1, g2)
	}
	if g3 < g2*3/2 {
		t.Fatalf("指数退避应逐步拉长: g2=%v g3=%v", g2, g3)
	}
}

// TestRetryDelayCap 验证指数退避被 MaxInterval 封顶。
func TestRetryDelayCap(t *testing.T) {
	p := &RetryPolicy{MaxAttempts: 5, Interval: 100 * time.Millisecond,
		Backoff: BackoffExponential, MaxInterval: 150 * time.Millisecond}
	got := []time.Duration{p.delay(1), p.delay(2), p.delay(3), p.delay(4)}
	want := []time.Duration{100 * time.Millisecond, 150 * time.Millisecond, 150 * time.Millisecond, 150 * time.Millisecond}
	judge(t, "retry-delay-cap", "Interval=100ms MaxInterval=150ms exponential", got, want)
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("delay(%d) 期望 %v, 实际 %v", i+1, want[i], got[i])
		}
	}
}

// TestRetryExhaustedFails 重试耗尽：failed 终态可看出尝试次数与最后一次失败原因。
func TestRetryExhaustedFails(t *testing.T) {
	r := newTestRunner(t, Config{MaxWorkers: 1, MaxQueue: 2})
	ctx := context.Background()

	var calls int32
	fn := func(ctx context.Context) (any, error) {
		c := atomic.AddInt32(&calls, 1)
		return nil, fmt.Errorf("boom-%d", c)
	}
	policy := &RetryPolicy{MaxAttempts: 3, Interval: 10 * time.Millisecond}
	if _, err := r.Submit(ctx, "exh", Payload{Name: "job", Retry: policy}, fn); err != nil {
		t.Fatal(err)
	}
	fin := waitTerminal(t, r, "exh")
	judge(t, "retry-exhausted", "MaxAttempts=3 全部失败",
		fmt.Sprintf("status=%s attempts=%d errCode=%s err=%v", fin.Status, fin.Attempts, errCode(fin.Err), fin.Err),
		"failed attempts=3 task_failed 含 boom-3")
	if fin.Status != StatusFailed {
		t.Fatalf("重试耗尽应为 failed, 实际 %s", fin.Status)
	}
	if fin.Attempts != 3 || atomic.LoadInt32(&calls) != 3 {
		t.Fatalf("应共尝试 3 次, attempts=%d calls=%d", fin.Attempts, calls)
	}
	if fin.Err == nil || errCode(fin.Err) != CodeFailed {
		t.Fatalf("终态错误应为 task_failed, 实际 %v", fin.Err)
	}
	if got := fin.Err.Error(); !contains(got, "boom-3") {
		t.Fatalf("终态错误应保留最后一次失败原因 boom-3, 实际 %v", got)
	}
	// 终态重放：同键同内容再提交返回同一失败结局。
	if _, err := r.Submit(ctx, "exh", Payload{Name: "job", Retry: policy}, fn); errCode(err) != CodeFailed {
		t.Fatalf("终态重放应返回 failed 原因, 实际 %v", err)
	}
	if atomic.LoadInt32(&calls) != 3 {
		t.Fatalf("终态重放不得触发新执行, calls=%d", calls)
	}
}

// TestRetryNonRetryableError 永久性错误不重试。
func TestRetryNonRetryableError(t *testing.T) {
	r := newTestRunner(t, Config{MaxWorkers: 1, MaxQueue: 2})
	ctx := context.Background()

	var calls int32
	fn := func(ctx context.Context) (any, error) {
		atomic.AddInt32(&calls, 1)
		return nil, NonRetryable(errors.New("bad-params"))
	}
	policy := &RetryPolicy{MaxAttempts: 5, Interval: 10 * time.Millisecond}
	if _, err := r.Submit(ctx, "perm", Payload{Name: "job", Retry: policy}, fn); err != nil {
		t.Fatal(err)
	}
	fin := waitTerminal(t, r, "perm")
	judge(t, "retry-non-retryable", "MaxAttempts=5 但返回 NonRetryable",
		fmt.Sprintf("status=%s attempts=%d calls=%d", fin.Status, fin.Attempts, atomic.LoadInt32(&calls)),
		"failed attempts=1 calls=1")
	if fin.Status != StatusFailed || fin.Attempts != 1 || atomic.LoadInt32(&calls) != 1 {
		t.Fatalf("永久错误不应重试, 实际 %+v calls=%d", fin, calls)
	}
	if !errors.Is(fin.Err, fin.Err.Unwrap()) && !contains(fin.Err.Error(), "bad-params") {
		t.Fatalf("终态错误应保留原始原因, 实际 %v", fin.Err)
	}
}

// TestRetryWaitDuplicateSubmit 重试等待期间：同键同内容复用同一任务，不同内容冲突。
func TestRetryWaitDuplicateSubmit(t *testing.T) {
	r := newTestRunner(t, Config{MaxWorkers: 2, MaxQueue: 4})
	ctx := context.Background()

	var calls int32
	fn := func(ctx context.Context) (any, error) {
		c := atomic.AddInt32(&calls, 1)
		if c == 1 {
			return nil, errors.New("transient")
		}
		return "second-try-ok", nil
	}
	payload := Payload{Name: "job", Params: 7,
		Retry: &RetryPolicy{MaxAttempts: 2, Interval: 200 * time.Millisecond}}
	if _, err := r.Submit(ctx, "dup", payload, fn); err != nil {
		t.Fatal(err)
	}
	ensureStatus(t, r, "dup", StatusRetryWait)

	// 等待期间同键同内容再提交：阻塞到终态并复用同一任务的结果。
	type res struct {
		state *TaskState
		err   error
	}
	done := make(chan res, 1)
	go func() {
		s, err := r.Submit(ctx, "dup", payload, fn)
		done <- res{s, err}
	}()

	// 同键不同内容：立即冲突拒绝。
	_, err := r.Submit(ctx, "dup", Payload{Name: "job", Params: 8,
		Retry: &RetryPolicy{MaxAttempts: 2, Interval: 200 * time.Millisecond}}, fn)
	judge(t, "retry-wait-conflict", "retry_wait 期间同键不同内容", errCode(err), CodeIdempotentConflict)
	if errCode(err) != CodeIdempotentConflict {
		t.Fatalf("内容不一致应冲突, 实际 %v", err)
	}

	out := <-done
	judge(t, "retry-wait-duplicate", "retry_wait 期间同键同内容再提交",
		fmt.Sprintf("status=%s result=%v calls=%d", out.state.Status, out.state.Result, atomic.LoadInt32(&calls)),
		"succeeded/second-try-ok calls=2")
	if out.err != nil || out.state.Status != StatusSucceeded || out.state.Result != "second-try-ok" {
		t.Fatalf("重复提交应复用同一任务的最终成功, 实际 %+v err=%v", out.state, out.err)
	}
	if atomic.LoadInt32(&calls) != 2 {
		t.Fatalf("不得另起并行执行, 总执行次数应为 2, 实际 %d", calls)
	}
}

// TestCancelDuringRetryWait 取消打断重试等待：及时停下且不再发起新尝试。
func TestCancelDuringRetryWait(t *testing.T) {
	r := newTestRunner(t, Config{MaxWorkers: 1, MaxQueue: 2})
	ctx := context.Background()

	var calls int32
	fn := func(ctx context.Context) (any, error) {
		atomic.AddInt32(&calls, 1)
		return nil, errors.New("transient")
	}
	payload := Payload{Name: "job", Retry: &RetryPolicy{MaxAttempts: 5, Interval: 80 * time.Millisecond}}
	if _, err := r.Submit(ctx, "cw", payload, fn); err != nil {
		t.Fatal(err)
	}
	ensureStatus(t, r, "cw", StatusRetryWait)

	s, err := r.Cancel(ctx, "cw", "user-abort")
	judge(t, "cancel-retry-wait", "retry_wait 期间取消",
		fmt.Sprintf("code=%v status=%s", errCode(err), s.Status), "nil/canceled")
	if err != nil || s.Status != StatusCanceled {
		t.Fatalf("重试等待中应可取消, status=%s err=%v", s.Status, err)
	}

	time.Sleep(200 * time.Millisecond) // 远超重试间隔，观察是否仍有新尝试
	st, _ := r.Get(ctx, "cw")
	judge(t, "cancel-retry-no-new-attempt", "取消后等待 200ms",
		fmt.Sprintf("status=%s calls=%d", st.Status, atomic.LoadInt32(&calls)), "canceled calls=1")
	if st.Status != StatusCanceled || atomic.LoadInt32(&calls) != 1 {
		t.Fatalf("取消后不得再发起尝试, status=%s calls=%d", st.Status, calls)
	}
}

// TestCancelRunningRetryTaskLateResult 运行中取消带重试任务：晚到的可重试错误不触发新尝试。
func TestCancelRunningRetryTaskLateResult(t *testing.T) {
	r := newTestRunner(t, Config{MaxWorkers: 1, MaxQueue: 2})
	ctx := context.Background()

	started := make(chan struct{})
	var calls int32
	fn := func(ctx context.Context) (any, error) {
		atomic.AddInt32(&calls, 1)
		close(started)
		<-ctx.Done()
		// 协作退出后返回一个“值得重试”的错误：也不得触发重试。
		return nil, errors.New("transient-after-cancel")
	}
	payload := Payload{Name: "job", Retry: &RetryPolicy{MaxAttempts: 3, Interval: 20 * time.Millisecond}}
	if _, err := r.Submit(ctx, "cr", payload, fn); err != nil {
		t.Fatal(err)
	}
	<-started
	if _, err := r.Cancel(ctx, "cr", "stop"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond) // 覆盖重试间隔窗口
	st, _ := r.Get(ctx, "cr")
	judge(t, "cancel-running-late-result", "运行中取消, fn 随后返回可重试错误",
		fmt.Sprintf("status=%s calls=%d", st.Status, atomic.LoadInt32(&calls)), "canceled calls=1")
	if st.Status != StatusCanceled || atomic.LoadInt32(&calls) != 1 {
		t.Fatalf("晚到的可重试错误不得覆盖取消结局, status=%s calls=%d", st.Status, calls)
	}
}

// TestShutdownDrainsRetryWait 关闭宽限期内：等待中的重试到期后仍执行并保留结果。
func TestShutdownDrainsRetryWait(t *testing.T) {
	r := newManualRunner(t, Config{MaxWorkers: 1, MaxQueue: 2, ShutdownGrace: 2 * time.Second})
	ctx := context.Background()

	var calls int32
	fn := func(ctx context.Context) (any, error) {
		c := atomic.AddInt32(&calls, 1)
		if c == 1 {
			return nil, errors.New("transient")
		}
		return "retried-during-grace", nil
	}
	payload := Payload{Name: "job", Retry: &RetryPolicy{MaxAttempts: 2, Interval: 50 * time.Millisecond}}
	if _, err := r.Submit(ctx, "sd", payload, fn); err != nil {
		t.Fatal(err)
	}
	ensureStatus(t, r, "sd", StatusRetryWait)

	if err := r.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	s, _ := r.Get(ctx, "sd")
	judge(t, "shutdown-drains-retry", "retry_wait 期间关闭, grace=2s interval=50ms",
		fmt.Sprintf("status=%s result=%v attempts=%d", s.Status, s.Result, s.Attempts),
		"succeeded/retried-during-grace attempts=2")
	if s.Status != StatusSucceeded || s.Result != "retried-during-grace" || s.Attempts != 2 {
		t.Fatalf("宽限期内重试应执行完并保留成功结果, 实际 %+v", s)
	}
}

// TestShutdownAbortsPendingRetry 宽限期结束仍未到重试时间：标记 aborted 且不再发起尝试。
func TestShutdownAbortsPendingRetry(t *testing.T) {
	r := newManualRunner(t, Config{MaxWorkers: 1, MaxQueue: 2, ShutdownGrace: 100 * time.Millisecond})
	ctx := context.Background()

	var calls int32
	fn := func(ctx context.Context) (any, error) {
		atomic.AddInt32(&calls, 1)
		return nil, errors.New("transient")
	}
	// 重试间隔 5s，远超宽限期 100ms。
	payload := Payload{Name: "job", Retry: &RetryPolicy{MaxAttempts: 3, Interval: 5 * time.Second}}
	if _, err := r.Submit(ctx, "sa", payload, fn); err != nil {
		t.Fatal(err)
	}
	ensureStatus(t, r, "sa", StatusRetryWait)

	begin := time.Now()
	if err := r.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(begin)
	s, _ := r.Get(ctx, "sa")
	judge(t, "shutdown-aborts-pending-retry", "retry_wait(interval=5s) + grace=100ms",
		fmt.Sprintf("status=%s errCode=%s elapsed=%v calls=%d", s.Status, errCode(s.Err), elapsed.Round(10*time.Millisecond), atomic.LoadInt32(&calls)),
		"aborted/aborted ~100ms calls=1")
	if s.Status != StatusAborted || errCode(s.Err) != CodeAborted {
		t.Fatalf("宽限期后未收敛的重试等待应为 aborted, 实际 %+v", s)
	}
	if atomic.LoadInt32(&calls) != 1 {
		t.Fatalf("关闭后不得再发起新尝试, calls=%d", calls)
	}
}

// TestRetryReadmitCapacityFull 重试再准入时容量不足且不可等待：确定结局 failed(queue_timeout)。
func TestRetryReadmitCapacityFull(t *testing.T) {
	r := newTestRunner(t, Config{MaxWorkers: 1, MaxQueue: 0, MaxQueueWait: 0})
	ctx := context.Background()

	// B 先执行并失败进入 retry_wait（等待期间不占容量）。
	var bCalls int32
	bFn := func(ctx context.Context) (any, error) {
		atomic.AddInt32(&bCalls, 1)
		return nil, errors.New("transient")
	}
	bPayload := Payload{Name: "b", Retry: &RetryPolicy{MaxAttempts: 3, Interval: 120 * time.Millisecond}}
	if _, err := r.Submit(ctx, "B", bPayload, bFn); err != nil {
		t.Fatal(err)
	}
	ensureStatus(t, r, "B", StatusRetryWait)

	// A 随即占住唯一容量；B 的重试到期时容量满且 wait=0。
	aStarted := make(chan struct{})
	aRelease := make(chan struct{})
	aFn := func(ctx context.Context) (any, error) {
		close(aStarted)
		<-aRelease
		return "a", nil
	}
	if _, err := r.Submit(ctx, "A", Payload{Name: "a"}, aFn); err != nil {
		t.Fatal(err)
	}
	<-aStarted

	fin := waitTerminal(t, r, "B")
	judge(t, "retry-readmit-capacity-full", "B 重试到期时容量被 A 占满且 wait=0",
		fmt.Sprintf("status=%s errCode=%s attempts=%d", fin.Status, errCode(fin.Err), fin.Attempts),
		"failed/queue_timeout attempts=1")
	if fin.Status != StatusFailed || errCode(fin.Err) != CodeQueueTimeout {
		t.Fatalf("容量不足的重试再准入应失败为 queue_timeout, 实际 %+v", fin)
	}
	if fin.Attempts != 1 || atomic.LoadInt32(&bCalls) != 1 {
		t.Fatalf("未再准入不得增加尝试次数, attempts=%d calls=%d", fin.Attempts, bCalls)
	}
	close(aRelease)
}

// TestResubmitAfterTransientReject 瞬时容量拒绝后：同键同内容可重新提交并成功，键不被毒化。
func TestResubmitAfterTransientReject(t *testing.T) {
	r := newTestRunner(t, Config{MaxWorkers: 1, MaxQueue: 0, MaxQueueWait: 0})
	ctx := context.Background()

	started := make(chan struct{})
	release := make(chan struct{})
	occupy := func(ctx context.Context) (any, error) {
		close(started)
		<-release
		return "occupy", nil
	}
	if _, err := r.Submit(ctx, "occupy", Payload{Name: "o"}, occupy); err != nil {
		t.Fatal(err)
	}
	<-started

	payload := Payload{Name: "work", Params: 1}
	var runs int32
	fn := func(ctx context.Context) (any, error) {
		atomic.AddInt32(&runs, 1)
		return "done", nil
	}

	// 容量满：首次提交被 queue_full 拒绝。
	_, err := r.Submit(ctx, "retry-key", payload, fn)
	judge(t, "transient-reject", "容量满时首次提交", errCode(err), CodeQueueFull)
	if errCode(err) != CodeQueueFull {
		t.Fatalf("期望 queue_full, 实际 %v", err)
	}

	// 容量恢复后：同键同内容可正常提交并执行，而不是永远重放 rejected。
	close(release)
	waitTerminal(t, r, "occupy")
	s, err := r.Submit(ctx, "retry-key", payload, fn)
	judge(t, "resubmit-after-reject", "容量恢复后同键同内容再提交",
		fmt.Sprintf("code=%v status=%s", errCode(err), s.Status), "nil/queued|running")
	if err != nil {
		t.Fatalf("容量恢复后应能重新提交, 实际 %v", err)
	}
	fin := waitTerminal(t, r, "retry-key")
	if fin.Status != StatusSucceeded || fin.Result != "done" || atomic.LoadInt32(&runs) != 1 {
		t.Fatalf("重新提交应成功执行一次, 实际 %+v runs=%d", fin, runs)
	}
	judge(t, "resubmit-executed", "retry-key 终态",
		fmt.Sprintf("status=%s result=%v runs=%d", fin.Status, fin.Result, atomic.LoadInt32(&runs)),
		"succeeded/done runs=1")

	// 同键不同内容仍然冲突。
	_, err = r.Submit(ctx, "retry-key", Payload{Name: "work", Params: 2}, fn)
	judge(t, "resubmit-conflict", "同键不同内容", errCode(err), CodeIdempotentConflict)
	if errCode(err) != CodeIdempotentConflict {
		t.Fatalf("内容不一致仍应冲突, 实际 %v", err)
	}
}

// gaps 计算相邻时间点的间隔，用于日志展示。
func gaps(ts []time.Time) []time.Duration {
	out := make([]time.Duration, 0, len(ts)-1)
	for i := 1; i < len(ts); i++ {
		out = append(out, ts[i].Sub(ts[i-1]).Round(time.Millisecond))
	}
	return out
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (func() bool {
		for i := 0; i+len(sub) <= len(s); i++ {
			if s[i:i+len(sub)] == sub {
				return true
			}
		}
		return false
	})()
}
