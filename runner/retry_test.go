package runner

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// flakyFunc 返回一个前 failTimes 次以可重试错误失败、随后成功的任务函数，
// 并记录每次尝试的时间点。
func flakyFunc(failTimes int, result any, attemptTimes *[]time.Time) Func {
	var calls int32
	return func(ctx context.Context) (any, error) {
		n := atomic.AddInt32(&calls, 1)
		if attemptTimes != nil {
			*attemptTimes = append(*attemptTimes, time.Now())
		}
		if int(n) <= failTimes {
			return nil, Retryable(fmt.Errorf("transient-%d", n))
		}
		return result, nil
	}
}

func TestRetryPolicyNormalize(t *testing.T) {
	cases := []struct {
		name     string
		p        RetryPolicy
		ok       bool
		attempts int
	}{
		{"zero-value-no-retry", RetryPolicy{}, true, 1},
		{"valid-fixed", RetryPolicy{MaxAttempts: 3, Interval: time.Millisecond}, true, 3},
		{"negative-attempts", RetryPolicy{MaxAttempts: -1}, false, 0},
		{"exceeds-hard-limit", RetryPolicy{MaxAttempts: MaxRetryAttempts + 1}, false, 0},
		{"negative-interval", RetryPolicy{MaxAttempts: 2, Interval: -time.Millisecond}, false, 0},
		{"unknown-backoff", RetryPolicy{MaxAttempts: 2, Backoff: "random"}, false, 0},
		{"valid-exponential", RetryPolicy{MaxAttempts: 4, Interval: time.Millisecond, Backoff: BackoffExponential}, true, 4},
	}
	for _, c := range cases {
		np, err := c.p.normalize()
		got := err == nil
		judge(t, "policy:"+c.name, c.p, fmt.Sprintf("ok=%v attempts=%d", got, np.MaxAttempts), c.ok)
		if got != c.ok {
			t.Fatalf("%s: 期望 ok=%v, 实际 err=%v", c.name, c.ok, err)
		}
		if c.ok && np.MaxAttempts != c.attempts {
			t.Fatalf("%s: 归一化后 MaxAttempts=%d, 期望 %d", c.name, np.MaxAttempts, c.attempts)
		}
		if !c.ok && errCode(err) != CodeInvalidArgument {
			t.Fatalf("%s: 期望 invalid_argument, 实际 %v", c.name, err)
		}
	}
}

func TestRetryBackoffDelay(t *testing.T) {
	fixed := RetryPolicy{MaxAttempts: 4, Interval: 10 * time.Millisecond, Backoff: BackoffFixed}
	exp := RetryPolicy{MaxAttempts: 5, Interval: 10 * time.Millisecond,
		Backoff: BackoffExponential, MaxInterval: 25 * time.Millisecond}
	for i := 1; i <= 3; i++ {
		if d := fixed.delay(i); d != 10*time.Millisecond {
			t.Fatalf("固定退避第 %d 次应为 10ms, 实际 %v", i, d)
		}
	}
	want := []time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 25 * time.Millisecond, 25 * time.Millisecond}
	for i, w := range want {
		got := exp.delay(i + 1)
		judge(t, "exponential-delay", fmt.Sprintf("attempt=%d", i+1), got, w)
		if got != w {
			t.Fatalf("指数退避第 %d 次期望 %v, 实际 %v", i+1, w, got)
		}
	}
}

func TestRetryableMarker(t *testing.T) {
	base := errors.New("boom")
	if IsRetryable(base) {
		t.Fatal("普通错误不应判定为可重试")
	}
	marked := Retryable(base)
	if !IsRetryable(marked) {
		t.Fatal("Retryable 标记后应判定为可重试")
	}
	if !IsRetryable(fmt.Errorf("wrap: %w", marked)) {
		t.Fatal("包装链中应仍可判定为可重试")
	}
	if !errors.Is(marked, base) {
		t.Fatal("标记后应保留 errors.Is 语义")
	}
	if Retryable(nil) != nil {
		t.Fatal("Retryable(nil) 应返回 nil")
	}
	judge(t, "retryable-marker", "普通/标记/包装三类错误", "仅标记类可重试", "符合预期")
}

func TestRetrySucceedsAfterTransientFailures(t *testing.T) {
	r := newTestRunner(t, Config{MaxWorkers: 2, MaxQueue: 4})
	ctx := context.Background()

	var times []time.Time
	begin := time.Now()
	_, err := r.SubmitWithRetry(ctx, "flaky", Payload{Name: "f"},
		flakyFunc(2, "ok-after-2-retries", &times),
		RetryPolicy{MaxAttempts: 3, Interval: 40 * time.Millisecond, Backoff: BackoffFixed})
	if err != nil {
		t.Fatalf("提交失败: %v", err)
	}
	fin := waitTerminal(t, r, "flaky")
	elapsed := time.Since(begin)

	judge(t, "retry-success", "前 2 次可重试失败, 固定间隔 40ms, 最多 3 次",
		fmt.Sprintf("status=%s attempts=%d result=%v elapsed=%v",
			fin.Status, fin.Attempts, fin.Result, elapsed.Round(10*time.Millisecond)),
		"succeeded attempts=3 elapsed>=80ms")
	if fin.Status != StatusSucceeded || fin.Result != "ok-after-2-retries" {
		t.Fatalf("应最终成功, 实际 %+v", fin)
	}
	if fin.Attempts != 3 {
		t.Fatalf("应尝试 3 次, 实际 %d", fin.Attempts)
	}
	if len(times) != 3 {
		t.Fatalf("任务函数应被调用 3 次, 实际 %d", len(times))
	}
	// 固定间隔：第 2、3 次尝试相对前一次都应间隔约 40ms。
	for i := 1; i < len(times); i++ {
		gap := times[i].Sub(times[i-1])
		if gap < 30*time.Millisecond || gap > 500*time.Millisecond {
			t.Fatalf("第 %d 次重试间隔异常: %v", i+1, gap)
		}
	}
}

func TestRetryWaitStateObservable(t *testing.T) {
	r := newTestRunner(t, Config{MaxWorkers: 1, MaxQueue: 2})
	ctx := context.Background()

	if _, err := r.SubmitWithRetry(ctx, "waiting", Payload{Name: "w"},
		flakyFunc(1, "done", nil),
		RetryPolicy{MaxAttempts: 2, Interval: 150 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	// 第一次尝试很快失败，随后应处于 retry_wait：进行中、可见下次尝试时间与上次失败原因。
	deadline := time.Now().Add(time.Second)
	var observed TaskState
	for time.Now().Before(deadline) {
		s, _ := r.Get(ctx, "waiting")
		if s.Status == StatusRetryWait {
			observed = *s
			break
		}
		time.Sleep(time.Millisecond)
	}
	judge(t, "retry-wait-observable", "首次失败后轮询状态",
		fmt.Sprintf("status=%s attempts=%d nextRetrySet=%v errCode=%s",
			observed.Status, observed.Attempts, !observed.NextRetryAt.IsZero(), errCode(observed.Err)),
		"retry_wait attempts=1 nextRetrySet=true errCode=task_failed")
	if observed.Status != StatusRetryWait {
		t.Fatalf("应观测到 retry_wait, 实际 %s", observed.Status)
	}
	if observed.Attempts != 1 || observed.NextRetryAt.IsZero() {
		t.Fatalf("retry_wait 期间应可见 attempts=1 与下次尝试时间, 实际 %+v", observed)
	}
	if observed.Status.IsTerminal() {
		t.Fatal("retry_wait 必须是进行中状态")
	}
	fin := waitTerminal(t, r, "waiting")
	if fin.Status != StatusSucceeded || fin.Attempts != 2 {
		t.Fatalf("等待结束后应第 2 次尝试成功, 实际 %+v", fin)
	}
}

func TestNonRetryableErrorFailsImmediately(t *testing.T) {
	r := newTestRunner(t, Config{MaxWorkers: 1, MaxQueue: 2})
	ctx := context.Background()

	var runs int32
	fn := func(ctx context.Context) (any, error) {
		atomic.AddInt32(&runs, 1)
		return nil, errors.New("bad-params") // 未标记 Retryable：参数/数据类错误
	}
	if _, err := r.SubmitWithRetry(ctx, "no-retry", Payload{Name: "n"}, fn,
		RetryPolicy{MaxAttempts: 5, Interval: time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	fin := waitTerminal(t, r, "no-retry")
	judge(t, "non-retryable", "不可重试错误 + MaxAttempts=5",
		fmt.Sprintf("status=%s attempts=%d runs=%d", fin.Status, fin.Attempts, atomic.LoadInt32(&runs)),
		"failed attempts=1")
	if fin.Status != StatusFailed || fin.Attempts != 1 || atomic.LoadInt32(&runs) != 1 {
		t.Fatalf("不可重试错误不得重试, 实际 %+v runs=%d", fin, runs)
	}
	if errCode(fin.Err) != CodeFailed {
		t.Fatalf("失败原因应为 task_failed, 实际 %v", fin.Err)
	}
}

func TestRetryExhaustedEndsFailed(t *testing.T) {
	r := newTestRunner(t, Config{MaxWorkers: 1, MaxQueue: 2})
	ctx := context.Background()

	cause := errors.New("always-transient")
	var runs int32
	fn := func(ctx context.Context) (any, error) {
		atomic.AddInt32(&runs, 1)
		return nil, Retryable(cause)
	}
	if _, err := r.SubmitWithRetry(ctx, "exhaust", Payload{Name: "e"}, fn,
		RetryPolicy{MaxAttempts: 3, Interval: 10 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	fin := waitTerminal(t, r, "exhaust")
	judge(t, "retry-exhausted", "始终可重试失败, MaxAttempts=3",
		fmt.Sprintf("status=%s attempts=%d errCode=%s", fin.Status, fin.Attempts, errCode(fin.Err)),
		"failed attempts=3 task_failed")
	if fin.Status != StatusFailed || fin.Attempts != 3 {
		t.Fatalf("次数用尽后应 failed 且 attempts=3, 实际 %+v", fin)
	}
	if !errors.Is(fin.Err, cause) {
		t.Fatalf("终态错误应包含最后一次失败原因, 实际 %v", fin.Err)
	}
	// 终态后重复提交：重放失败，不再执行。
	_, err := r.SubmitWithRetry(ctx, "exhaust", Payload{Name: "e"}, fn,
		RetryPolicy{MaxAttempts: 3, Interval: time.Millisecond})
	if errCode(err) != CodeFailed || atomic.LoadInt32(&runs) != 3 {
		t.Fatalf("终态重放不应再执行, err=%v runs=%d", err, runs)
	}
}
