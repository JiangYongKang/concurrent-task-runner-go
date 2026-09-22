package taskrunner

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newTestRunner(t *testing.T, cfg Config) *Runner {
	t.Helper()
	r, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = r.Shutdown(ctx)
	})
	return r
}

func TestSubmitHappyPathAndReplay(t *testing.T) {
	r := newTestRunner(t, DefaultConfig())
	var execs atomic.Int64
	key := "key-1"
	req := SubmitRequest{IdempotencyKey: key, Payload: []byte("p1"), Func: onceFunc(&execs, []byte("r1"))}

	res, err := r.Submit(context.Background(), req)
	if err != nil {
		t.Fatalf("first submit: %v", err)
	}
	logCase(t, "SubmitReplay", req, "first submit replay=%v", res.Replay)
	if res.Replay {
		t.Fatal("first submit must not be a replay")
	}

	// 等待任务完成。
	info := waitForStatus(t, r, key, StatusSucceeded, time.Second)
	if string(info.Result) != "r1" {
		t.Fatalf("unexpected result %q", info.Result)
	}

	// 完全相同的请求重放：不得再次执行，直接返回同一结果。
	res2, err := r.Submit(context.Background(), req)
	if err != nil {
		t.Fatalf("replay submit: %v", err)
	}
	logCase(t, "SubmitReplay", req, "replay submit replay=%v id-same=%v",
		res2.Replay, res2.Task.ID == info.ID)
	if !res2.Replay || res2.Task.ID != info.ID || string(res2.Task.Result) != "r1" {
		t.Fatalf("replay must return the same task/result, got %+v", res2.Task)
	}
	if got := execs.Load(); got != 1 {
		t.Fatalf("func must execute exactly once, got %d", got)
	}
}

func TestSubmitIdempotencyConflict(t *testing.T) {
	r := newTestRunner(t, DefaultConfig())
	var execs atomic.Int64
	key := "key-conflict"
	r1 := SubmitRequest{IdempotencyKey: key, Payload: []byte("payload-A"), Func: onceFunc(&execs, []byte("A"))}
	r2 := SubmitRequest{IdempotencyKey: key, Payload: []byte("payload-B"), Func: onceFunc(&execs, []byte("B"))}

	if _, err := r.Submit(context.Background(), r1); err != nil {
		t.Fatalf("submit A: %v", err)
	}
	waitForStatus(t, r, key, StatusSucceeded, time.Second)

	_, err := r.Submit(context.Background(), r2)
	logCase(t, "IdempotencyConflict", []string{string(r1.Payload), string(r2.Payload)},
		"err=%v kind=%s", err, KindOf(err))
	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("expected ErrIdempotencyConflict, got %v", err)
	}
	// 冲突提交不得影响已有任务，也不得执行。
	info, _ := r.Get(context.Background(), key)
	if info.Status != StatusSucceeded || string(info.Result) != "A" {
		t.Fatalf("existing task mutated by conflict submit: %+v", info)
	}
	if execs.Load() != 1 {
		t.Fatalf("conflict submit must not execute func, execs=%d", execs.Load())
	}
}

func TestSubmitInvalidAndNotFound(t *testing.T) {
	r := newTestRunner(t, DefaultConfig())

	_, err := r.Submit(context.Background(), SubmitRequest{IdempotencyKey: "", Func: onceFunc(new(atomic.Int64), nil)})
	logCase(t, "InvalidArgument", "empty key", "err=%v", err)
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("expected invalid argument, got %v", err)
	}

	_, err = r.Submit(context.Background(), SubmitRequest{IdempotencyKey: "k", Func: nil})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("expected invalid argument for nil func, got %v", err)
	}

	_, err = r.Get(context.Background(), "missing")
	logCase(t, "NotFound", "key=missing", "err=%v kind=%s", err, KindOf(err))
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected not found, got %v", err)
	}
}

// TestConcurrentDuplicateSubmit 并发重复提交同一幂等键：
// 恰好一次真实执行，其余全部为 Replay，且最终结果一致。
func TestConcurrentDuplicateSubmit(t *testing.T) {
	r := newTestRunner(t, DefaultConfig())
	var execs atomic.Int64
	const n = 64
	key := "dedup-key"
	req := SubmitRequest{IdempotencyKey: key, Payload: []byte("same"), Func: func() TaskFunc {
		// 让任务稍慢，制造并发窗口。
		return func(ctx context.Context, payload []byte) ([]byte, error) {
			execs.Add(1)
			time.Sleep(20 * time.Millisecond)
			return []byte("same-result"), nil
		}
	}()}

	var replayed atomic.Int64
	var rejected atomic.Int64
	var wg sync.WaitGroup
	errKinds := make(chan ErrorKind, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := r.Submit(context.Background(), req)
			switch {
			case err != nil:
				errKinds <- KindOf(err)
				rejected.Add(1)
			case res.Replay:
				replayed.Add(1)
			}
		}()
	}
	wg.Wait()
	close(errKinds)
	for k := range errKinds {
		if k != KindIdempotencyConflict {
			t.Fatalf("only conflict/replay allowed under same payload, got %s", k)
		}
	}

	info := waitForStatus(t, r, key, StatusSucceeded, 2*time.Second)
	logCase(t, "ConcurrentDuplicateSubmit",
		map[string]int{"goroutines": n, "execs": int(execs.Load())},
		"replayed=%d rejected=%d finalStatus=%s result=%s",
		replayed.Load(), rejected.Load(), info.Status, info.Result)

	if execs.Load() != 1 {
		t.Fatalf("expected exactly 1 execution, got %d", execs.Load())
	}
	if replayed.Load() != int64(n-1) {
		t.Fatalf("expected %d replays, got %d", n-1, replayed.Load())
	}
	if rejected.Load() != 0 {
		t.Fatalf("same-payload concurrent submits must replay, not reject: %d", rejected.Load())
	}
	if string(info.Result) != "same-result" {
		t.Fatalf("result mismatch: %q", info.Result)
	}
}
