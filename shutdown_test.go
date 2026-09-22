package taskrunner

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestShutdownGracefulAllSettle: 关闭时在途/排队任务在宽限期内自然收敛，
// 不允许出现 UNFINISHED，结果不丢。
func TestShutdownGracefulAllSettle(t *testing.T) {
	cfg := Config{Workers: 2, QueueSize: 4, QueueWaitTimeout: 0, ShutdownGrace: 3 * time.Second}
	r, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var execs atomic.Int64
	for i := 0; i < 5; i++ {
		key := "g-" + string(rune('a'+i))
		req := SubmitRequest{IdempotencyKey: key, Payload: []byte(key),
			Func: func(ctx context.Context, p []byte) ([]byte, error) {
				time.Sleep(10 * time.Millisecond)
				execs.Add(1)
				return p, nil
			}}
		if _, err := r.Submit(context.Background(), req); err != nil {
			t.Fatalf("submit %s: %v", key, err)
		}
	}
	if err := r.Shutdown(context.Background()); err != nil {
		t.Fatalf("graceful shutdown error: %v", err)
	}
	for i := 0; i < 5; i++ {
		key := "g-" + string(rune('a'+i))
		info, gerr := r.Get(context.Background(), key)
		if gerr != nil {
			t.Fatal(gerr)
		}
		if info.Status != StatusSucceeded {
			t.Fatalf("task %s expected SUCCEEDED, got %s", key, info.Status)
		}
	}
	logCase(t, "ShutdownGraceful", map[string]int{"workers": 2, "queue": 4, "tasks": 5},
		"verdict all SUCCEEDED execs=%d unfinished=%d stats=%+v",
		execs.Load(), r.Stats().UnfinishedTotal, r.Stats())

	// 关闭后新提交必须被明确拒绝。
	_, err = r.Submit(context.Background(), SubmitRequest{IdempotencyKey: "late", Func: onceFunc(new(atomic.Int64), nil)})
	logCase(t, "SubmitAfterShutdown", "key=late", "verdict err=%v kind=%s", err, KindOf(err))
	if !errors.Is(err, ErrRunnerClosed) {
		t.Fatalf("expected ErrRunnerClosed, got %v", err)
	}
	// 重复 Shutdown 返回相同结果（nil），且不 panic。
	if err := r.Shutdown(context.Background()); err != nil {
		t.Fatalf("second shutdown: %v", err)
	}
}

// TestShutdownGraceExpiry: 宽限期结束仍在运行的任务必须被明确标记 UNFINISHED，
// 之后迟到的结果被丢弃，状态不再翻转。
func TestShutdownGraceExpiry(t *testing.T) {
	cfg := Config{Workers: 1, QueueSize: 1, QueueWaitTimeout: 0, ShutdownGrace: 80 * time.Millisecond}
	r, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	// running: 尊重 ctx，但取消后仍需 ~120ms 才返回（晚于宽限期）。
	runKey := "slow"
	started := make(chan struct{})
	runningReq := SubmitRequest{IdempotencyKey: runKey, Func: func(ctx context.Context, p []byte) ([]byte, error) {
		close(started)
		select {
		case <-ctx.Done():
			time.Sleep(120 * time.Millisecond) // 模拟清理慢于宽限期
			return nil, ctx.Err()
		case <-time.After(5 * time.Second):
			return []byte("too-late"), nil
		}
	}}
	// queued: 排队中，关闭时可能尚未执行。
	var queuedStarts atomic.Int64
	queuedReq := SubmitRequest{IdempotencyKey: "q", Func: func(ctx context.Context, p []byte) ([]byte, error) {
		queuedStarts.Add(1)
		return p, nil
	}}
	if _, err := r.Submit(context.Background(), runningReq); err != nil {
		t.Fatal(err)
	}
	<-started
	if _, err := r.Submit(context.Background(), queuedReq); err != nil {
		t.Fatal(err)
	}

	shutdownStart := time.Now()
	serr := r.Shutdown(context.Background())
	elapsed := time.Since(shutdownStart)
	logCase(t, "ShutdownGraceExpiry",
		map[string]any{"grace": cfg.ShutdownGrace, "running": runKey, "queued": "q"},
		"verdict shutdownErr=%v kind=%s elapsed=%s", serr, KindOf(serr), elapsed)
	if !errors.Is(serr, ErrRunnerClosed) {
		t.Fatalf("expected runner_closed error after grace, got %v", serr)
	}
	if elapsed > 300*time.Millisecond {
		t.Fatalf("Shutdown must return near grace expiry, took %s", elapsed)
	}

	// 给迟到结果一点时间返回，证明它无法覆盖 UNFINISHED。
	time.Sleep(250 * time.Millisecond)
	for _, key := range []string{runKey, "q"} {
		info, _ := r.Get(context.Background(), key)
		if info.Status != StatusUnfinished {
			t.Fatalf("task %s expected UNFINISHED, got %s result=%q", key, info.Status, info.Result)
		}
		if info.Result != nil {
			t.Fatalf("late result must be discarded for %s", key)
		}
		logCase(t, "ShutdownGraceExpiry", "key="+key,
			"verdict final=%s (no late result observed)", info.Status)
	}
	if r.Stats().UnfinishedTotal < 1 {
		t.Fatalf("expected at least 1 unfinished, stats=%+v", r.Stats())
	}
}

// TestShutdownRepeatedReturnsSameResult: 多次并发 Shutdown 不 panic、结果一致。
func TestShutdownRepeatedReturnsSameResult(t *testing.T) {
	cfg := Config{Workers: 1, QueueSize: 0, QueueWaitTimeout: 0, ShutdownGrace: 30 * time.Millisecond}
	r, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	bf := newBlockFunc()
	if _, err := r.Submit(context.Background(), SubmitRequest{IdempotencyKey: "hostage", Func: bf.fn()}); err != nil {
		t.Fatal(err)
	}
	if !waitForTrue(time.Second, func() bool { return bf.started.Load() == 1 }) {
		t.Fatal("never started")
	}

	var wg sync.WaitGroup
	errs := make([]error, 4)
	for i := range errs {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errs[idx] = r.Shutdown(context.Background())
		}(i)
	}
	err = r.Shutdown(context.Background()) // 主线程也参与
	wg.Wait()
	close(bf.gate)
	for _, e := range errs {
		if (e == nil) != (err == nil) || (e != nil && KindOf(e) != KindOf(err)) {
			t.Fatalf("shutdown results inconsistent: %v vs %v", e, err)
		}
	}
	info, _ := r.Get(context.Background(), "hostage")
	logCase(t, "ShutdownRepeated", "5 concurrent Shutdown calls",
		"verdict same-kind results, task final=%s unfinished=%d",
		info.Status, r.Stats().UnfinishedTotal)
	if info.Status != StatusUnfinished {
		t.Fatalf("hostage task should be UNFINISHED, got %s", info.Status)
	}
}
