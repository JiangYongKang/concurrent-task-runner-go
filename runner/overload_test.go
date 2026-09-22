package runner

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// blockingTask 返回一个会阻塞直到 release 关闭的任务及 release 函数。
func blockingTask() (Func, func()) {
	started := make(chan struct{})
	release := make(chan struct{})
	fn := func(ctx context.Context) (any, error) {
		close(started)
		select {
		case <-release:
			return "done", nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	rel := func() { close(release) }
	// started 通道随 fn 闭包使用；为避免竞态，额外暴露 started。
	fn = withStarted(fn, started)
	return fn, rel
}

func withStarted(fn Func, started chan struct{}) Func {
	return fn
}

func TestQueueFullRejectImmediately(t *testing.T) {
	// 1 个 worker，队列容量 0，不等待：第一个占 worker，第二个必须立即被拒绝。
	r := newTestRunner(t, Config{MaxWorkers: 1, MaxQueue: 0, MaxQueueWait: 0})
	ctx := context.Background()

	started1 := make(chan struct{})
	release1 := make(chan struct{})
	var runs int32
	fn1 := func(ctx context.Context) (any, error) {
		atomic.AddInt32(&runs, 1)
		close(started1)
		<-release1
		return "x", nil
	}
	if _, err := r.Submit(ctx, "occupy", Payload{Name: "a"}, fn1); err != nil {
		t.Fatal(err)
	}
	<-started1

	_, err := r.Submit(ctx, "overflow", Payload{Name: "b"}, quickFunc(1))
	judge(t, "queue-full", "worker 被占满且 queue=0 wait=0", errCode(err), CodeQueueFull)
	if errCode(err) != CodeQueueFull {
		t.Fatalf("期望 queue_full，实际 %v", err)
	}
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("应能 errors.Is(ErrQueueFull)，实际 %v", err)
	}

	// 被拒绝任务不得占用资源、不得执行。
	if s, ok := r.Get(ctx, "overflow"); !ok || s.Status != StatusRejected {
		t.Fatalf("被拒绝任务应为 rejected 终态，实际 %+v ok=%v", s, ok)
	}
	judge(t, "rejected-terminal", "overflow 状态", "rejected", mustStatus(r, "overflow"))

	// 已有任务不受影响：释放后成功完成。
	close(release1)
	fin := waitTerminal(t, r, "occupy")
	if fin.Status != StatusSucceeded || fin.Result != "x" {
		t.Fatalf("在途任务应不受拒绝影响并成功, 实际 %+v", fin)
	}
	judge(t, "existing-unaffected", "释放 occupy 后状态",
		fmt.Sprintf("succeeded/x runs=%d", atomic.LoadInt32(&runs)),
		fmt.Sprintf("status=%s result=%v", fin.Status, fin.Result))
}

func TestQueueWaitTimeout(t *testing.T) {
	// queue=0，但允许等待 50ms；worker 一直忙 -> 等待超时。
	r := newTestRunner(t, Config{MaxWorkers: 1, MaxQueue: 0, MaxQueueWait: 50 * time.Millisecond})
	ctx := context.Background()

	started := make(chan struct{})
	release := make(chan struct{})
	fn1 := func(ctx context.Context) (any, error) {
		close(started)
		<-release
		return nil, nil
	}
	if _, err := r.Submit(ctx, "busy", Payload{Name: "a"}, fn1); err != nil {
		t.Fatal(err)
	}
	<-started

	begin := time.Now()
	_, err := r.Submit(ctx, "waiter", Payload{Name: "b"}, quickFunc(2))
	elapsed := time.Since(begin)
	judge(t, "queue-timeout", "等待 50ms 后仍无空位",
		fmt.Sprintf("code=%s elapsed=%v", errCode(err), elapsed.Round(10*time.Millisecond)),
		"queue_timeout ~50ms")
	if errCode(err) != CodeQueueTimeout {
		t.Fatalf("期望 queue_timeout，实际 %v", err)
	}
	if elapsed < 40*time.Millisecond || elapsed > 500*time.Millisecond {
		t.Fatalf("应在 ~50ms 后被拒绝，实际 %v", elapsed)
	}
	close(release)
}

func TestQueueWaitSucceedsWhenSlotFrees(t *testing.T) {
	// queue=0，等待 2s；第一个任务快速完成，等待者应被接纳执行，而不是超时。
	r := newTestRunner(t, Config{MaxWorkers: 1, MaxQueue: 0, MaxQueueWait: 2 * time.Second})
	ctx := context.Background()

	firstStarted := make(chan struct{})
	fn1 := func(ctx context.Context) (any, error) {
		close(firstStarted)
		time.Sleep(30 * time.Millisecond)
		return "first", nil
	}
	if _, err := r.Submit(ctx, "first", Payload{Name: "a"}, fn1); err != nil {
		t.Fatal(err)
	}
	<-firstStarted

	s, err := r.Submit(ctx, "second", Payload{Name: "b"}, quickFunc("second"))
	if err != nil {
		t.Fatalf("空位释放后等待者应被接纳，实际 %v", err)
	}
	fin := waitTerminal(t, r, s.IdemKey)
	judge(t, "wait-admitted", "first 完成后 second 入队执行", fin.Status, StatusSucceeded)
	if fin.Status != StatusSucceeded || fin.Result != "second" {
		t.Fatalf("second 应成功，实际 %+v", fin)
	}
}

func TestBoundedConcurrency(t *testing.T) {
	const workers = 2
	const total = 20
	r := newTestRunner(t, Config{MaxWorkers: workers, MaxQueue: total, MaxQueueWait: 0})
	ctx := context.Background()

	var concurrent int32
	var maxConcurrent int32
	task := func(ctx context.Context) (any, error) {
		cur := atomic.AddInt32(&concurrent, 1)
		for {
			m := atomic.LoadInt32(&maxConcurrent)
			if cur <= m || atomic.CompareAndSwapInt32(&maxConcurrent, m, cur) {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
		atomic.AddInt32(&concurrent, -1)
		return 1, nil
	}
	for i := 0; i < total; i++ {
		if _, err := r.Submit(ctx, fmt.Sprintf("t-%d", i), Payload{Name: "load", Params: i}, task); err != nil {
			t.Fatalf("任务 %d 提交失败: %v", i, err)
		}
	}
	for i := 0; i < total; i++ {
		waitTerminal(t, r, fmt.Sprintf("t-%d", i))
	}
	judge(t, "bounded-concurrency", fmt.Sprintf("workers=%d total=%d", workers, total),
		maxConcurrent, int32(workers))
	if maxConcurrent > workers {
		t.Fatalf("并发数不得超过 %d，实测峰值 %d", workers, maxConcurrent)
	}
	st := r.Stats()
	if st.Succeeded != total {
		t.Fatalf("应有 %d 成功，实际 %d", total, st.Succeeded)
	}
}

func mustStatus(r *Runner, key string) Status {
	s, ok := r.Get(context.Background(), key)
	if !ok {
		return "<missing>"
	}
	return s.Status
}
