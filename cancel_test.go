package taskrunner

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestCancelQueuedNeverRuns: 排队中的任务被取消后绝不执行。
func TestCancelQueuedNeverRuns(t *testing.T) {
	cfg := Config{Workers: 1, QueueSize: 2, QueueWaitTimeout: 0, ShutdownGrace: 2 * time.Second}
	r := newTestRunner(t, cfg)

	bf := newBlockFunc()
	if _, err := r.Submit(context.Background(), SubmitRequest{IdempotencyKey: "run1", Func: bf.fn()}); err != nil {
		t.Fatal(err)
	}
	if !waitForTrue(time.Second, func() bool { return bf.started.Load() == 1 }) {
		t.Fatal("run1 never started")
	}

	bf2 := newBlockFunc()
	if _, err := r.Submit(context.Background(), SubmitRequest{IdempotencyKey: "q1", Func: bf2.fn()}); err != nil {
		t.Fatal(err)
	}
	if err := r.Cancel(context.Background(), "q1"); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	info := waitForStatus(t, r, "q1", StatusCancelled, time.Second)
	logCase(t, "CancelQueued", "key=q1 while PENDING",
		"verdict status=%s result=%v executions=%d", info.Status, info.Result, bf2.started.Load())
	if info.Result != nil {
		t.Fatal("cancelled task must have no result")
	}

	close(bf.gate) // 排空，q1 即使被 worker 取到也必须跳过
	time.Sleep(50 * time.Millisecond)
	if bf2.started.Load() != 0 {
		t.Fatalf("cancelled queued task must never execute, got starts=%d", bf2.started.Load())
	}
	final, _ := r.Get(context.Background(), "q1")
	if final.Status != StatusCancelled {
		t.Fatalf("status changed after drain: %s", final.Status)
	}
}

// TestCancelRunningDiscardsLateResult: 取消执行中的任务后，
// 即使任务函数无视 ctx 返回了结果，该结果也必须被丢弃，
// 对外永远观察不到“完成结果”。
func TestCancelRunningDiscardsLateResult(t *testing.T) {
	cfg := Config{Workers: 1, QueueSize: 0, QueueWaitTimeout: 0, ShutdownGrace: 2 * time.Second}
	r := newTestRunner(t, cfg)

	lf := newLateResultFunc() // gate 打开后无视 ctx 直接返回 "late"
	if _, err := r.Submit(context.Background(), SubmitRequest{IdempotencyKey: "c1", Func: lf.fn()}); err != nil {
		t.Fatal(err)
	}
	if !waitForTrue(time.Second, func() bool { return lf.started.Load() == 1 }) {
		t.Fatal("task never started")
	}
	if err := r.Cancel(context.Background(), "c1"); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	// 取消后再放行，函数返回迟到的成功结果。
	close(lf.gate)
	<-lf.returned
	// 再给 worker 结算一点时间，然后断言结果被丢弃且状态不翻转。
	time.Sleep(20 * time.Millisecond)

	last, _ := r.Get(context.Background(), "c1")
	logCase(t, "CancelRunning", "cancel while RUNNING, func returns late success",
		"verdict final=%s result=%v", last.Status, last.Result)
	if last.Status != StatusCancelled || last.Result != nil {
		t.Fatalf("expected CANCELLED with no result, got %+v", last)
	}
}

// TestCancelRunningCooperative: 尊重 ctx 的任务以 ctx.Err() 结束，仍为 CANCELLED。
func TestCancelRunningCooperative(t *testing.T) {
	r := newTestRunner(t, DefaultConfig())
	started := make(chan struct{})
	req := SubmitRequest{IdempotencyKey: "coop", Func: func(ctx context.Context, p []byte) ([]byte, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	if _, err := r.Submit(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	<-started
	if err := r.Cancel(context.Background(), "coop"); err != nil {
		t.Fatal(err)
	}
	info := waitForStatus(t, r, "coop", StatusCancelled, 2*time.Second)
	logCase(t, "CancelCooperative", "task waits on ctx", "verdict status=%s errMsg=%q",
		info.Status, info.ErrMessage)
	if info.Result != nil {
		t.Fatal("no result after cancellation")
	}
}

func TestCancelTerminalAndMissing(t *testing.T) {
	r := newTestRunner(t, DefaultConfig())
	var execs int
	req := SubmitRequest{IdempotencyKey: "done", Func: func(context.Context, []byte) ([]byte, error) {
		execs++
		return []byte("final"), nil
	}}
	if _, err := r.Submit(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	info := waitForStatus(t, r, "done", StatusSucceeded, time.Second)

	// 已终态：取消幂等 no-op，结果不变。
	if err := r.Cancel(context.Background(), "done"); err != nil {
		t.Fatalf("cancel terminal must be no-op without error: %v", err)
	}
	again, _ := r.Get(context.Background(), "done")
	logCase(t, "CancelTerminal", "cancel after SUCCEEDED", "verdict unchanged=%s result=%q",
		again.Status, again.Result)
	if again.Status != info.Status || string(again.Result) != "final" {
		t.Fatalf("terminal task altered by cancel: %+v", again)
	}

	err := r.Cancel(context.Background(), "ghost")
	logCase(t, "CancelMissing", "key=ghost", "verdict err=%v kind=%s", err, KindOf(err))
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}
