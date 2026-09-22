package runner

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func TestCancelQueuedNeverRuns(t *testing.T) {
	// 1 worker 长期被占，queue 足够大，后两个任务保持 queued，取消后绝不执行。
	r := newTestRunner(t, Config{MaxWorkers: 1, MaxQueue: 4, MaxQueueWait: 0})
	ctx := context.Background()

	started := make(chan struct{})
	release := make(chan struct{})
	var ran int32
	head := func(ctx context.Context) (any, error) {
		close(started)
		<-release
		return "head", nil
	}
	queued := func(ctx context.Context) (any, error) {
		atomic.AddInt32(&ran, 1)
		return "ran", nil
	}
	if _, err := r.Submit(ctx, "head", Payload{Name: "h"}, head); err != nil {
		t.Fatal(err)
	}
	<-started
	for _, k := range []string{"q1", "q2"} {
		if _, err := r.Submit(ctx, k, Payload{Name: "q"}, queued); err != nil {
			t.Fatal(err)
		}
	}
	ensureStatus(t, r, "q1", StatusQueued)

	s, err := r.Cancel(ctx, "q1", "user-abort")
	judge(t, "cancel-queued", "取消排队任务 q1",
		fmt.Sprintf("code=%v status=%s", errCode(err), s.Status), "nil/canceled")
	if err != nil || s.Status != StatusCanceled {
		t.Fatalf("排队任务应取消成功，status=%s err=%v", s.Status, err)
	}

	close(release)
	waitTerminal(t, r, "head")
	waitTerminal(t, r, "q1")
	time.Sleep(30 * time.Millisecond) // 给 q2 执行窗口

	if atomic.LoadInt32(&ran) != 1 {
		t.Fatalf("q1 被取消后不得执行，q2 应执行一次，实际 ran=%d", ran)
	}
	q1, _ := r.Get(ctx, "q1")
	judge(t, "canceled-result", "q1 终态", q1.Status, StatusCanceled)
	if q1.Status != StatusCanceled || q1.Result != nil {
		t.Fatalf("q1 应保持 canceled 且无结果，实际 %+v", q1)
	}
}

func TestCancelRunningDiscardsResult(t *testing.T) {
	r := newTestRunner(t, Config{MaxWorkers: 1, MaxQueue: 1})
	ctx := context.Background()

	started := make(chan struct{})
	var finishedAfterCancel int32
	task := func(ctx context.Context) (any, error) {
		close(started)
		<-ctx.Done() // 协作响应取消
		atomic.AddInt32(&finishedAfterCancel, 1)
		return "late-result", nil
	}
	if _, err := r.Submit(ctx, "run1", Payload{Name: "r"}, task); err != nil {
		t.Fatal(err)
	}
	<-started

	s, err := r.Cancel(ctx, "run1", "no longer needed")
	if err != nil || s.Status != StatusCanceled {
		t.Fatalf("运行中任务应可取消，status=%s err=%v", s.Status, err)
	}
	judge(t, "cancel-running", "取消运行中任务", s.Status, StatusCanceled)

	// 取消后再观察，状态必须仍是 canceled，业务返回值不得成为结果。
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&finishedAfterCancel) == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	st, _ := r.Get(ctx, "run1")
	judge(t, "no-result-after-cancel", "业务在取消后返回时再观察",
		fmt.Sprintf("status=%s result=%v errCode=%s", st.Status, st.Result, errCode(st.Err)),
		"canceled/<nil>/task_canceled")
	if st.Status != StatusCanceled || st.Result != nil {
		t.Fatalf("取消后不得出现可观测完成结果，实际 %+v", st)
	}
	if errCode(st.Err) != CodeTaskCanceled {
		t.Fatalf("取消原因应为 task_canceled，实际 %v", st.Err)
	}

	// 幂等取消：再次取消返回同一状态而不报错。
	s2, err := r.Cancel(ctx, "run1", "again")
	if err != nil || s2.Status != StatusCanceled {
		t.Fatalf("重复取消应幂等，status=%s err=%v", s2.Status, err)
	}
}

func TestCancelFinishedTaskIsTerminal(t *testing.T) {
	r := newTestRunner(t, Config{MaxWorkers: 1, MaxQueue: 1})
	ctx := context.Background()
	if _, err := r.Submit(ctx, "done", Payload{Name: "d"}, quickFunc("v")); err != nil {
		t.Fatal(err)
	}
	waitTerminal(t, r, "done")

	_, err := r.Cancel(ctx, "done", "late")
	judge(t, "cancel-terminal", "取消已成功任务", errCode(err), CodeTerminal)
	if errCode(err) != CodeTerminal || !errors.Is(err, ErrTerminal) {
		t.Fatalf("应返回 terminal，实际 %v", err)
	}
}

func ensureStatus(t *testing.T, r *Runner, key string, want Status) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s, ok := r.Get(context.Background(), key); ok && s.Status == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("任务 %s 未在超时内达到状态 %s", key, want)
}
