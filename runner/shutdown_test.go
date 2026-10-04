package runner

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func newManualRunner(t *testing.T, cfg Config) *Runner {
	t.Helper()
	r, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return r // 不自动 Shutdown
}

func TestShutdownGraceRunningTasksSucceed(t *testing.T) {
	// 宽限 2s：在途任务协作响应 ctx 并在宽限期内正常结算。
	r := newManualRunner(t, Config{MaxWorkers: 2, MaxQueue: 2, ShutdownGrace: 2 * time.Second})
	ctx := context.Background()

	started := make(chan struct{})
	task := func(ctx context.Context) (any, error) {
		close(started)
		<-ctx.Done() // 关闭时收到取消信号
		time.Sleep(20 * time.Millisecond)
		return "finished-during-grace", nil
	}
	if _, err := r.Submit(ctx, "g1", Payload{Name: "g"}, task); err != nil {
		t.Fatal(err)
	}
	<-started

	begin := time.Now()
	if err := r.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(begin)

	s, _ := r.Get(ctx, "g1")
	judge(t, "shutdown-grace-success", "在途任务在宽限期内收敛",
		fmt.Sprintf("status=%s result=%v elapsed=%v", s.Status, s.Result, elapsed.Round(10*time.Millisecond)),
		"succeeded/finished-during-grace")
	if s.Status != StatusSucceeded || s.Result != "finished-during-grace" {
		t.Fatalf("宽限期内正常返回的任务应保留成功结果，实际 %+v", s)
	}
}

func TestShutdownAbortAfterGrace(t *testing.T) {
	// 宽限 80ms：任务无视 ctx 继续运行，超时后必须标记为 aborted。
	r := newManualRunner(t, Config{MaxWorkers: 1, MaxQueue: 1, ShutdownGrace: 80 * time.Millisecond})
	ctx := context.Background()

	started := make(chan struct{})
	released := make(chan struct{})
	task := func(ctx context.Context) (any, error) {
		close(started)
		<-released // 模拟不响应取消的失控任务
		return "should-not-observe", nil
	}
	if _, err := r.Submit(ctx, "stuck", Payload{Name: "s"}, task); err != nil {
		t.Fatal(err)
	}
	<-started

	begin := time.Now()
	if err := r.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(begin)

	s, _ := r.Get(ctx, "stuck")
	judge(t, "shutdown-abort", "宽限期后仍未结束",
		fmt.Sprintf("status=%s errCode=%s elapsed=%v", s.Status, errCode(s.Err), elapsed.Round(10*time.Millisecond)),
		"aborted/aborted ~80ms")
	if s.Status != StatusAborted {
		t.Fatalf("应标记 aborted，实际 %s", s.Status)
	}
	if errCode(s.Err) != CodeAborted {
		t.Fatalf("错误原因应为 aborted，实际 %v", s.Err)
	}
	// 让失控协程退出，避免泄漏影响其它测试进程。
	close(released)
	time.Sleep(20 * time.Millisecond)
	// 失控任务晚到的返回值不得覆盖 aborted。
	s2, _ := r.Get(ctx, "stuck")
	if s2.Status != StatusAborted || s2.Result != nil {
		t.Fatalf("晚到结果不得覆盖 aborted，实际 %+v", s2)
	}
}

func TestShutdownDrainsQueuedAndRejectsNew(t *testing.T) {
	r := newManualRunner(t, Config{MaxWorkers: 1, MaxQueue: 4, ShutdownGrace: time.Second})
	ctx := context.Background()

	started := make(chan struct{})
	release := make(chan struct{})
	head := func(ctx context.Context) (any, error) {
		close(started)
		select {
		case <-release:
			return "h", nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if _, err := r.Submit(ctx, "head", Payload{Name: "h"}, head); err != nil {
		t.Fatal(err)
	}
	<-started
	var queuedRan int32
	qf := func(ctx context.Context) (any, error) { atomic.AddInt32(&queuedRan, 1); return 1, nil }
	for _, k := range []string{"q1", "q2"} {
		if _, err := r.Submit(ctx, k, Payload{Name: "q"}, qf); err != nil {
			t.Fatal(err)
		}
	}

	// 关闭瞬间并发提交新任务，只可能有三种可解释结局：
	//  1. 关闭开始后到达 -> shutting_down（rejected）；
	//  2. 容量已满且 wait=0（1 running + 4 queue）-> queue_full（rejected）；
	//  3. 关闭开始前恰好入队 -> 宽限期内被排空执行 -> succeeded。
	// 不允许出现结果丢失或无法解释的结局。
	const n = 20
	type outcome struct {
		state *TaskState
		code  Code
	}
	outs := make(chan outcome, n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			s, err := r.Submit(ctx, fmt.Sprintf("late-%d", i), Payload{Name: "late"}, quickFunc(i))
			outs <- outcome{state: s, code: errCode(err)}
		}()
	}

	if err := r.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	close(release)

	rejected, admitted := 0, 0
	for i := 0; i < n; i++ {
		o := <-outs
		switch o.code {
		case CodeShuttingDown, CodeQueueFull:
			if o.state != nil && o.state.Status != StatusRejected {
				t.Fatalf("%s 拒绝应为 rejected 终态，实际 %s", o.code, o.state.Status)
			}
			rejected++
		case "":
			// 关闭前入队：宽限期内被排空执行，应保留成功结果。
			st, ok := r.Get(ctx, o.state.IdemKey)
			if !ok || st.Status != StatusSucceeded {
				t.Fatalf("关闭前入队任务应在宽限期内执行成功，实际 %+v ok=%v", st, ok)
			}
			admitted++
		default:
			t.Fatalf("关闭期间提交出现未预期结局 %s", o.code)
		}
	}
	judge(t, "shutdown-rejects-new", "关闭期间 20 个并发新提交",
		fmt.Sprintf("rejected(shutting_down/queue_full)=%d admitted-then-drained=%d", rejected, admitted),
		"两类之和=20，无结果丢失")
	if rejected+admitted != n {
		t.Fatalf("结局总数应为 %d，实际 %d+%d", n, rejected, admitted)
	}

	// 排队任务在宽限期内被排空执行并保留成功结果。
	for _, k := range []string{"q1", "q2"} {
		s, _ := r.Get(ctx, k)
		if s.Status != StatusSucceeded {
			t.Fatalf("排队任务 %s 应在宽限期内执行成功，实际 %s", k, s.Status)
		}
	}
	judge(t, "shutdown-queued-drained", "q1/q2 状态",
		statuses(r, "q1", "q2"), "succeeded,succeeded")
	if atomic.LoadInt32(&queuedRan) != 2 {
		t.Fatalf("排队任务应在宽限期内各执行一次，实际执行 %d 次", queuedRan)
	}

	// 关闭后的提交一律拒绝。
	_, err := r.Submit(ctx, "after", Payload{Name: "a"}, quickFunc(1))
	if errCode(err) != CodeShuttingDown {
		t.Fatalf("关闭后提交应返回 shutting_down，实际 %v", err)
	}
	// 关闭幂等：重复调用立即返回。
	if err := r.Shutdown(context.Background()); err != nil {
		t.Fatalf("重复 Shutdown 应幂等，实际 %v", err)
	}
}

func TestShutdownZeroGraceAbortsRunning(t *testing.T) {
	r := newManualRunner(t, Config{MaxWorkers: 1, MaxQueue: 1, ShutdownGrace: 0})
	ctx := context.Background()
	started := make(chan struct{})
	release := make(chan struct{})
	task := func(ctx context.Context) (any, error) {
		close(started)
		<-release
		return nil, nil
	}
	if _, err := r.Submit(ctx, "z", Payload{Name: "z"}, task); err != nil {
		t.Fatal(err)
	}
	<-started
	if err := r.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	s, _ := r.Get(ctx, "z")
	judge(t, "zero-grace", "grace=0 的在途任务", s.Status, StatusAborted)
	if s.Status != StatusAborted {
		t.Fatalf("零宽限期应立即 abort，实际 %s", s.Status)
	}
	close(release)
}

func statuses(r *Runner, keys ...string) string {
	out := ""
	for i, k := range keys {
		s, _ := r.Get(context.Background(), k)
		if i > 0 {
			out += ","
		}
		out += string(s.Status)
	}
	return out
}
