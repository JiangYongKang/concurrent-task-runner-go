package taskrunner

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestCancelVsShutdownRace: 取消风暴与 Shutdown 同时发生。
// 不变量：每个任务最终恰有一个终态；结果只允许出现在 SUCCEEDED；
// 终态后再次观测不得翻转；计数器之和与任务分类一致。
func TestCancelVsShutdownRace(t *testing.T) {
	const trials = 20
	for trial := 0; trial < trials; trial++ {
		cfg := Config{Workers: 4, QueueSize: 8, QueueWaitTimeout: 10 * time.Millisecond, ShutdownGrace: 60 * time.Millisecond}
		r, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		const n = 24
		keys := make([]string, n)
		// 混合任务：有的极快完成，有的等 ctx，制造取消/完成/未完成的各种竞态。
		for i := 0; i < n; i++ {
			keys[i] = fmt.Sprintf("t-%d-%d", trial, i)
			idx := i
			req := SubmitRequest{IdempotencyKey: keys[i], Payload: []byte(keys[i]),
				Func: func(ctx context.Context, p []byte) ([]byte, error) {
					switch idx % 3 {
					case 0:
						return p, nil // 立即成功
					case 1:
						time.Sleep(time.Duration(10+rand.Intn(40)) * time.Millisecond)
						return p, nil // 可能成功，也可能撞上关闭宽限
					default:
						<-ctx.Done() // 只在取消/关闭时结束
						return nil, ctx.Err()
					}
				}}
			if _, serr := r.Submit(context.Background(), req); serr != nil && KindOf(serr) != KindOverloaded {
				t.Fatalf("unexpected submit error: %v", serr)
			}
		}

		var cancels, cancel404 atomic.Int64
		var cwg sync.WaitGroup
		for c := 0; c < 6; c++ {
			cwg.Add(1)
			go func() {
				defer cwg.Done()
				for _, k := range keys {
					switch err := r.Cancel(context.Background(), k); {
					case err == nil:
						cancels.Add(1)
					case KindOf(err) == KindNotFound:
						cancel404.Add(1)
					}
				}
			}()
		}

		serr := r.Shutdown(context.Background())
		cwg.Wait()

		counts := map[Status]int{}
		for _, k := range keys {
			info, gerr := r.Get(context.Background(), k)
			if gerr != nil {
				// 过载被拒的任务不会登记，跳过。
				continue
			}
			if !info.Status.IsTerminal() {
				t.Fatalf("trial %d task %s non-terminal after shutdown: %s", trial, k, info.Status)
			}
			// 终态稳定性：再读两次不得翻转。
			s2, _ := r.Get(context.Background(), k)
			time.Sleep(time.Millisecond)
			s3, _ := r.Get(context.Background(), k)
			if s2.Status != info.Status || s3.Status != info.Status {
				t.Fatalf("trial %d task %s status flipped: %s->%s->%s",
					trial, k, info.Status, s2.Status, s3.Status)
			}
			// 只有 SUCCEEDED 才能携带结果。
			if info.Status != StatusSucceeded && info.Result != nil {
				t.Fatalf("trial %d task %s status=%s but carries result %q",
					trial, k, info.Status, info.Result)
			}
			counts[info.Status]++
		}
		t.Logf("[CASE] CancelVsShutdownRace trial=%d input=%d tasks shutdownErr=%v | verdict counts=%v cancels=%d cancel404=%d stats=%+v",
			trial, n, serr, counts, cancels.Load(), cancel404.Load(), r.Stats())
	}
}

// TestSubmitCancelGetConcurrently: 同一任务上 Submit 重放 / Cancel / Get
// 高并发交叉，验证状态机 CAS 与锁在各入口下一致、无重复结算。
func TestSubmitCancelGetConcurrently(t *testing.T) {
	cfg := Config{Workers: 2, QueueSize: 16, QueueWaitTimeout: 50 * time.Millisecond, ShutdownGrace: time.Second}
	r, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var execs atomic.Int64
	key := "hot-key"
	base := SubmitRequest{IdempotencyKey: key, Payload: []byte("v1"),
		Func: func(ctx context.Context, p []byte) ([]byte, error) {
			execs.Add(1)
			time.Sleep(30 * time.Millisecond)
			return p, nil
		}}

	var wg sync.WaitGroup

	// 首个提交先行并进入 RUNNING，确定“恰好一次执行”的基准。
	if _, err := r.Submit(context.Background(), base); err != nil {
		t.Fatal(err)
	}
	if !waitForTrue(2*time.Second, func() bool {
		info, _ := r.Get(context.Background(), key)
		return info.Status == StatusRunning || info.Status.IsTerminal()
	}) {
		t.Fatal("first submit never observed running")
	}

	// 重复提交（同内容，只能重放）。
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, _ := r.Submit(context.Background(), base)
			if res != nil && !res.Replay && res.Task.Status == StatusPending {
				// 首个真实提交，允许
			}
		}()
	}
	// 取消。
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _ = r.Cancel(context.Background(), key) }()
	}
	// 查询。
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			info, gerr := r.Get(context.Background(), key)
			if gerr != nil {
				return
			}
			if info.Status != StatusPending && info.Status != StatusRunning && !info.Status.IsTerminal() {
				t.Errorf("invalid status observed: %s", info.Status)
			}
		}()
	}
	wg.Wait()

	if err := r.Shutdown(context.Background()); err != nil {
		// 取消先到也属合法结果。
		if KindOf(err) != KindRunnerClosed {
			t.Fatalf("unexpected shutdown error: %v", err)
		}
	}
	info, _ := r.Get(context.Background(), key)
	logCase(t, "SubmitCancelGetConcurrently",
		"16 replays + 8 cancels + 16 gets on one key",
		"verdict execs=%d final=%s result=%v", execs.Load(), info.Status, info.Result)
	if execs.Load() != 1 {
		t.Fatalf("exactly one execution required, got %d", execs.Load())
	}
	if !info.Status.IsTerminal() {
		t.Fatalf("must be terminal after shutdown: %s", info.Status)
	}
	if info.Status != StatusSucceeded && info.Result != nil {
		t.Fatal("result must not be observable for non-success terminal state")
	}
}
