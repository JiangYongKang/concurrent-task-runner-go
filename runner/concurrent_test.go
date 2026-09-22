package runner

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

// TestConcurrentDuplicateSubmit 验证高并发下同键重复提交绝不触发重复执行。
func TestConcurrentDuplicateSubmit(t *testing.T) {
	r := newTestRunner(t, Config{MaxWorkers: 4, MaxQueue: 64, MaxQueueWait: 5_000_000_000})
	ctx := context.Background()

	var runs int32
	started := make(chan struct{})
	release := make(chan struct{})
	fn := func(ctx context.Context) (any, error) {
		atomic.AddInt32(&runs, 1)
		close(started) // 单执行者时安全
		<-release
		return "唯一结果", nil
	}

	const n = 64
	var wg sync.WaitGroup
	errs := make([]error, n)
	statuses := make([]string, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s, err := r.Submit(ctx, "dup-key", Payload{Name: "pay", Params: 42}, fn)
			errs[i] = err
			statuses[i] = string(s.Status)
		}(i)
	}

	<-started // 等唯一一次执行开始，期间其它提交应在等待重放
	close(release)
	wg.Wait()

	// 首个提交者在“入队即返回”时可能观测到 queued（提交表示受理，不等待完成）；
	// 其余重复提交者阻塞至终态后重放 succeeded。核心不变量：只执行一次、最终结果唯一。
	replayOK, acceptedNonTerminal := 0, 0
	for i := range errs {
		switch {
		case errs[i] == nil && statuses[i] == string(StatusSucceeded):
			replayOK++
		case errs[i] == nil && (statuses[i] == string(StatusQueued) || statuses[i] == string(StatusRunning)):
			acceptedNonTerminal++
		default:
			t.Errorf("goroutine %d 未预期结局: status=%s err=%v", i, statuses[i], errs[i])
		}
	}
	judge(t, "concurrent-duplicate",
		fmt.Sprintf("goroutines=%d payload 相同", n),
		fmt.Sprintf("runs=%d succeeded-replays=%d accepted-early=%d",
			atomic.LoadInt32(&runs), replayOK, acceptedNonTerminal),
		"runs=1, replay+accepted=64")
	if atomic.LoadInt32(&runs) != 1 {
		t.Fatalf("同一幂等键只应执行一次，实际 %d 次", runs)
	}
	if replayOK+acceptedNonTerminal != n {
		t.Fatalf("64 个提交结局应全部可解释，实际 replay=%d accepted=%d", replayOK, acceptedNonTerminal)
	}

	final := waitTerminal(t, r, "dup-key")
	if final.Status != StatusSucceeded || final.Result != "唯一结果" {
		t.Fatalf("任务最终应为唯一成功结果，实际 %+v", final)
	}
	if acceptedNonTerminal > 1 {
		t.Fatalf("只应有 1 个首提交者提前返回，实际 %d", acceptedNonTerminal)
	}
}

// TestConcurrentSubmitDistinctKeys 验证不同键高并发下状态与计数一致。
func TestConcurrentSubmitDistinctKeys(t *testing.T) {
	r := newTestRunner(t, Config{MaxWorkers: 8, MaxQueue: 128, MaxQueueWait: 5_000_000_000})
	ctx := context.Background()

	const n = 500
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("k-%d", i)
			if _, err := r.Submit(ctx, key, Payload{Name: "work", Params: i}, quickFunc(i)); err != nil {
				t.Errorf("提交 %s 失败: %v", key, err)
				return
			}
		}(i)
	}
	wg.Wait()
	for i := 0; i < n; i++ {
		waitTerminal(t, r, fmt.Sprintf("k-%d", i))
	}
	st := r.Stats()
	judge(t, "concurrent-distinct", fmt.Sprintf("keys=%d workers=8 queue=128", n),
		fmt.Sprintf("submitted=%d succeeded=%d", st.Submitted, st.Succeeded),
		"submitted=500 succeeded=500")
	if st.Submitted != n || st.Succeeded != n {
		t.Fatalf("计数不一致: %+v", st)
	}
}

// TestConcurrentCancelRace 在任务运行中并发取消与重复提交，验证状态唯一且可解释。
func TestConcurrentCancelRace(t *testing.T) {
	r := newTestRunner(t, Config{MaxWorkers: 2, MaxQueue: 8, MaxQueueWait: 5_000_000_000})
	ctx := context.Background()

	started := make(chan struct{})
	fn := func(ctx context.Context) (any, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if _, err := r.Submit(ctx, "race-1", Payload{Name: "r"}, fn); err != nil {
		t.Fatal(err)
	}
	<-started

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, _ = r.Cancel(ctx, "race-1", "concurrent-cancel")
		}()
		go func() {
			defer wg.Done()
			s, err := r.Submit(ctx, "race-1", Payload{Name: "r"}, fn)
			// 取消先发生：重放应得到 canceled；绝不会出现 succeeded。
			if s.Status == StatusSucceeded {
				t.Errorf("取消竞态后不得观测到成功")
			}
			_ = err
		}()
	}
	wg.Wait()

	s, _ := r.Get(ctx, "race-1")
	judge(t, "cancel-submit-race", "16 并发取消 + 16 并发重复提交",
		string(s.Status), "canceled")
	if s.Status != StatusCanceled {
		t.Fatalf("终态应为 canceled，实际 %s", s.Status)
	}
}
