package taskrunner

import (
	"context"
	"fmt"
	"sort"
	"sync/atomic"
	"testing"
	"time"
)

// percentile 返回已排序延迟切片的第 p 百分位。
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p)
	return sorted[idx]
}

// BenchmarkSubmitThroughput 度量“提交 + 执行”路径在并行提交下的吞吐。
// 每个任务做约 200µs 的模拟工作；可用 -benchtime 控制负载，例如：
//
//	go test -run='^$' -bench=BenchmarkSubmitThroughput -benchtime=20000x
func BenchmarkSubmitThroughput(b *testing.B) {
	cfg := Config{Workers: 32, QueueSize: 1024, QueueWaitTimeout: 5 * time.Second, ShutdownGrace: 10 * time.Second}
	r, err := New(cfg)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := r.Shutdown(ctx); err != nil {
			b.Fatal(err)
		}
	})

	work := func(ctx context.Context, p []byte) ([]byte, error) {
		time.Sleep(200 * time.Microsecond)
		return p, nil
	}
	var seq atomic.Int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			key := fmt.Sprintf("bench-%d", seq.Add(1))
			if _, err := r.Submit(context.Background(), SubmitRequest{
				IdempotencyKey: key, Payload: []byte(key), Func: work,
			}); err != nil {
				b.Fatalf("submit under benchmark capacity: %v", err)
			}
		}
	})
	b.StopTimer()
}

// TestStressLoad 是固定负载、可在本机重复运行的压测：
// 32 worker / 256 队列，提交 3000 个 ~200µs 任务，统计
// 提交延迟与端到端延迟（FinishedAt - SubmittedAt）的分位数与吞吐。
func TestStressLoad(t *testing.T) {
	const total = 3000
	cfg := Config{Workers: 32, QueueSize: 256, QueueWaitTimeout: 5 * time.Second, ShutdownGrace: 30 * time.Second}
	r, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	submitLat := make([]time.Duration, 0, total)
	submitLatCh := make(chan time.Duration, total)
	startWall := time.Now()
	for i := 0; i < total; i++ {
		key := fmt.Sprintf("stress-%05d", i)
		req := SubmitRequest{IdempotencyKey: key, Payload: []byte(key),
			Func: func(ctx context.Context, p []byte) ([]byte, error) {
				time.Sleep(200 * time.Microsecond)
				return p, nil
			}}
		t0 := time.Now()
		if _, err := r.Submit(context.Background(), req); err != nil {
			t.Fatalf("submit %s: %v", key, err)
		}
		submitLatCh <- time.Since(t0)
	}
	close(submitLatCh)
	for d := range submitLatCh {
		submitLat = append(submitLat, d)
	}
	submitWall := time.Since(startWall)

	// 等待全部到达终态，采集端到端延迟。
	e2e := make([]time.Duration, 0, total)
	if !waitForTrue(20*time.Second, func() bool {
		if int(r.Stats().SubmittedTotal) != total {
			return false
		}
		e2e = e2e[:0]
		for i := 0; i < total; i++ {
			info, gerr := r.Get(context.Background(), fmt.Sprintf("stress-%05d", i))
			if gerr != nil || !info.Status.IsTerminal() {
				return false
			}
			if info.Status != StatusSucceeded {
				t.Fatalf("unexpected non-success under capacity: %s", info.Status)
			}
			e2e = append(e2e, info.FinishedAt.Sub(info.SubmittedAt))
		}
		return true
	}) {
		t.Fatalf("tasks did not all settle, stats=%+v", r.Stats())
	}
	totalWall := time.Since(startWall)

	if err := r.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	sort.Slice(submitLat, func(i, j int) bool { return submitLat[i] < submitLat[j] })
	sort.Slice(e2e, func(i, j int) bool { return e2e[i] < e2e[j] })

	throughput := float64(total) / totalWall.Seconds()
	t.Logf("[CASE] StressLoad input workers=%d queue=%d tasks=%d work=200µs | "+
		"verdict rejected=%d cancel=%d unfinished=%d | "+
		"submit-wall=%s submit-lat p50=%s p95=%s p99=%s max=%s | "+
		"e2e p50=%s p95=%s p99=%s max=%s | throughput=%.0f tasks/s total-wall=%s",
		cfg.Workers, cfg.QueueSize, total,
		r.Stats().RejectedTotal, r.Stats().CancelledTotal, r.Stats().UnfinishedTotal,
		submitWall,
		percentile(submitLat, .5), percentile(submitLat, .95),
		percentile(submitLat, .99), percentile(submitLat, 1),
		percentile(e2e, .5), percentile(e2e, .95), percentile(e2e, .99), percentile(e2e, 1),
		throughput, totalWall)

	if r.Stats().RejectedTotal != 0 || r.Stats().UnfinishedTotal != 0 {
		t.Fatal("capacity was sufficient: no rejection/unfinished expected")
	}
	if throughput < 10000 {
		t.Fatalf("throughput unexpectedly low: %.0f tasks/s", throughput)
	}
}
