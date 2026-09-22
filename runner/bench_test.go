package runner

import (
	"context"
	"fmt"
	"sort"
	"sync/atomic"
	"testing"
	"time"
)

// BenchmarkSubmitThroughput 测量轻量任务在有界并发下的提交+收敛吞吐。
// 运行：go test ./runner/ -bench=BenchmarkSubmitThroughput -benchtime=2s
func BenchmarkSubmitThroughput(b *testing.B) {
	cfg := Config{MaxWorkers: 16, MaxQueue: 4096, MaxQueueWait: 2 * time.Second}
	r, err := New(cfg)
	if err != nil {
		b.Fatal(err)
	}
	defer r.Shutdown(context.Background())
	ctx := context.Background()

	var i int64
	fn := func(ctx context.Context) (any, error) { return 1, nil }
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n := atomic.AddInt64(&i, 1)
			key := fmt.Sprintf("bench-%d", n)
			if _, err := r.Submit(ctx, key, Payload{Name: "bench", Params: n}, fn); err != nil {
				b.Fatalf("提交失败: %v", err)
			}
		}
	})
	// 等待全部收敛后再结束计时区间，统计完整吞吐。
	for n := int64(1); n <= i; n++ {
		waitTerminalB(b, r, fmt.Sprintf("bench-%d", n))
	}
	b.StopTimer()
	st := r.Stats()
	b.ReportMetric(float64(st.Succeeded), "succeeded-total")
}

// BenchmarkSubmitTailLatency 在固定负载下报告提交受理延迟的 p50/p95/p99。
func BenchmarkSubmitTailLatency(b *testing.B) {
	cfg := Config{MaxWorkers: 8, MaxQueue: 1024, MaxQueueWait: 2 * time.Second}
	r, err := New(cfg)
	if err != nil {
		b.Fatal(err)
	}
	defer r.Shutdown(context.Background())
	ctx := context.Background()

	latencies := make([]time.Duration, 0, b.N)
	fn := func(ctx context.Context) (any, error) {
		// 极小工作量，用于暴露排队带来的尾延迟。
		time.Sleep(20 * time.Microsecond)
		return 1, nil
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("lat-%d", i)
		start := time.Now()
		if _, err := r.Submit(ctx, key, Payload{Name: "lat", Params: i}, fn); err != nil {
			b.Fatalf("提交失败: %v", err)
		}
		latencies = append(latencies, time.Since(start))
	}
	for i := 0; i < b.N; i++ {
		waitTerminalB(b, r, fmt.Sprintf("lat-%d", i))
	}
	b.StopTimer()

	sort.Slice(latencies, func(a, c int) bool { return latencies[a] < latencies[c] })
	pick := func(p float64) time.Duration {
		idx := int(float64(len(latencies)) * p)
		if idx >= len(latencies) {
			idx = len(latencies) - 1
		}
		return latencies[idx]
	}
	b.ReportMetric(float64(pick(0.50).Microseconds()), "p50-submit-μs")
	b.ReportMetric(float64(pick(0.95).Microseconds()), "p95-submit-μs")
	b.ReportMetric(float64(pick(0.99).Microseconds()), "p99-submit-μs")
}

// BenchmarkOverloadedReject 测量过载时拒绝路径：必须立即返回、不堆积、不改已有任务。
func BenchmarkOverloadedReject(b *testing.B) {
	r, block := newBenchBlocker(b)
	defer r.Shutdown(context.Background())
	defer close(block)
	ctx := context.Background()

	var seq, rejected, admitted int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n := atomic.AddInt64(&seq, 1)
			key := fmt.Sprintf("over-%d", n)
			if _, err := r.Submit(ctx, key, Payload{Name: "o", Params: n}, quickFunc(1)); err != nil {
				atomic.AddInt64(&rejected, 1)
			} else {
				atomic.AddInt64(&admitted, 1)
			}
		}
	})
	b.StopTimer()
	b.ReportMetric(float64(atomic.LoadInt64(&rejected)), "rejected")
	b.ReportMetric(float64(atomic.LoadInt64(&admitted)), "admitted")
}

// newBenchBlocker 预置占满 worker+queue 的阻塞任务，返回 runner 与其释放通道。
func newBenchBlocker(b *testing.B) (*Runner, chan struct{}) {
	r, err := New(Config{MaxWorkers: 4, MaxQueue: 16, MaxQueueWait: 0})
	if err != nil {
		b.Fatal(err)
	}
	block := make(chan struct{})
	for i := 0; i < 20; i++ {
		if _, err := r.Submit(context.Background(), fmt.Sprintf("block-%d", i), Payload{Name: "b"},
			func(ctx context.Context) (any, error) {
				select {
				case <-block:
					return nil, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}); err != nil {
			b.Fatalf("预置阻塞任务失败: %v", err)
		}
	}
	return r, block
}

func waitTerminalB(b *testing.B, r *Runner, key string) {
	b.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if s, ok := r.Get(context.Background(), key); ok && s.Status.IsTerminal() {
			return
		}
		time.Sleep(50 * time.Microsecond)
	}
	b.Fatalf("任务 %s 未收敛", key)
}
