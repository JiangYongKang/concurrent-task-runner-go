// Command stress 在本机对 runner 做可重复的并发压测，输出吞吐与尾延迟，
// 并可注入过载、取消与优雅关闭阶段，验证各结局计数自洽。
//
// 示例：
//
//	go run ./cmd/stress -workers=8 -queue=256 -n=20000 -conc=64 -work=100µs
//	go run ./cmd/stress -workers=2 -queue=4 -n=5000 -conc=32 -work=2ms -overload
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/highcumontoa/concurrent-task-runner-go/runner"
)

type counters struct {
	ok           int64
	rejectedFull int64
	rejectedWait int64
	shutting     int64
	conflict     int64
	other        int64
}

func main() {
	var (
		workers   = flag.Int("workers", 8, "最大并发执行数")
		queue     = flag.Int("queue", 256, "最大排队长度")
		wait      = flag.Duration("queue-wait", 50*time.Millisecond, "队列满时的等待上限")
		grace     = flag.Duration("grace", 3*time.Second, "关闭宽限期")
		n         = flag.Int("n", 10000, "提交任务总数")
		conc      = flag.Int("conc", 32, "并发提交协程数")
		work      = flag.Duration("work", 200*time.Microsecond, "单任务模拟耗时")
		overload  = flag.Bool("overload", false, "使用小容量制造过载，统计拒绝")
		cancelPct = flag.Int("cancel-pct", 0, "随机取消已提交任务的百分比 0-100")
		dupPct    = flag.Int("dup-pct", 0, "重复使用幂等键的百分比（测试幂等重放）0-100")
		retryPct  = flag.Int("retry-pct", 0, "携带重试策略且前两次瞬时失败的任务百分比 0-100")
	)
	flag.Parse()

	if *overload {
		*workers = 2
		*queue = 4
		*wait = 5 * time.Millisecond
	}

	cfg := runner.Config{
		MaxWorkers:    *workers,
		MaxQueue:      *queue,
		MaxQueueWait:  *wait,
		ShutdownGrace: *grace,
	}
	r, err := runner.New(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "创建 runner 失败: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("配置: workers=%d queue=%d queueWait=%s grace=%s n=%d conc=%d work=%s retry-pct=%d GOMAXPROCS=%d\n",
		*workers, *queue, *wait, *grace, *n, *conc, *work, *retryPct, runtime.GOMAXPROCS(0))

	latencies := make([]time.Duration, 0, *n)
	var mu sync.Mutex
	var c counters
	var submitted int64

	// 可选取消器：随机取消一部分任务。
	cancelStop := make(chan struct{})
	var cancelWg sync.WaitGroup
	if *cancelPct > 0 {
		cancelWg.Add(1)
		go func() {
			defer cancelWg.Done()
			ticker := time.NewTicker(100 * time.Microsecond)
			defer ticker.Stop()
			for {
				select {
				case <-cancelStop:
					return
				case <-ticker.C:
					id := atomic.LoadInt64(&submitted)
					if id == 0 {
						continue
					}
					key := fmt.Sprintf("task-%d", id-1)
					_, _ = r.Cancel(context.Background(), key, "stress-random-cancel")
				}
			}
		}()
	}

	ctx := context.Background()
	start := time.Now()
	var wg sync.WaitGroup
	for g := 0; g < *conc; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for {
				i := atomic.AddInt64(&submitted, 1) - 1
				if i >= int64(*n) {
					return
				}
				// dupPct 概率复用较早的幂等键，触发幂等重放/冲突路径。
				key := fmt.Sprintf("task-%d", i)
				payload := runner.Payload{Name: "work", Params: i}
				if *dupPct > 0 && i > 10 && int(i%100) < *dupPct {
					key = fmt.Sprintf("task-%d", (i/10)*10) // 复用 10 个一批中的某个键
					// 故意一半概率携带不同参数以制造冲突，另一半完全相同以重放。
					if i%2 == 0 {
						payload.Params = -i
					} else {
						payload.Params = int64(0)
					}
				}
				// retryPct 概率携带重试策略：前两次返回可重试的瞬时失败，第三次成功。
				useRetry := *retryPct > 0 && int(i%100) < *retryPct
				if useRetry {
					payload.Retry = &runner.RetryPolicy{
						MaxAttempts: 3,
						Interval:    2 * time.Millisecond,
						Backoff:     runner.BackoffExponential,
					}
				}
				var attempts int32
				fn := func(ctx context.Context) (any, error) {
					fail := useRetry && atomic.AddInt32(&attempts, 1) < 3
					select {
					case <-time.After(*work):
						if fail {
							return nil, fmt.Errorf("transient-%d", i)
						}
						return i, nil
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				}
				t0 := time.Now()
				_, err := r.Submit(ctx, key, payload, fn)
				d := time.Since(t0)
				mu.Lock()
				latencies = append(latencies, d)
				mu.Unlock()
				classify(&c, err)
			}
		}(g)
	}
	wg.Wait()
	close(cancelStop)
	cancelWg.Wait()
	submitElapsed := time.Since(start)

	// 优雅关闭：宽限期内收敛，超时标记 aborted。
	shutdownStart := time.Now()
	if err := r.Shutdown(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "shutdown 失败: %v\n", err)
	}
	shutdownElapsed := time.Since(shutdownStart)

	st := r.Stats()
	mu.Lock()
	sort.Slice(latencies, func(a, b int) bool { return latencies[a] < latencies[b] })
	pct := func(p float64) time.Duration {
		idx := int(float64(len(latencies)) * p)
		if idx >= len(latencies) {
			idx = len(latencies) - 1
		}
		return latencies[idx]
	}
	p50, p95, p99, pMax := pct(0.5), pct(0.95), pct(0.99), latencies[len(latencies)-1]
	mu.Unlock()

	total := c.ok + c.rejectedFull + c.rejectedWait + c.shutting + c.conflict + c.other
	qps := float64(total) / submitElapsed.Seconds()
	fmt.Println("----------------------------------------")
	fmt.Printf("提交阶段耗时: %s  有效提交 QPS(含拒绝): %.0f\n", submitElapsed.Round(time.Microsecond), qps)
	fmt.Printf("关闭阶段耗时: %s (grace=%s)\n", shutdownElapsed.Round(time.Microsecond), *grace)
	fmt.Printf("提交受理延迟: p50=%s p95=%s p99=%s max=%s\n", p50.Round(time.Microsecond),
		p95.Round(time.Microsecond), p99.Round(time.Microsecond), pMax.Round(time.Microsecond))
	fmt.Println("提交侧结局:")
	fmt.Printf("  成功/重放=%d queue_full=%d queue_timeout=%d shutting_down=%d idempotent_conflict=%d other=%d (合计=%d)\n",
		c.ok, c.rejectedFull, c.rejectedWait, c.shutting, c.conflict, c.other, total)
	fmt.Println("服务端终态计数:")
	fmt.Printf("  submitted=%d succeeded=%d failed=%d canceled=%d rejected=%d aborted=%d (关闭后 retrying=%d)\n",
		st.Submitted, st.Succeeded, st.Failed, st.Canceled, st.Rejected, st.Aborted, st.Retrying)

	// 自洽性校验：所有终态计数之和应等于累计登记数，保证无结果丢失/重复结算。
	tracked := st.Succeeded + st.Failed + st.Canceled + st.Rejected + st.Aborted
	if tracked != st.Submitted {
		fmt.Fprintf(os.Stderr, "不一致: 终态合计=%d 但登记=%d\n", tracked, st.Submitted)
		os.Exit(2)
	}
	if st.Retrying != 0 {
		fmt.Fprintf(os.Stderr, "不一致: 关闭后仍有 %d 个任务处于重试等待\n", st.Retrying)
		os.Exit(2)
	}
	fmt.Println("一致性校验: 终态合计 == 登记数 ✓ 关闭后无残留重试等待 ✓")
}

func classify(c *counters, err error) {
	if err == nil {
		atomic.AddInt64(&c.ok, 1)
		return
	}
	var te *runner.TaskError
	if errors.As(err, &te) {
		switch te.Code {
		case runner.CodeQueueFull:
			atomic.AddInt64(&c.rejectedFull, 1)
		case runner.CodeQueueTimeout:
			atomic.AddInt64(&c.rejectedWait, 1)
		case runner.CodeShuttingDown:
			atomic.AddInt64(&c.shutting, 1)
		case runner.CodeIdempotentConflict:
			atomic.AddInt64(&c.conflict, 1)
		default:
			atomic.AddInt64(&c.other, 1)
		}
		return
	}
	atomic.AddInt64(&c.other, 1)
}
