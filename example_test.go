package taskrunner

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Example 演示提交、幂等重放、冲突拒绝、过载拒绝、查询与优雅关闭的完整流程。
func Example() {
	ctx := context.Background()
	r, err := New(Config{
		Workers:          2,
		QueueSize:        1,
		QueueWaitTimeout: 20 * time.Millisecond,
		ShutdownGrace:    time.Second,
	})
	if err != nil {
		panic(err)
	}

	// 一个可被 ctx 取消控制的慢任务，用于占满容量演示过载。
	gate := make(chan struct{})
	slow := func(ctx context.Context, p []byte) ([]byte, error) {
		select {
		case <-gate:
			return p, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	quick := func(ctx context.Context, p []byte) ([]byte, error) { return p, nil }

	// 占满 2 worker + 1 队列 = 3 个容量槽。
	_, _ = r.Submit(ctx, SubmitRequest{IdempotencyKey: "s1", Payload: []byte("s1"), Func: slow})
	_, _ = r.Submit(ctx, SubmitRequest{IdempotencyKey: "s2", Payload: []byte("s2"), Func: slow})
	_, _ = r.Submit(ctx, SubmitRequest{IdempotencyKey: "s3", Payload: []byte("s3"), Func: slow})

	// 第 4 个新任务：容量满，等待超时后被明确拒绝。
	_, err = r.Submit(ctx, SubmitRequest{IdempotencyKey: "overflow", Func: quick})
	fmt.Println("overflow:", KindOf(err))

	// 同键同载荷：幂等重放，即使在容量满时也不占用资源。
	replay, err := r.Submit(ctx, SubmitRequest{IdempotencyKey: "s1", Payload: []byte("s1"), Func: slow})
	fmt.Println("replay err:", err, "replayed:", replay.Replay)

	// 同键不同载荷：冲突，原因可与非法参数/不存在区分。
	_, err = r.Submit(ctx, SubmitRequest{IdempotencyKey: "s1", Payload: []byte("different"), Func: quick})
	fmt.Println("conflict:", errors.Is(err, ErrIdempotencyConflict))

	// 参数非法与任务不存在。
	_, err = r.Submit(ctx, SubmitRequest{IdempotencyKey: "", Func: quick})
	fmt.Println("invalid:", KindOf(err))
	err = r.Cancel(ctx, "ghost")
	fmt.Println("notfound:", KindOf(err))

	// 取消排队任务（绝不执行）。
	_ = r.Cancel(ctx, "s3")
	info, _ := r.Get(ctx, "s3")
	fmt.Println("s3:", info.Status)

	// 放行在途任务后优雅关闭。
	close(gate)
	if err := r.Shutdown(ctx); err != nil {
		fmt.Println("shutdown:", KindOf(err))
	} else {
		fmt.Println("shutdown: ok")
	}
	// Output:
	// overflow: overloaded
	// replay err: <nil> replayed: true
	// conflict: true
	// invalid: invalid_argument
	// notfound: not_found
	// s3: CANCELLED
	// shutdown: ok
}
