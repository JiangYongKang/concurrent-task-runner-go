package taskrunner

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// blockFunc 返回一个可被门闩控制的任务函数：
// 任务在 gate 打开前阻塞，并监听 ctx 取消；started 用于观测是否真正执行。
// 走到 ctx 分支时返回 ctx.Err()；走到 gate 分支时返回 result。
type blockFunc struct {
	gate           chan struct{}
	started        atomic.Int64
	done           atomic.Int64
	cancelObserved atomic.Int64
	result         []byte
}

func newBlockFunc() *blockFunc {
	return &blockFunc{gate: make(chan struct{}), result: []byte("ok")}
}

func (b *blockFunc) fn() TaskFunc {
	return func(ctx context.Context, payload []byte) ([]byte, error) {
		b.started.Add(1)
		select {
		case <-b.gate:
		case <-ctx.Done():
			b.cancelObserved.Add(1)
			return nil, ctx.Err()
		}
		b.done.Add(1)
		return append([]byte(nil), b.result...), nil
	}
}

// lateResultFunc 无论 ctx 是否取消，都在 gate 放行后才返回成功结果，
// 用于验证“取消后迟到的成功结果必须被丢弃”。
type lateResultFunc struct {
	gate     chan struct{}
	returned chan struct{}
	started  atomic.Int64
}

func newLateResultFunc() *lateResultFunc {
	return &lateResultFunc{gate: make(chan struct{}), returned: make(chan struct{})}
}

func (b *lateResultFunc) fn() TaskFunc {
	return func(ctx context.Context, payload []byte) ([]byte, error) {
		b.started.Add(1)
		defer close(b.returned)
		<-b.gate // 故意不监听 ctx，模拟不协作的任务体
		return []byte("late"), nil
	}
}

// waitForStatus 轮询直到任务达到指定状态或超时。
func waitForStatus(t *testing.T, r *Runner, key string, want Status, timeout time.Duration) *TaskInfo {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var info *TaskInfo
	for time.Now().Before(deadline) {
		got, err := r.Get(context.Background(), key)
		if err != nil {
			t.Fatalf("Get(%q) unexpected error: %v", key, err)
		}
		info = got
		if got.Status == want {
			return got
		}
		time.Sleep(time.Millisecond * 2)
	}
	t.Fatalf("task %q did not reach %s within %s, last=%s", key, want, timeout, info.Status)
	return nil
}

func waitForTrue(timeout time.Duration, f func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if f() {
			return true
		}
		time.Sleep(time.Millisecond * 2)
	}
	return false
}

// logCase 打印测试输入与判定依据，满足“单测日志中需要打印输入与判定依据”。
func logCase(t *testing.T, name string, input any, verdict string, args ...any) {
	t.Helper()
	msg := fmt.Sprintf(verdict, args...)
	t.Logf("[CASE] %s | input=%#v | verdict=%s", name, input, msg)
}

// onceFunc 统计某任务函数被实际执行的次数（用于验证不重复执行）。
func onceFunc(counter *atomic.Int64, result []byte) TaskFunc {
	return func(ctx context.Context, payload []byte) ([]byte, error) {
		counter.Add(1)
		return append([]byte(nil), result...), nil
	}
}

var _ = sync.Mutex{}
