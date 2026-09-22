package taskrunner

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestOverloadImmediateReject: Workers=1 QueueSize=0 QueueWaitTimeout=0，
// 在途任务占满唯一容量后，新任务必须立即收到 ErrOverloaded，
// 且被拒绝任务不留记录、不影响在途任务。
func TestOverloadImmediateReject(t *testing.T) {
	cfg := Config{Workers: 1, QueueSize: 0, QueueWaitTimeout: 0, ShutdownGrace: 2 * time.Second}
	r := newTestRunner(t, cfg)

	bf := newBlockFunc()
	r1 := SubmitRequest{IdempotencyKey: "occupy", Payload: []byte("p"), Func: bf.fn()}
	if _, err := r.Submit(context.Background(), r1); err != nil {
		t.Fatalf("first submit: %v", err)
	}
	if !waitForTrue(time.Second, func() bool { return bf.started.Load() == 1 }) {
		t.Fatal("first task never started")
	}

	before, _ := r.Get(context.Background(), "occupy")
	_, err := r.Submit(context.Background(),
		SubmitRequest{IdempotencyKey: "rejected", Payload: []byte("p2"), Func: bf.fn()})
	logCase(t, "OverloadImmediate",
		map[string]any{"workers": 1, "queue": 0, "waitTimeout": 0, "inflight": before.Status},
		"second submit err=%v kind=%s", err, KindOf(err))
	if !errors.Is(err, ErrOverloaded) {
		t.Fatalf("expected ErrOverloaded, got %v", err)
	}

	// 被拒绝的任务不得登记（Get 应 NotFound），也未执行。
	if _, err := r.Get(context.Background(), "rejected"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("rejected task must not be registered, got err=%v", err)
	}

	// 被拒绝任务不占用资源：释放门闩后在途任务正常成功，结果不丢。
	close(bf.gate)
	info := waitForStatus(t, r, "occupy", StatusSucceeded, 2*time.Second)
	logCase(t, "OverloadImmediate", "gate released",
		"occupier final=%s result=%q stats=%+v", info.Status, info.Result, r.Stats())
	if string(info.Result) != "ok" {
		t.Fatalf("occupier result lost or changed: %q", info.Result)
	}
	if r.Stats().RejectedTotal != 1 {
		t.Fatalf("expected 1 rejection counted, got %d", r.Stats().RejectedTotal)
	}
}

// TestOverloadQueueWaitTimeout: 有界排队容量耗尽后，等待超过 QueueWaitTimeout
// 的提交被拒绝；排队中的已有任务状态不受影响。
func TestOverloadQueueWaitTimeout(t *testing.T) {
	cfg := Config{Workers: 1, QueueSize: 1, QueueWaitTimeout: 60 * time.Millisecond, ShutdownGrace: 2 * time.Second}
	r := newTestRunner(t, cfg)

	bf := newBlockFunc()
	// 1 个运行中 + 1 个排队，占满 capacity=2。
	if _, err := r.Submit(context.Background(), SubmitRequest{IdempotencyKey: "running", Func: bf.fn()}); err != nil {
		t.Fatal(err)
	}
	if !waitForTrue(time.Second, func() bool { return bf.started.Load() == 1 }) {
		t.Fatal("first task never started")
	}
	if _, err := r.Submit(context.Background(), SubmitRequest{IdempotencyKey: "queued", Func: bf.fn()}); err != nil {
		t.Fatalf("queued submit should be accepted: %v", err)
	}
	if !waitForTrue(time.Second, func() bool { return r.Stats().Queued == 1 }) {
		t.Fatalf("expected 1 queued, stats=%+v", r.Stats())
	}

	start := time.Now()
	_, err := r.Submit(context.Background(), SubmitRequest{IdempotencyKey: "overflow", Func: bf.fn()})
	waited := time.Since(start)
	logCase(t, "OverloadWaitTimeout",
		map[string]any{"capacity": 2, "queueFull": true, "waitTimeout": cfg.QueueWaitTimeout},
		"third submit waited=%s err=%v kind=%s", waited, err, KindOf(err))
	if !errors.Is(err, ErrOverloaded) {
		t.Fatalf("expected ErrOverloaded after wait, got %v", err)
	}
	if waited < 50*time.Millisecond {
		t.Fatalf("should have waited ~timeout, got %s", waited)
	}

	// 释放后两个任务先后成功；overflow 始终不存在。
	close(bf.gate)
	q1 := waitForStatus(t, r, "running", StatusSucceeded, 2*time.Second)
	q2 := waitForStatus(t, r, "queued", StatusSucceeded, 2*time.Second)
	if q1.Status != StatusSucceeded || q2.Status != StatusSucceeded {
		t.Fatalf("existing tasks must complete normally: %s,%s", q1.Status, q2.Status)
	}
	if _, err := r.Get(context.Background(), "overflow"); !errors.Is(err, ErrNotFound) {
		t.Fatal("overflow task must not be registered")
	}
}
