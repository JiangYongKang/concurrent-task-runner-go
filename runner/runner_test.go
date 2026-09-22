package runner

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// judge 打印测试输入与判定依据，满足“单测日志中打印输入与判定依据”的要求。
func judge(t *testing.T, scene string, input, got, want any) {
	t.Helper()
	t.Logf("[判定] scene=%s input=%+v got=%+v want=%+v", scene, input, got, want)
}

func newTestRunner(t *testing.T, cfg Config) *Runner {
	t.Helper()
	r, err := New(cfg)
	if err != nil {
		t.Fatalf("New 返回错误: %v", err)
	}
	t.Cleanup(func() { _ = r.Shutdown(context.Background()) })
	return r
}

func quickFunc(v any) Func {
	return func(ctx context.Context) (any, error) { return v, nil }
}

func TestConfigValidate(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config
		want Code
		ok   bool
	}{
		{"zero workers", Config{MaxWorkers: 0, MaxQueue: 1}, CodeInvalidArgument, false},
		{"negative queue", Config{MaxWorkers: 1, MaxQueue: -1}, CodeInvalidArgument, false},
		{"valid", Config{MaxWorkers: 1, MaxQueue: 0}, "", true},
	}
	for _, c := range cases {
		_, err := c.cfg.Validate()
		got := err == nil
		judge(t, "config:"+c.name, c.cfg, got, c.ok)
		if got != c.ok {
			t.Fatalf("%s: 期望 ok=%v, 实际 err=%v", c.name, c.ok, err)
		}
		if !c.ok {
			var te *TaskError
			if !errors.As(err, &te) || te.Code != c.want {
				t.Fatalf("%s: 期望 code=%s, 实际 %v", c.name, c.want, err)
			}
		}
	}
}

func TestSubmitInvalidArgument(t *testing.T) {
	r := newTestRunner(t, Config{MaxWorkers: 2, MaxQueue: 4})
	ctx := context.Background()

	if _, err := r.Submit(ctx, "", Payload{Name: "n"}, quickFunc(1)); err == nil {
		t.Fatal("空幂等键应被拒绝")
	} else {
		judge(t, "empty-key", "idemKey=''", errCode(err), CodeInvalidArgument)
		if errCode(err) != CodeInvalidArgument {
			t.Fatalf("期望 %s, 实际 %v", CodeInvalidArgument, err)
		}
	}
	if _, err := r.Submit(ctx, "k1", Payload{Name: "n"}, nil); err == nil {
		t.Fatal("nil fn 应被拒绝")
	} else {
		judge(t, "nil-fn", "fn=nil", errCode(err), CodeInvalidArgument)
		if errCode(err) != CodeInvalidArgument {
			t.Fatalf("期望 %s, 实际 %v", CodeInvalidArgument, err)
		}
	}
}

func TestGetNotFound(t *testing.T) {
	r := newTestRunner(t, Config{MaxWorkers: 1, MaxQueue: 1})
	if _, ok := r.Get(context.Background(), "missing"); ok {
		t.Fatal("不存在的任务应返回 ok=false")
	}
	judge(t, "get-not-found", "idemKey=missing", false, false)

	if _, err := r.Cancel(context.Background(), "missing", "x"); errCode(err) != CodeNotFound {
		t.Fatalf("取消不存在任务应返回 not_found，实际 %v", err)
	}
	judge(t, "cancel-not-found", "idemKey=missing", errCode(errOrDummy(r, "missing")), CodeNotFound)
}

func errOrDummy(r *Runner, key string) error {
	_, err := r.Cancel(context.Background(), key, "x")
	return err
}

func errCode(err error) Code {
	var te *TaskError
	if errors.As(err, &te) {
		return te.Code
	}
	return ""
}

func TestSubmitSuccessAndReplay(t *testing.T) {
	var runs int32
	r := newTestRunner(t, Config{MaxWorkers: 2, MaxQueue: 2})
	ctx := context.Background()
	fn := func(ctx context.Context) (any, error) {
		atomic.AddInt32(&runs, 1)
		return "ok", nil
	}

	s1, err := r.Submit(ctx, "job-1", Payload{Name: "add", Params: map[string]int{"x": 1}}, fn)
	if err != nil {
		t.Fatalf("首次提交失败: %v", err)
	}
	waitTerminal(t, r, "job-1")

	// 相同键 + 相同内容：重放，不重复执行。
	s2, err := r.Submit(ctx, "job-1", Payload{Name: "add", Params: map[string]int{"x": 1}}, fn)
	if err != nil || s2.Status != StatusSucceeded || s2.Result != "ok" {
		t.Fatalf("重放应返回成功结果, state=%+v err=%v", s2, err)
	}
	judge(t, "idempotent-replay", "同键同内容第二次提交",
		fmt.Sprintf("runs=%d status=%s result=%v", atomic.LoadInt32(&runs), s2.Status, s2.Result),
		"runs=1 status=succeeded result=ok")
	if atomic.LoadInt32(&runs) != 1 {
		t.Fatalf("重复提交不得触发重复执行, runs=%d", runs)
	}
	_ = s1
}

func TestSubmitIdempotentConflict(t *testing.T) {
	r := newTestRunner(t, Config{MaxWorkers: 2, MaxQueue: 4})
	ctx := context.Background()

	if _, err := r.Submit(ctx, "job-2", Payload{Name: "add", Params: 1}, quickFunc(1)); err != nil {
		t.Fatal(err)
	}
	// 同键不同内容：必须被拒绝，且失败原因与非法参数/不存在相区分。
	_, err := r.Submit(ctx, "job-2", Payload{Name: "add", Params: 2}, quickFunc(1))
	judge(t, "idempotent-conflict", "同键 Params 1->2", errCode(err), CodeIdempotentConflict)
	if errCode(err) != CodeIdempotentConflict {
		t.Fatalf("期望 idempotent_conflict, 实际 %v", err)
	}
	if !errors.Is(err, ErrIdempotentConflict) {
		t.Fatalf("应可通过 errors.Is 识别 ErrIdempotentConflict, 实际 %v", err)
	}

	// 不同 Name 也构成冲突。
	_, err = r.Submit(ctx, "job-2", Payload{Name: "sub", Params: 1}, quickFunc(1))
	if errCode(err) != CodeIdempotentConflict {
		t.Fatalf("Name 不同也应冲突, 实际 %v", err)
	}
	judge(t, "idempotent-conflict-name", "同键 Name add->sub", errCode(err), CodeIdempotentConflict)
}

// waitTerminal 轮询直到任务进入终态或超时。
func waitTerminal(t *testing.T, r *Runner, key string) TaskState {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if s, ok := r.Get(context.Background(), key); ok && s.Status.IsTerminal() {
			return *s
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("任务 %s 未在超时内终结", key)
	return TaskState{}
}

// 防止编译器在文件演进中抱怨未使用的导入。
var _ = sync.Mutex{}
var _ context.Context
