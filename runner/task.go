package runner

import (
	"context"
	"time"
)

// Status 表示任务生命周期中的状态。
type Status string

const (
	StatusQueued    Status = "queued"    // 已登记，等待执行
	StatusRunning   Status = "running"   // 正在执行
	StatusSucceeded Status = "succeeded" // 执行成功（终态）
	StatusFailed    Status = "failed"    // 业务函数返回错误（终态）
	StatusCanceled  Status = "canceled"  // 被用户取消或关闭时取消（终态）
	StatusRejected  Status = "rejected"  // 因过载/关闭被拒绝（终态）
	StatusAborted   Status = "aborted"   // 关闭宽限期后仍未结束（终态）
)

// IsTerminal 报告状态是否为终态。终态任务的结果与状态不再变化。
func (s Status) IsTerminal() bool {
	switch s {
	case StatusSucceeded, StatusFailed, StatusCanceled, StatusRejected, StatusAborted:
		return true
	default:
		return false
	}
}

// Func 是用户任务函数。实现应监听 ctx.Done() 以便及时响应取消与关闭。
type Func func(ctx context.Context) (any, error)

// Payload 是提交内容，参与幂等冲突判定：
// 同一幂等键复用但 Name 或 Params 不同，视为冲突提交。
type Payload struct {
	Name   string
	Params any
}

// TaskState 是任务状态的可观测快照（值拷贝，可安全持有）。
type TaskState struct {
	ID          string
	IdemKey     string
	Status      Status
	SubmittedAt time.Time
	StartedAt   time.Time
	FinishedAt  time.Time
	Result      any
	Err         *TaskError
	Attempts    int
}

// Task 是内部任务记录。所有字段均由 Runner 的单一互斥锁保护，
// done 在任务进入终态时关闭，用于唤醒等待幂等结果的提交者。
type Task struct {
	idemKey     string
	fingerprint string // 提交内容指纹，用于冲突判定
	fn          Func
	state       TaskState

	cancel context.CancelFunc // 运行期取消函数；进入 running 前为 nil
	done   chan struct{}      // 进入终态时关闭
}

func newTask(idemKey, fingerprint string, p Payload, fn Func) *Task {
	t := &Task{
		idemKey:     idemKey,
		fingerprint: fingerprint,
		fn:          fn,
		done:        make(chan struct{}),
	}
	t.state.ID = idemKey
	t.state.IdemKey = idemKey
	t.state.Status = StatusQueued
	t.state.SubmittedAt = nowFunc()
	return t
}

// snapshot 在持锁状态下返回状态值拷贝。
func (t *Task) snapshot() TaskState {
	s := t.state
	return s
}
