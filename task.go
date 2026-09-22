package taskrunner

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sync"
	"time"
)

// Status 是任务对外可观察的状态。
type Status int

const (
	StatusPending    Status = iota // 已入队，等待执行
	StatusRunning                  // 正在执行
	StatusSucceeded                // 成功完成，结果可读
	StatusFailed                   // 执行返回错误，错误信息可读
	StatusCancelled                // 已被取消，不会产生完成结果
	StatusUnfinished               // 关闭宽限期结束仍未结束
)

func (s Status) String() string {
	switch s {
	case StatusPending:
		return "PENDING"
	case StatusRunning:
		return "RUNNING"
	case StatusSucceeded:
		return "SUCCEEDED"
	case StatusFailed:
		return "FAILED"
	case StatusCancelled:
		return "CANCELLED"
	case StatusUnfinished:
		return "UNFINISHED"
	default:
		return "UNKNOWN"
	}
}

// IsTerminal 报告状态是否为终态（终态不可逆，且全生命周期只结算一次）。
func (s Status) IsTerminal() bool {
	switch s {
	case StatusSucceeded, StatusFailed, StatusCancelled, StatusUnfinished:
		return true
	}
	return false
}

// TaskFunc 是任务的执行体。ctx 在任务被取消或服务关闭时被取消，
// 执行体应当通过监听 ctx.Done() 协作式提前退出。
type TaskFunc func(ctx context.Context, payload []byte) ([]byte, error)

// SubmitRequest 是提交请求。
type SubmitRequest struct {
	// IdempotencyKey 由调用方提供，为空会被拒绝。
	IdempotencyKey string
	// Payload 参与幂等冲突判定（SHA-256 摘要比较）。
	Payload []byte
	// Func 为任务执行体，nil 会被拒绝。
	Func TaskFunc
}

// TaskInfo 是任务状态与结果的只读快照（切片均为拷贝）。
type TaskInfo struct {
	ID          string
	Key         string
	Status      Status
	PayloadHash string
	Result      []byte
	ErrMessage  string
	Attempts    int
	SubmittedAt time.Time
	StartedAt   time.Time
	FinishedAt  time.Time
}

// SubmitResult 是提交结果；Replay 为 true 表示幂等重放，未触发执行。
type SubmitResult struct {
	Replay bool
	Task   *TaskInfo
}

// taskEntry 是任务的内部记录。
type taskEntry struct {
	id          string
	key         string
	payloadHash string
	payload     []byte
	fn          TaskFunc

	mu   sync.Mutex
	info TaskInfo

	// runCtx 在取消/关闭时取消；cancel 在终态后调用，防止上下文泄漏。
	runCtx context.Context
	cancel context.CancelFunc

	// done 在 worker 侧彻底处理完该任务（执行完毕或跳过）后关闭一次。
	done chan struct{}
}

func newTaskEntry(req SubmitRequest) (*taskEntry, error) {
	id, err := randomID()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	now := time.Now()
	payload := append([]byte(nil), req.Payload...)
	e := &taskEntry{
		id:          id,
		key:         req.IdempotencyKey,
		payloadHash: hashPayload(req.Payload),
		payload:     payload,
		fn:          req.Func,
		runCtx:      ctx,
		cancel:      cancel,
		done:        make(chan struct{}),
	}
	e.info = TaskInfo{
		ID:          id,
		Key:         req.IdempotencyKey,
		Status:      StatusPending,
		PayloadHash: e.payloadHash,
		SubmittedAt: now,
	}
	return e, nil
}

// snapshot 返回当前信息的加锁拷贝。
func (e *taskEntry) snapshot() *TaskInfo {
	e.mu.Lock()
	defer e.mu.Unlock()
	cp := e.info
	if e.info.Result != nil {
		cp.Result = append([]byte(nil), e.info.Result...)
	}
	return &cp
}

// beginRunning 由 worker 调用：仅 Pending -> Running 成功。
func (e *taskEntry) beginRunning() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.info.Status != StatusPending {
		return false
	}
	e.info.Status = StatusRunning
	e.info.Attempts = 1
	e.info.StartedAt = time.Now()
	return true
}

// settleResult 由 worker 正常执行完毕后调用：仅 Running -> Succeeded/Failed。
// 若任务已被取消/标记未完成，则结果被丢弃，返回 false。
func (e *taskEntry) settleResult(result []byte, err error) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.info.Status != StatusRunning {
		return false
	}
	if err != nil {
		e.info.Status = StatusFailed
		e.info.ErrMessage = err.Error()
	} else {
		e.info.Status = StatusSucceeded
		e.info.Result = append([]byte(nil), result...)
	}
	e.info.FinishedAt = time.Now()
	return true
}

// requestCancel 由取消接口调用：Pending/Running -> Cancelled。
// 已是终态时为幂等 no-op；返回状态是否发生了迁移。
func (e *taskEntry) requestCancel() (changed bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.info.Status.IsTerminal() {
		return false
	}
	e.info.Status = StatusCancelled
	e.info.FinishedAt = time.Now()
	e.cancel()
	return true
}

// markUnfinished 由关闭宽限期到期路径调用：Pending/Running -> Unfinished。
// 已是终态（含已取消）时不会覆盖，保证不重复结算。
func (e *taskEntry) markUnfinished() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.info.Status.IsTerminal() {
		return false
	}
	e.info.Status = StatusUnfinished
	e.info.FinishedAt = time.Now()
	e.cancel()
	return true
}

func (e *taskEntry) status() Status {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.info.Status
}

// finishOnce 关闭 done，恰好一次，由 worker 处理完任务时调用。
func (e *taskEntry) finishOnce() {
	// 仅在未关闭时关闭：用 select 非阻塞探测。
	select {
	case <-e.done:
	default:
		close(e.done)
	}
}

func hashPayload(p []byte) string {
	sum := sha256.Sum256(p)
	return hex.EncodeToString(sum[:])
}

func randomID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// 确保编译期引用 errors（任务执行错误包装时复用）。
var _ = errors.Is
