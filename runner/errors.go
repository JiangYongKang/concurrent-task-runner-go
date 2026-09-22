package runner

import (
	"errors"
	"fmt"
)

// Code 表示可区分的失败原因，调用方可以按 Code 分支处理。
type Code string

const (
	CodeInvalidArgument    Code = "invalid_argument"    // 参数非法
	CodeIdempotentConflict Code = "idempotent_conflict" // 幂等键复用但载荷冲突
	CodeNotFound           Code = "not_found"           // 任务不存在
	CodeQueueFull          Code = "queue_full"          // 排队队列已满
	CodeQueueTimeout       Code = "queue_timeout"       // 队列满且等待超时
	CodeShuttingDown       Code = "shutting_down"       // 服务关闭中，拒绝新提交
	CodeTaskCanceled       Code = "task_canceled"       // 任务已被取消（等待结果时）
	CodeTerminal           Code = "terminal"            // 任务已终结，无法取消
	CodeAborted            Code = "aborted"             // 宽限期结束仍未结束
)

// TaskError 携带可区分的失败原因，便于调用方判定与日志检索。
type TaskError struct {
	Op   string // 发生错误的操作，如 "submit"、"cancel"
	Code Code   // 机器可判定的失败分类
	Msg  string // 人类可读描述
	Err  error  // 被包装的底层错误
}

func (e *TaskError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("runner: %s: %s: %s: %v", e.Op, e.Code, e.Msg, e.Err)
	}
	return fmt.Sprintf("runner: %s: %s: %s", e.Op, e.Code, e.Msg)
}

func (e *TaskError) Unwrap() error { return e.Err }

// 哨兵错误，便于 errors.Is 判定。
var (
	ErrInvalidArgument    = errors.New("runner: invalid argument")
	ErrIdempotentConflict = errors.New("runner: idempotency key reused with different payload")
	ErrNotFound           = errors.New("runner: task not found")
	ErrQueueFull          = errors.New("runner: queue full")
	ErrQueueTimeout       = errors.New("runner: timed out waiting for queue slot")
	ErrShuttingDown       = errors.New("runner is shutting down")
	ErrTaskCanceled       = errors.New("runner: task canceled")
	ErrTerminal           = errors.New("runner: task already in terminal state")
	ErrAborted            = errors.New("runner: task aborted after shutdown grace")
)

func newError(op string, code Code, msg string, cause error) *TaskError {
	var sentinel error
	switch code {
	case CodeInvalidArgument:
		sentinel = ErrInvalidArgument
	case CodeIdempotentConflict:
		sentinel = ErrIdempotentConflict
	case CodeNotFound:
		sentinel = ErrNotFound
	case CodeQueueFull:
		sentinel = ErrQueueFull
	case CodeQueueTimeout:
		sentinel = ErrQueueTimeout
	case CodeShuttingDown:
		sentinel = ErrShuttingDown
	case CodeTaskCanceled:
		sentinel = ErrTaskCanceled
	case CodeTerminal:
		sentinel = ErrTerminal
	case CodeAborted:
		sentinel = ErrAborted
	default:
		sentinel = cause
	}
	return &TaskError{Op: op, Code: code, Msg: msg, Err: errors.Join(sentinel, cause)}
}
