package taskrunner

import "fmt"

// ErrorKind 以机器可判定的方式区分失败原因，使参数非法、幂等冲突、
// 任务不存在、过载、服务关闭、任务取消等情形可以被调用方明确区分。
type ErrorKind string

const (
	// KindInvalidArgument: 参数非法（空 key、nil 任务函数等）。
	KindInvalidArgument ErrorKind = "invalid_argument"
	// KindIdempotencyConflict: 幂等键复用但请求内容冲突。
	KindIdempotencyConflict ErrorKind = "idempotency_conflict"
	// KindNotFound: 任务不存在（或已被淘汰且不可查）。
	KindNotFound ErrorKind = "not_found"
	// KindOverloaded: 队列已满且在排队等待时限内未获得空位。
	KindOverloaded ErrorKind = "overloaded"
	// KindRunnerClosed: 服务关闭中或已关闭，不再接受新任务。
	KindRunnerClosed ErrorKind = "runner_closed"
	// KindTaskCanceled: 任务已被取消。
	KindTaskCanceled ErrorKind = "task_canceled"
)

// Error 是本包返回的带分类错误。
type Error struct {
	Kind  ErrorKind
	Msg   string
	Cause error
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	if e.Msg != "" {
		return string(e.Kind) + ": " + e.Msg
	}
	return string(e.Kind)
}

func (e *Error) Unwrap() error { return e.Cause }

// Is 使 errors.Is 可按 Kind 匹配对应哨兵错误，
// 同时保留带附加信息的错误实例与哨兵的等值匹配。
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	if !ok {
		return false
	}
	return e.Kind == t.Kind
}

// NewError 构造一个指定分类与附加信息的错误。
func NewError(kind ErrorKind, format string, args ...any) *Error {
	return &Error{Kind: kind, Msg: fmt.Sprintf(format, args...)}
}

// 哨兵错误，调用方可使用 errors.Is 判定失败原因。
var (
	ErrInvalidArgument     = &Error{Kind: KindInvalidArgument, Msg: "invalid argument"}
	ErrIdempotencyConflict = &Error{Kind: KindIdempotencyConflict, Msg: "idempotency key reused with a conflicting request"}
	ErrNotFound            = &Error{Kind: KindNotFound, Msg: "task not found"}
	ErrOverloaded          = &Error{Kind: KindOverloaded, Msg: "queue full and wait timeout elapsed"}
	ErrRunnerClosed        = &Error{Kind: KindRunnerClosed, Msg: "runner is closed"}
	ErrTaskCanceled        = &Error{Kind: KindTaskCanceled, Msg: "task was canceled"}
)

// KindOf 返回错误对应的分类；非本包错误返回空串。
func KindOf(err error) ErrorKind {
	if e, ok := err.(*Error); ok {
		return e.Kind
	}
	return ""
}
