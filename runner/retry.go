package runner

import (
	"errors"
	"time"
)

// BackoffKind 表示重试间隔的增长方式。
type BackoffKind string

const (
	// BackoffFixed 每次重试间隔固定为 Interval。
	BackoffFixed BackoffKind = "fixed"
	// BackoffExponential 间隔逐次翻倍（Interval, 2*Interval, 4*Interval, ...），
	// 封顶 MaxInterval。
	BackoffExponential BackoffKind = "exponential"
)

const (
	// MaxRetryAttempts 是 MaxAttempts 的硬上限，防止实质性无限重试。
	MaxRetryAttempts = 16
	// DefaultMaxRetryInterval 是未显式设置 MaxInterval 时指数退避的默认封顶。
	DefaultMaxRetryInterval = 30 * time.Second
)

// RetryPolicy 描述任务失败后的自动重试策略。
// 零值等价于“不重试”（MaxAttempts=1），与不指定策略的 Submit 行为一致。
type RetryPolicy struct {
	// MaxAttempts 总尝试次数上限（含首次执行），取值 [1, MaxRetryAttempts]；
	// 0 归一化为 1，即不重试。
	MaxAttempts int
	// Interval 第一次重试前的等待间隔，必须 >= 0；指数退避时作为基数。
	Interval time.Duration
	// Backoff 间隔增长方式：BackoffFixed（默认）或 BackoffExponential。
	Backoff BackoffKind
	// MaxInterval 指数退避的间隔封顶；<=0 时使用 DefaultMaxRetryInterval。
	MaxInterval time.Duration
}

// normalize 校验并归一化策略；非法取值返回 CodeInvalidArgument。
func (p RetryPolicy) normalize() (RetryPolicy, error) {
	if p.MaxAttempts < 0 {
		return p, newError("retry", CodeInvalidArgument, "MaxAttempts must be >= 0", nil)
	}
	if p.MaxAttempts > MaxRetryAttempts {
		return p, newError("retry", CodeInvalidArgument,
			"MaxAttempts exceeds hard limit", nil)
	}
	if p.MaxAttempts == 0 {
		p.MaxAttempts = 1
	}
	if p.Interval < 0 {
		return p, newError("retry", CodeInvalidArgument, "Interval must be >= 0", nil)
	}
	if p.Backoff == "" {
		p.Backoff = BackoffFixed
	}
	if p.Backoff != BackoffFixed && p.Backoff != BackoffExponential {
		return p, newError("retry", CodeInvalidArgument, "unknown Backoff kind", nil)
	}
	if p.MaxInterval < 0 {
		return p, newError("retry", CodeInvalidArgument, "MaxInterval must be >= 0", nil)
	}
	if p.MaxInterval == 0 {
		p.MaxInterval = DefaultMaxRetryInterval
	}
	if p.Interval > p.MaxInterval {
		p.Interval = p.MaxInterval
	}
	return p, nil
}

// enabled 报告策略是否会在失败后再尝试。
func (p RetryPolicy) enabled() bool { return p.MaxAttempts > 1 }

// delay 返回第 attempt 次尝试（从 1 计）失败后的等待时长。
func (p RetryPolicy) delay(attempt int) time.Duration {
	d := p.Interval
	if p.Backoff == BackoffExponential {
		for i := 1; i < attempt; i++ {
			if d >= p.MaxInterval {
				return p.MaxInterval
			}
			d *= 2
		}
		if d > p.MaxInterval {
			d = p.MaxInterval
		}
	}
	return d
}

// retryableError 标记“值得重试”的业务失败。
type retryableError struct{ err error }

func (e retryableError) Error() string { return e.err.Error() }
func (e retryableError) Unwrap() error { return e.err }

// Retryable 把 err 标记为可重试失败：任务函数返回该错误时，
// 若重试策略仍有剩余次数，将安排下一轮尝试；否则与不可重试失败一样进入 failed。
// 传入 nil 时返回 nil。
func Retryable(err error) error {
	if err == nil {
		return nil
	}
	return retryableError{err}
}

// IsRetryable 报告 err 是否被标记为可重试（含包装链）。
func IsRetryable(err error) bool {
	var re retryableError
	return errors.As(err, &re)
}
