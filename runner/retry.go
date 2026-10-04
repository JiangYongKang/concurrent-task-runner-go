package runner

import (
	"errors"
	"fmt"
	"time"
)

// BackoffKind 表示重试间隔的增长策略。
type BackoffKind int

const (
	// BackoffFixed 每次重试间隔固定为 Interval。
	BackoffFixed BackoffKind = iota
	// BackoffExponential 每次重试间隔按 2 倍逐步拉长（Interval*2^(已失败次数-1)），
	// MaxInterval>0 时封顶。
	BackoffExponential
)

// 重试策略的硬约束上限，防止“实质性无限重试”与失控的等待时间。
const (
	// MaxRetryAttempts 是 MaxAttempts 允许的最大值（含首次尝试）。
	MaxRetryAttempts = 32
	// MaxRetryInterval 是单次重试间隔允许的最大值。
	MaxRetryInterval = 5 * time.Minute
)

// RetryPolicy 描述任务失败后的自动重试策略。
// 零值（未指定）等价于 MaxAttempts=1，即不重试，行为与上一版完全一致。
type RetryPolicy struct {
	// MaxAttempts 总尝试次数上限（含首次），取值 [1, MaxRetryAttempts]。
	// 0 与 1 都表示只尝试一次、不重试。
	MaxAttempts int
	// Interval 重试基础间隔；MaxAttempts>1 时必须 > 0 且 <= MaxRetryInterval。
	Interval time.Duration
	// Backoff 间隔增长策略：BackoffFixed 固定 / BackoffExponential 逐步拉长。
	Backoff BackoffKind
	// MaxInterval 指数退避时的间隔上限；<=0 表示不封顶（仍受 MaxRetryInterval 约束）。
	MaxInterval time.Duration
}

// normalize 校验并归一化重试策略；nil 策略归一化为“不重试”。
func normalizeRetry(p *RetryPolicy) (*RetryPolicy, error) {
	if p == nil {
		return &RetryPolicy{MaxAttempts: 1}, nil
	}
	cp := *p
	if cp.MaxAttempts < 0 {
		return nil, newError("submit", CodeInvalidArgument,
			fmt.Sprintf("retry MaxAttempts must be >= 0, got %d", cp.MaxAttempts), nil)
	}
	if cp.MaxAttempts == 0 {
		cp.MaxAttempts = 1
	}
	if cp.MaxAttempts > MaxRetryAttempts {
		return nil, newError("submit", CodeInvalidArgument,
			fmt.Sprintf("retry MaxAttempts must be <= %d, got %d", MaxRetryAttempts, cp.MaxAttempts), nil)
	}
	if cp.Backoff != BackoffFixed && cp.Backoff != BackoffExponential {
		return nil, newError("submit", CodeInvalidArgument,
			fmt.Sprintf("unknown retry backoff kind: %d", cp.Backoff), nil)
	}
	if cp.MaxAttempts > 1 {
		if cp.Interval <= 0 {
			return nil, newError("submit", CodeInvalidArgument,
				"retry Interval must be > 0 when MaxAttempts > 1", nil)
		}
		if cp.Interval > MaxRetryInterval {
			return nil, newError("submit", CodeInvalidArgument,
				fmt.Sprintf("retry Interval must be <= %s, got %s", MaxRetryInterval, cp.Interval), nil)
		}
		if cp.MaxInterval < 0 {
			return nil, newError("submit", CodeInvalidArgument, "retry MaxInterval must be >= 0", nil)
		}
		if cp.MaxInterval > MaxRetryInterval {
			return nil, newError("submit", CodeInvalidArgument,
				fmt.Sprintf("retry MaxInterval must be <= %s, got %s", MaxRetryInterval, cp.MaxInterval), nil)
		}
		if cp.MaxInterval > 0 && cp.MaxInterval < cp.Interval {
			return nil, newError("submit", CodeInvalidArgument,
				"retry MaxInterval must be >= Interval", nil)
		}
	}
	return &cp, nil
}

// delay 计算第 failedAttempts 次失败之后、下一次尝试之前的等待时长（failedAttempts>=1）。
func (p *RetryPolicy) delay(failedAttempts int) time.Duration {
	d := p.Interval
	if p.Backoff == BackoffExponential {
		for i := 1; i < failedAttempts; i++ {
			d *= 2
			if p.MaxInterval > 0 && d >= p.MaxInterval {
				d = p.MaxInterval
				break
			}
			if d >= MaxRetryInterval {
				d = MaxRetryInterval
				break
			}
		}
	}
	if p.MaxInterval > 0 && d > p.MaxInterval {
		d = p.MaxInterval
	}
	if d > MaxRetryInterval {
		d = MaxRetryInterval
	}
	return d
}

// permanentError 包装“不值得重试”的业务错误（如参数/数据明显错误）。
type permanentError struct{ err error }

func (e *permanentError) Error() string { return e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

// NonRetryable 将 err 标记为永久性失败：带重试策略的任务返回该错误时不再重试，
// 直接进入 failed 终态。nil 输入原样返回 nil。
func NonRetryable(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// retryable 报告业务错误是否值得重试：默认值得重试（视为瞬时失败），
// 被 NonRetryable 包装的错误不值得重试。
func retryable(err error) bool {
	var pe *permanentError
	return !errors.As(err, &pe)
}
