package taskrunner

import "time"

// Config 描述 Runner 的并发、排队与关闭参数。零值字段由 DefaultConfig 补齐。
type Config struct {
	// Workers 是最大并发执行数（>0）。
	Workers int
	// QueueSize 是等待执行的排队容量（>=0）。
	// 在途任务上限 = Workers + QueueSize。
	QueueSize int
	// QueueWaitTimeout 是队列已满时，提交方最多等待出现空位的时间。
	// 0 表示立即拒绝；>0 期间内有空位则继续提交，超时返回 ErrOverloaded。
	QueueWaitTimeout time.Duration
	// ShutdownGrace 是 Shutdown 等待在途/排队任务自然收敛的宽限期。
	// 0 表示不等待；<0 表示无限等待直到全部完成。
	ShutdownGrace time.Duration
}

// DefaultConfig 返回一组可直接使用的默认配置：
// 4 个 worker、128 排队槽、提交等待 200ms、关闭宽限 5s。
func DefaultConfig() Config {
	return Config{
		Workers:          4,
		QueueSize:        128,
		QueueWaitTimeout: 200 * time.Millisecond,
		ShutdownGrace:    5 * time.Second,
	}
}

// Validate 校验配置，非法时返回 ErrInvalidArgument。
func (c Config) Validate() error {
	switch {
	case c.Workers <= 0:
		return NewError(KindInvalidArgument, "Workers must be > 0, got %d", c.Workers)
	case c.QueueSize < 0:
		return NewError(KindInvalidArgument, "QueueSize must be >= 0, got %d", c.QueueSize)
	case c.QueueWaitTimeout < 0:
		return NewError(KindInvalidArgument, "QueueWaitTimeout must be >= 0")
	}
	return nil
}
