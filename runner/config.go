package runner

import "time"

// Config 为 Runner 的有界并发、排队与关闭相关配置。
type Config struct {
	// MaxWorkers 最大并发执行数，必须 >= 1。
	MaxWorkers int
	// MaxQueue 最大排队长度：已登记但尚未开始执行的任务数上限，必须 >= 0。
	// 注意：提交先登记以保证幂等，登记后若无法在 MaxQueueWait 内进入队列，
	// 任务会被标记为 rejected，不占用执行资源。
	MaxQueue int
	// MaxQueueWait 队列满时等待空位的最长时间；<=0 表示不等待，立即拒绝。
	MaxQueueWait time.Duration
	// ShutdownGrace 关闭时在途任务的收敛宽限期；<=0 表示不等待，立即取消。
	ShutdownGrace time.Duration
}

// DefaultConfig 返回一组可用的默认配置：
// 8 个并发、1024 排队容量、队列满时最多等待 200ms、关闭宽限 5s。
func DefaultConfig() Config {
	return Config{
		MaxWorkers:    8,
		MaxQueue:      1024,
		MaxQueueWait:  200 * time.Millisecond,
		ShutdownGrace: 5 * time.Second,
	}
}

// Validate 校验配置并归一化，返回可直接使用的配置与错误。
func (c Config) Validate() (Config, error) {
	if c.MaxWorkers < 1 {
		return c, newError("config", CodeInvalidArgument, "MaxWorkers must be >= 1", nil)
	}
	if c.MaxQueue < 0 {
		return c, newError("config", CodeInvalidArgument, "MaxQueue must be >= 0", nil)
	}
	if c.MaxQueueWait < 0 {
		return c, newError("config", CodeInvalidArgument, "MaxQueueWait must be >= 0", nil)
	}
	if c.ShutdownGrace < 0 {
		return c, newError("config", CodeInvalidArgument, "ShutdownGrace must be >= 0", nil)
	}
	return c, nil
}
