package runner

// store 是幂等任务登记表（内存实现）。
// 它本身不加锁：所有访问都在 Runner 的单一互斥锁保护下进行，
// 以保证“登记—状态转移—结算”的原子性与可解释性。
type store struct {
	tasks map[string]*Task
}

func newStore() *store {
	return &store{tasks: make(map[string]*Task)}
}

// get 返回幂等键对应的任务（调用方持 Runner 锁）。
func (s *store) get(key string) (*Task, bool) {
	t, ok := s.tasks[key]
	return t, ok
}

// putIfAbsent 登记新任务；若键已存在则返回已有任务且 loaded=true。
func (s *store) putIfAbsent(key string, t *Task) (existing *Task, loaded bool) {
	if old, ok := s.tasks[key]; ok {
		return old, true
	}
	s.tasks[key] = t
	return t, false
}

// put 以新记录替换键下的旧记录（用于瞬时拒绝后的同内容重新提交）。
// 旧记录已终结且不再被引用，替换不影响其已重放给调用方的结果。
func (s *store) put(key string, t *Task) {
	s.tasks[key] = t
}

// all 返回当前所有任务快照列表（调用方持 Runner 锁）。
func (s *store) all() []*Task {
	out := make([]*Task, 0, len(s.tasks))
	for _, t := range s.tasks {
		out = append(out, t)
	}
	return out
}

func (s *store) len() int { return len(s.tasks) }
