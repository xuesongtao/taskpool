package taskpool

type Task struct {
	f taskFunc
}

func NewTask(fn taskFunc) *Task {
	obj := &Task{}
	obj.f = fn
	return obj
}
