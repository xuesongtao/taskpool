package taskpool

import (
	"context"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

type (
	sec            = int64
	logLevel       int
	taskFunc       func()
	TaskPoolOption func(p *TaskPool)
)

const (
	// 默认
	defaultIsPre              bool          = false        // 是否预先分配协程
	defaultPolDuration        time.Duration = time.Second  // 哨兵默认轮询时间
	defaultWorkerMaxLifeCycle sec           = 10 * sec(60) // worker 最大存活期(单位: 秒)

	// 状态
	closed int32 = 1 // 任务池是否关闭

	// 日志
	levelInfo logLevel = iota
	levelError
)

// 设置日志 log
func WithPoolLogger(logger cjLogger) TaskPoolOption {
	return func(p *TaskPool) {
		p.log = logger
	}
}

// 设置 taskPool 中哨兵轮询地时间
func WithPolTime(t time.Duration) TaskPoolOption {
	return func(p *TaskPool) {
		p.polTime = t
	}
}

// 设置 taskPool 中空闲的 worker 存活的时间
func WithWorkerMaxLifeCycle(timeForSec sec) TaskPoolOption {
	return func(p *TaskPool) {
		p.workerMaxLifeCycle = timeForSec
	}
}

// 预分配协程
func WithProGoWorker() TaskPoolOption {
	return func(p *TaskPool) {
		p.isPre = true
	}
}

type worker struct {
	ctx       context.Context // 用于传递关闭信号
	startTime int64           // 记录开始的时间
	taskCh    chan taskFunc
}

func newWorker(ctx context.Context) *worker {
	return &worker{
		ctx:       ctx,
		startTime: time.Now().Unix(),
		taskCh:    make(chan taskFunc),
	}
}

func (w *worker) goWorker(pool *TaskPool) {
	go func() {
		defer func() {
			atomic.AddInt32(&pool.running, -1)
			// 放会池中
			pool.workerCache.Put(w)
			if err := recover(); err != nil {
				pool.printStackInfo("goWorker", err)
			}
		}()
		for {
			select {
			case f := <-w.taskCh:
				if f == nil {
					pool.print(levelInfo, "exit goWorker")
					return
				}
				f()

				// 放入 freeWorkerQueue 为了后面复用
				if isGiveUp := pool.freeWorkerQueueAppend(w); isGiveUp {
					return
				}

				// 通知阻塞的去取
				pool.cond.Signal()
			case <-w.ctx.Done():
				pool.print(levelInfo, "task pool is closed, worker is closed")
				return
			}
		}
	}()
}

type TaskPool struct {
	isPre              bool          // 是否预先分配协程
	running            int32         // 正在运行的数量
	blocking           int32         // 阻塞的个数
	isClosed           int32         // cancel() 后设置为 ture
	capacity           int           // 最大工作数
	poolName           string        // 任务池的名称, 用日志记录前缀
	log                cjLogger      // log
	polTime            time.Duration // 哨兵默认轮询时间
	lastCleanUpTime    int64         // 上一次哨兵清理 worker 的时间
	workerMaxLifeCycle sec           // worker 最大存活周期(单位: 秒)
	freeWorkerQueue    []*worker     // 存放 worker, 保证能复用
	cancel             context.CancelFunc
	workerCache        sync.Pool
	rwMu               sync.RWMutex
	cond               *sync.Cond
}

// 通过此方法内部创建 ctx, 只能通过 Close() 来关闭协程池
func NewTaskPool(poolName string, capacity int, opts ...TaskPoolOption) *TaskPool {
	ctx, cancel := context.WithCancel(context.Background())
	if capacity <= 0 || capacity >= 9999 {
		capacity = runtime.NumCPU()
	}
	t := &TaskPool{
		isPre:              defaultIsPre,
		capacity:           capacity,
		poolName:           "(" + poolName + ")",
		freeWorkerQueue:    make([]*worker, 0, capacity),
		log:                newCjLogger(),
		polTime:            defaultPolDuration,
		workerMaxLifeCycle: defaultWorkerMaxLifeCycle,
		cancel:             cancel,
	}
	// 设置配置项
	for _, opt := range opts {
		opt(t)
	}
	t.workerCache.New = func() interface{} {
		return newWorker(ctx)
	}
	// 判断下是否与预分配
	if t.isPre {
		t.preGoWorker()
	}
	t.cond = sync.NewCond(&t.rwMu)

	// 起一个哨兵, 目的:
	// 1.用于定时唤醒阻塞队列
	// 2.定时删除生命到期了的 worker
	go t.poolSentinel(ctx)
	t.printf(levelInfo, "create taskPool is success, capacity: %d", t.capacity)
	return t
}

// 预先分配
func (t *TaskPool) preGoWorker() {
	for i := 0; i < t.capacity; i++ {
		w := t.workerCache.Get().(*worker)
		w.goWorker(t)
		t.freeWorkerQueueAppend(w)
		t.running++
	}
}

// 对外通过此方法向协程池添加任务
// 使用:
// 		1. 如果任务为 func() 的话可以直接传入,
// 		2. 如果带参的 func 需要包裹下, 如: test(1, 2, 3) => func() {test(1, 2, 3)}
func (t *TaskPool) Submit(task taskFunc) {
	if atomic.LoadInt32(&t.isClosed) == closed {
		t.print(levelError, "task pool is closed")
		return
	}
	w := t.getFreeWorker()
	w.taskCh <- task
}

// 生成 goroutine, 如果运行的数量大于等于最大数目的时候进行阻塞
func (t *TaskPool) getFreeWorker() (w *worker) {
	runFunc := func() {
		w = t.workerCache.Get().(*worker)
		w.goWorker(t)
	}
	t.rwMu.Lock()
rePop:
	w = t.freeWorkerQueueLPop()
	if w != nil {
		t.rwMu.Unlock()
	} else if int(t.running) < t.capacity {
		t.running++
		t.rwMu.Unlock()
		runFunc()
	} else {
		t.blocking++
		t.printf(levelInfo, "enter wait: running: %d, blocking: %d, freeWorkerLen: %v", t.running, t.blocking, len(t.freeWorkerQueue))
		// 有一个哨兵间隔 t.polTime 轮询, 根据 freeWorkerQueue 是否有空闲的 worker 进行唤醒
		t.cond.Wait()
		t.blocking--

		// 从 freeWorkerQueue 获取 worker, 如果有就返回, 没有的话就从新走下流程
		w = t.freeWorkerQueueLPop()
		if w == nil {
			goto rePop
		}
		t.rwMu.Unlock()
	}
	return
}

// 从 freeWorkerQueue 头取一个 worker
func (t *TaskPool) freeWorkerQueueLPop() (w *worker) {
	l := len(t.freeWorkerQueue)
	if l == 0 {
		return nil
	}
	w = t.freeWorkerQueue[0]
	t.freeWorkerQueue[0] = nil
	t.freeWorkerQueue = append(t.freeWorkerQueue[1:])
	return
}

// 归还 worker, 从 freeWorkerQueue 尾部追加
func (t *TaskPool) freeWorkerQueueAppend(w *worker) (isGiveUp bool) {
	t.rwMu.Lock()
	defer t.rwMu.Unlock()

	curTime := time.Now().Unix()
	// 如果存活时间到了就直接丢掉
	if curTime > w.startTime+t.workerMaxLifeCycle {
		t.print(levelInfo, "current goroutine is expire, it is give up")
		return true
	}

	t.freeWorkerQueue = append(t.freeWorkerQueue, w)
	return false
}

// 1. 定时唤醒加入阻塞队列的 worker
// 2. 空闲的时候清除 freeWorkerQueue 里的 worker
func (t *TaskPool) poolSentinel(ctx context.Context) {
	heartbeat := time.NewTicker(t.polTime)
	defer func() {
		heartbeat.Stop()
		if err := recover(); err != nil {
			t.printStackInfo("poolSentinel", err)
		}

		// 释放
		t.Close()
	}()

	// 这里根据轮询时间换算清理时间, 这里换算公式如: 1s 轮询一次, 1min 才清理一次
	cleanUpTime := 60 * sec(t.polTime/time.Second)
	for {
		select {
		case timeVal := <-heartbeat.C:
			t.awaken()

			if timeVal.Unix()-t.lastCleanUpTime > cleanUpTime {
				t.cleanUp()
				t.lastCleanUpTime = time.Now().Unix()
			}
		case <-ctx.Done():
			t.print(levelInfo, "pool is closed")
			return
		}
	}
}

// 唤醒阻塞队列
func (t *TaskPool) awaken() {
	// 判断 freeWorkerQueue 里是否有空闲的 worker,
	// 1. 如果空闲的总数等于设置的容量就全部释放
	// 2. 有多少空闲数就释放几个
	t.rwMu.RLock()
	freeWorkerLen := len(t.freeWorkerQueue)
	running := t.running
	t.rwMu.RUnlock()

	// 以下存在 data race, 可以不用管
	// 全部释放阻塞队列这里有两种情况:
	// 1. 空闲队列满就直接释放所有阻塞 task
	// 2. 队列里地协程都过期了, 还有任务被阻塞需要释放防止 task 一直不被执行.
	if freeWorkerLen == t.capacity || running == 0 {
		t.cond.Broadcast()
		return
	}

	// 有多少空闲的释放多少
	for i := 0; i < freeWorkerLen; i++ {
		t.cond.Signal()
	}

	// 剩余可运行的数量
	remainCanRunning := t.capacity - int(running)
	for j := 0; j < remainCanRunning; j++ {
		t.cond.Signal()
	}
}

// 清理生命周期到期的 worker
func (t *TaskPool) cleanUp() {
	t.rwMu.Lock()
	defer t.rwMu.Unlock()

	curTimestamp := time.Now().Unix()
	l := len(t.freeWorkerQueue)

	// 避免池子没有任务也打印日志
	if l > 0 || t.running > 0 || t.blocking > 0 {
		t.printf(levelInfo, "sentinel clean up, freeWorker count: %d, running: %d, blocking: %d", l, t.running, t.blocking)
	}
	if l == 0 {
		return
	}

	// 每轮只清理一个就退出
	expireAtIndex := -1
	for index, w := range t.freeWorkerQueue {
		if curTimestamp > w.startTime+t.workerMaxLifeCycle {
			expireAtIndex = index
			w.taskCh <- nil
			break
		}
	}
	if expireAtIndex == -1 {
		return
	}

	// 先置空, 避免内存泄露
	t.freeWorkerQueue[expireAtIndex] = nil
	if expireAtIndex == 0 {
		t.freeWorkerQueue = append(t.freeWorkerQueue[1:])
	} else if expireAtIndex == l-1 {
		t.freeWorkerQueue = append(t.freeWorkerQueue[:l-1])
	} else {
		t.freeWorkerQueue = append(t.freeWorkerQueue[:expireAtIndex], t.freeWorkerQueue[expireAtIndex+1:]...)
	}
}

// 打印 runtime 的错误栈消息
func (t *TaskPool) printStackInfo(funcName string, shorErr interface{}) {
	errBytes := debug.Stack()
	t.printf(levelError, "funcName: %s, shortErr: %v, stackErr: %s", funcName, shorErr, string(errBytes))
}

func (t *TaskPool) print(level logLevel, v string) {
	if level == levelError {
		t.log.Error(t.poolName, v)
		return
	}
	t.log.Info(t.poolName, v)
}

func (t *TaskPool) printf(level logLevel, format string, v ...interface{}) {
	argsLen := len(v) + 1
	args := make([]interface{}, argsLen)

	// 把第一个参数设置为 poolName
	args[0] = t.poolName
	for i := 1; i < argsLen; i++ {
		args[i] = v[i-1]
	}

	// %s 为协程池的名字
	if level == levelError {
		t.log.Errorf("%s "+format, args...)
		return
	}
	t.log.Infof("%s "+format, args...)
}

// 关闭协程池, 注意: 每次调用完一定要释放
func (t *TaskPool) Close() {
	t.cancel()
	// 将空闲队列释放
	t.freeWorkerQueue = nil
	// 修改标记位
	atomic.StoreInt32(&t.isClosed, closed)
}

func (t *TaskPool) Running() int32 {
	t.rwMu.RLock()
	defer t.rwMu.RUnlock()
	return t.running
}

func (t *TaskPool) Blocking() int32 {
	t.rwMu.RLock()
	defer t.rwMu.RUnlock()
	return t.blocking
}

func (t *TaskPool) FreeWorkerQueueLen() int {
	t.rwMu.RLock()
	defer t.rwMu.RUnlock()
	return len(t.freeWorkerQueue)
}
