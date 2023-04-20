package taskpool

import (
	"context"
	"fmt"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// 默认
	defaultPolDuration        time.Duration = 5 * time.Minute // 哨兵默认轮询时间
	defaultWorkerMaxLifeCycle sec           = 10 * sec(60)    // worker 最大存活期(单位: 秒)

	// 状态
	closed int32 = 1 // 任务池是否关闭

	// 日志
	levelInfo logLevel = iota
	levelError
)

type (
	sec            = int64
	logLevel       int
	taskFunc       func()
	TaskPoolOption func(p *TaskPool)
)

// WithPoolLogger 设置日志 log
func WithPoolLogger(logger Logger) TaskPoolOption {
	return func(p *TaskPool) {
		p.log = logger
	}
}

// WithPoolPrint 设置是否打印 log
func WithPoolPrint(print bool) TaskPoolOption {
	return func(p *TaskPool) {
		p.printLog = print
	}
}

// WithPolTime 设置 taskPool 中哨兵轮询地时间
func WithPolTime(t time.Duration) TaskPoolOption {
	return func(p *TaskPool) {
		p.polTime = t
	}
}

// WithWorkerMaxLifeCycle 设置 taskPool 中空闲的 worker 存活的时间
func WithWorkerMaxLifeCycle(timeForSec sec) TaskPoolOption {
	return func(p *TaskPool) {
		p.workerMaxLifeCycle = timeForSec
	}
}

// WithProGoWorker 预分配协程
func WithProGoWorker() TaskPoolOption {
	return func(p *TaskPool) {
		p.isPre = true
	}
}

// worker 工作者
type worker struct {
	ctx       context.Context // 用于传递关闭信号
	startTime int64           // 记录开始的时间
	workNo    string          // work 编号
	taskCh    chan taskFunc
}

// newWorker
func newWorker(ctx context.Context) *worker {
	return &worker{
		ctx:    ctx,
		taskCh: make(chan taskFunc),
	}
}

// goWorker 起一个工作协程
func (w *worker) goWorker(pool *TaskPool) {
	go func() {
		defer func() {
			if err := recover(); err != nil {
				pool.printStackInfo(fmt.Sprintf("worker [%s]", w.workNo), err)
			}
			pool.workerCache.Put(w)
			atomic.AddInt32(&pool.running, -1)
		}()

		w.workNo = getGoId()
		pool.printf(levelInfo, "gen worker [%s] is ok", w.workNo)
		for {
			select {
			case f := <-w.taskCh:
				if f == nil {
					pool.printf(levelInfo, "pool clean up worker [%s] exit", w.workNo)
					return
				}
				f()

				// 放入 freeWorkerQueue 为了后面复用
				isGiveUp := pool.freeWorkerQueueAppend(w, true)
				pool.cond.Signal() // 通知阻塞的去取
				if isGiveUp {
					pool.printf(levelInfo, "worker [%s] is expire, it is give up", w.workNo)
					return
				}
			case <-w.ctx.Done():
				pool.printf(levelInfo, "pool close worker [%s] exit", w.workNo)
				return
			}
		}
	}()
}

// TaskPool 任务池
type TaskPool struct {
	isPre              bool          // 是否预先分配协程
	printLog           bool          // 是否打印 log
	running            int32         // 正在运行的数量
	blocking           int32         // 阻塞的个数
	isClosed           int32         // 标记是否关闭
	capacity           int           // 最大工作数
	poolName           string        // 任务池的名称, 用日志记录前缀
	log                Logger        // log
	polTime            time.Duration // 哨兵默认轮询时间
	lastCleanUpTime    int64         // 上一次哨兵清理 worker 的时间
	workerMaxLifeCycle sec           // worker 最大存活周期(单位: 秒)
	freeWorkerQueue    []*worker     // 存放 worker, 保证能复用
	cancel             context.CancelFunc
	workerCache        sync.Pool
	rwMu               sync.RWMutex
	cond               *sync.Cond
}

// NewTaskPool 通过此方法内部创建 ctx, 需要通过 Close() 来关闭协程池, 防止协程泄露
func NewTaskPool(poolName string, capacity int, opts ...TaskPoolOption) *TaskPool {
	ctx, cancel := context.WithCancel(context.Background())
	if capacity <= 0 || capacity >= 9999 {
		capacity = runtime.NumCPU()
	}
	t := &TaskPool{
		isPre:              false,
		printLog:           true,
		capacity:           capacity,
		poolName:           "(" + poolName + ")",
		freeWorkerQueue:    make([]*worker, 0, capacity),
		log:                newCjLogger(),
		polTime:            defaultPolDuration,
		workerMaxLifeCycle: defaultWorkerMaxLifeCycle,
		cancel:             cancel,
	}
	for _, opt := range opts {
		opt(t)
	}
	t.workerCache.New = func() interface{} {
		return newWorker(ctx)
	}
	if t.isPre {
		t.preGoWorker()
	}
	t.cond = sync.NewCond(&t.rwMu)

	// 开启一个哨兵, 目的:
	// 1.用于定时唤醒阻塞队列
	// 2.定时删除生命到期了的 worker
	go t.poolSentinel(ctx)
	t.printf(levelInfo, "create taskPool is success, clean worker time for %d sec, capacity: %d", t.workerMaxLifeCycle, t.capacity)
	return t
}

// preGoWorker 预先分配
func (t *TaskPool) preGoWorker() {
	for i := 0; i < t.capacity; i++ {
		t.freeWorkerQueueAppend(t.genGo(), false)
		t.running++
	}
}

// genGo 生成 goroutine
func (t *TaskPool) genGo() *worker {
	w := t.workerCache.Get().(*worker)
	w.startTime = time.Now().Unix()
	w.goWorker(t)
	return w
}

// Submit 对外通过此方法向协程池添加任务
// 使用:
// 		1. 如果任务为 func() 的话可以直接传入,
// 		2. 如果带参的 func 需要包裹下, 如: test(1, 2, 3) => func() {test(1, 2, 3)}
// 注: 调用 SafeClose(局部调用)的场景, 使用异步提交的时候会失败
func (t *TaskPool) Submit(task taskFunc, async ...bool) {
	if t.closed() {
		t.print(levelError, "task pool is closed")
		return
	}

	if len(async) > 0 && async[0] {
		go func() {
			w := t.getFreeWorker()
			w.taskCh <- task
		}()
	} else {
		w := t.getFreeWorker()
		w.taskCh <- task
	}
}

// getFreeWorker 获取 goroutine, 如果运行的数量大于等于最大数目的时候进行阻塞
func (t *TaskPool) getFreeWorker() (w *worker) {
	t.rwMu.Lock()
rePop:
	// 处理流程:
	// 1. 先从空闲队列取
	// 2. 如果运行的 goroutine 小于 容量就直接创建 goroutine
	// 3. 进行阻塞处理, 后台有个哨兵进行唤醒
	if w = t.freeWorkerQueueLPop(false); w != nil {
		t.rwMu.Unlock()
	} else if int(t.running) < t.capacity {
		t.running++
		t.rwMu.Unlock()
		w = t.genGo()
	} else {
		t.blocking++
		if t.printLog {
			t.printf(levelInfo, "pool enter wait [running: %d, blocking: %d, freeWorkerLen: %d]", t.running, t.blocking, len(t.freeWorkerQueue))
		}
		// 唤醒时机:
		// 1. 每个 worker 执行完任务后都会唤醒
		// 2. 有一个哨兵间隔 t.polTime 轮询, 根据 freeWorkerQueue 是否有空闲的 worker 进行唤醒
		t.cond.Wait()
		t.blocking--

		// 从 freeWorkerQueue 获取 worker, 如果有就返回, 没有的话就从新走下流程
		w = t.freeWorkerQueueLPop(false)
		if w == nil {
			goto rePop
		}
		t.rwMu.Unlock()
	}
	return
}

// freeWorkerQueueLPop 从 freeWorkerQueue 头取一个 worker
func (t *TaskPool) freeWorkerQueueLPop(needLock bool) (w *worker) {
	if needLock {
		t.rwMu.Lock()
		defer t.rwMu.Unlock()
	}

	if len(t.freeWorkerQueue) == 0 {
		return nil
	}
	w = t.freeWorkerQueue[0]
	// 复用
	t.freeWorkerQueue = append(t.freeWorkerQueue[:0], t.freeWorkerQueue[1:]...)
	return
}

// freeWorkerQueueAppend 归还 worker, 从 freeWorkerQueue 尾部追加
func (t *TaskPool) freeWorkerQueueAppend(w *worker, needLock bool) (isGiveUp bool) {
	if needLock {
		t.rwMu.Lock()
		defer t.rwMu.Unlock()
	}

	if t.running >= int32(t.capacity) {
		return true
	}

	// 如果存活时间到了就直接丢掉
	curTime := time.Now().Unix()
	if curTime-w.startTime > t.workerMaxLifeCycle {
		return true
	}

	t.freeWorkerQueue = append(t.freeWorkerQueue, w)
	return false
}

// poolSentinel 哨兵
// 1. 定时唤醒加入阻塞队列的 worker
// 2. 空闲的时候清除 freeWorkerQueue 里的 worker
func (t *TaskPool) poolSentinel(ctx context.Context) {
	heartbeat := time.NewTicker(t.polTime)
	defer func() {
		if err := recover(); err != nil {
			t.printStackInfo("poolSentinel", err)
		}
		heartbeat.Stop()

		// 释放子协程
		t.Close()
	}()

	cleanUpTime := sec(t.polTime / time.Second)
	for {
		select {
		case timeVal := <-heartbeat.C:
			// t.awaken() // worker 结束后会自动唤醒
			if timeVal.Unix()-t.lastCleanUpTime > cleanUpTime {
				t.cleanUp(false)
				t.lastCleanUpTime = time.Now().Unix()
			}
		case <-ctx.Done():
			t.print(levelInfo, "pool is closed")
			return
		}
	}
}

// Deprecated: awaken 唤醒阻塞队列
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

	// 上面根据队列空闲 worker 来释放有可能出现总队列数小于容量(过期的会被自动清理),
	// 所有可以再根据剩余可运行的数量进行唤醒
	remainCanRunning := t.capacity - int(running)
	for j := 0; j < remainCanRunning; j++ {
		t.cond.Signal()
	}
}

// cleanUp 清理生命周期到期的 worker
func (t *TaskPool) cleanUp(isSafeClose bool) {
	l := t.FreeWorkerQueueLen()
	// 避免池子没有任务也打印日志
	if !isSafeClose && (l > 0 || t.running > 0 || t.blocking > 0) && t.printLog {
		t.printf(levelInfo, "sentinel clean up [freeWorker: %d, running: %d, blocking: %d]", l, t.running, t.blocking)
	}
	if t.Blocking() > 0 {
		t.cond.Signal()
	}
	if l == 0 {
		return
	}

	// 1. 如果是调用 SafeClose() 就一次处理完
	if isSafeClose {
		w := t.freeWorkerQueueLPop(true)
		for w != nil {
			w.taskCh <- nil
			w = t.freeWorkerQueueLPop(true)
		}
		return
	}

	// 2. 如果是后台哨兵处理时, 每轮只清理一个就退出, 保证尽可能多的 worker 执行
	curTimestamp := time.Now().Unix()
	expireAtIndex := -1
	t.rwMu.Lock()
	defer t.rwMu.Unlock()
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

	// 通过清空在append达到复用
	if expireAtIndex == 0 {
		t.freeWorkerQueue = append(t.freeWorkerQueue[:0], t.freeWorkerQueue[1:]...)
	} else if expireAtIndex == l-1 {
		t.freeWorkerQueue = append(t.freeWorkerQueue[:0], t.freeWorkerQueue[:l-1]...)
	} else {
		t.freeWorkerQueue = append(t.freeWorkerQueue[:expireAtIndex], t.freeWorkerQueue[expireAtIndex+1:]...)
	}
}

// printStackInfo 打印 runtime 的错误栈消息
func (t *TaskPool) printStackInfo(funcName string, shortErr interface{}) {
	t.printf(levelError, "funcName: %s, shortErr: %v, stackErr: %s", funcName, shortErr, string(debug.Stack()))
}

// print
func (t *TaskPool) print(level logLevel, v string) {
	if level == levelError {
		t.log.Error(t.poolName, v)
		return
	}
	t.log.Info(t.poolName, v)
}

// printf
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

func (t *TaskPool) closed() bool {
	return atomic.LoadInt32(&t.isClosed) == closed
}

// Close 关闭协程池,
//
// 注意:
//     1. 每次调用完一定要释放
//     2. 局部使用推荐使用 SafeClose, 防止任务未执行完就退出
func (t *TaskPool) Close() {
	if t.closed() {
		return
	}
	atomic.StoreInt32(&t.isClosed, closed)

	t.cancel()
	t.rwMu.Lock()
	t.freeWorkerQueue = nil
	t.rwMu.Unlock()
}

// SafeClose 安全的关闭, 这样可以保证未处理的任务都执行完
// 注: 只能阻塞同步提交的任务
func (t *TaskPool) SafeClose(timeout ...time.Duration) {
	if t.closed() {
		return
	}
	var (
		ctx    = context.Background()
		cancel context.CancelFunc
	)
	if len(timeout) > 0 {
		ctx, cancel = context.WithTimeout(context.Background(), timeout[0])
		defer cancel()
	}

	// 将过期时间设置为 1 秒, 执行完了再回收时(freeWorkerQueueAppend)就直接舍弃掉
	t.rwMu.Lock()
	t.workerMaxLifeCycle = sec(1)
	t.rwMu.Unlock()
	defer t.Close()
	for {
		select {
		case <-ctx.Done():
			t.printf(levelInfo, "task pool have %d working", t.running)
			return
		default:
		}

		// 没有跑的 goroutine 也可以直接退出
		if t.Running() == 0 && t.Blocking() == 0 {
			return
		}
		t.cleanUp(true)
	}
}

// Running 获取运行 worker 数量
func (t *TaskPool) Running() int32 {
	return atomic.LoadInt32(&t.running)
}

// Blocking 获取阻塞的 worker 数量
func (t *TaskPool) Blocking() int32 {
	return atomic.LoadInt32(&t.blocking)
}

// FreeWorkerQueueLen 空闲队列池里的长度
func (t *TaskPool) FreeWorkerQueueLen() int {
	t.rwMu.RLock()
	defer t.rwMu.RUnlock()
	return len(t.freeWorkerQueue)
}

// getGoId 获取 goroutine id
func getGoId() (gid string) {
	var (
		buf     [50]byte
		idBytes [25]byte
	)
	size := runtime.Stack(buf[:], false)
	// fmt.Println("test:", string(buf[:]))
	// 如: goroutine 8 [running]
	j := 0
	for i := 0; i < size; i++ {
		v := buf[i]
		if v >= '0' && v <= '9' {
			idBytes[j] = v
			j++
		}
	}
	return string(idBytes[:])
}
