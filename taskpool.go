package taskpool

import (
	"context"
	"fmt"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// 默认
	defaultPolDuration        time.Duration = time.Minute // 哨兵默认轮询时间
	defaultWorkerMaxLifeCycle sec           = 5 * sec(60) // worker 最大存活期(单位: 秒)

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

// WithCtx 外部设置 content.Context
func WithCtx(ctx context.Context) TaskPoolOption {
	return func(p *TaskPool) {
		p.ctx = ctx
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
			pool.deCrRunning()
			pool.workerCache.Put(w)
			// fmt.Printf("run: %d, block: %d", pool.Running(), pool.Blocking())
			// 防止 running-1 发生在 getFreeWorker 之后, 就会出现一个协程一直阻塞, 需要再释放下
			pool.cond.Signal()
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
				if isGiveUp := pool.freeWorkerQueueAppend(w, true); isGiveUp {
					pool.printf(levelInfo, "worker [%s] is expire, it is give up", w.workNo)
					return
				}
				pool.cond.Signal() // 通知阻塞的去取
			case <-w.ctx.Done():
				pool.printf(levelInfo, "pool close worker [%s] exit", w.workNo)
				return
			}
		}
	}()
}

// TaskPool 任务池
type TaskPool struct {
	ctx                context.Context
	cancel             context.CancelFunc // 外部没有传入 content.Context, 内部会初始化此值
	isPre              bool               // 是否预先分配协程
	printLog           bool               // 是否打印 log
	running            int32              // 正在运行的数量
	blocking           int32              // 阻塞的个数
	isClosed           int32              // 标记是否关闭
	capacity           int                // 最大工作数
	poolName           string             // 任务池的名称, 用日志记录前缀
	log                Logger             // log
	polTime            time.Duration      // 哨兵默认轮询时间
	lastCleanUpTime    int64              // 上一次哨兵清理 worker 的时间
	workerMaxLifeCycle sec                // worker 最大存活周期(单位: 秒)
	freeWorkerQueue    []*worker          // 存放 worker, 保证能复用
	workerCache        sync.Pool
	rwMu               sync.RWMutex
	cond               *sync.Cond
}

// NewTaskPool 通过此方法内部创建 ctx, 需要通过 Close() 来关闭协程池, 防止协程泄露
func NewTaskPool(poolName string, capacity int, opts ...TaskPoolOption) *TaskPool {
	if capacity <= 0 || capacity >= 9999 {
		capacity = runtime.NumCPU()
	}
	t := &TaskPool{
		isPre:              false,
		printLog:           true,
		capacity:           capacity,
		poolName:           "(" + poolName + ")",
		freeWorkerQueue:    make([]*worker, 0, capacity),
		log:                newLogger(),
		polTime:            defaultPolDuration,
		workerMaxLifeCycle: defaultWorkerMaxLifeCycle,
		// cancel:             cancel,
	}

	for _, opt := range opts {
		opt(t)
	}

	if t.ctx == nil {
		t.ctx, t.cancel = context.WithCancel(context.Background())
	}
	t.workerCache.New = func() interface{} {
		return newWorker(t.ctx)
	}
	if t.isPre {
		t.preGoWorker()
	}
	t.cond = sync.NewCond(&t.rwMu)

	// 开启一个哨兵, 目的:
	// 1.用于定时唤醒阻塞队列
	// 2.定时删除生命到期了的 worker
	go t.poolSentinel()
	t.printf(levelInfo, "create taskPool is success, worker life time for %d sec, capacity: %d", t.workerMaxLifeCycle, t.capacity)
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
//  1. 如果任务为 func() 的话可以直接传入,
//  2. 如果带参的 func 需要包裹下, 如: test(1, 2, 3) => func() {test(1, 2, 3)}
//
// 注: 调用 SafeClose(局部调用)的场景, 使用异步提交的时候会失败
func (t *TaskPool) Submit(task taskFunc, async ...bool) {
	if t.closed() {
		t.print(levelError, "task pool is closed")
		return
	}

	if len(async) > 0 && async[0] {
		go func() {
			w := t.getFreeWorker()
			if t.closed() {
				return
			}
			w.taskCh <- task
		}()
	} else {
		w := t.getFreeWorker()
		if t.closed() { // 防止已经获取到 w, 但 pool 已经关了, 这里再验证下
			return
		}
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
		t.printf(levelInfo, "pool enter wait [running: %d, blocking: %d, freeWorkerLen: %d]", t.running, t.blocking, len(t.freeWorkerQueue))
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
func (t *TaskPool) poolSentinel() {
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
			if timeVal.Unix()-t.lastCleanUpTime > cleanUpTime {
				t.cleanUp(false)
				t.lastCleanUpTime = time.Now().Unix()
			}
		case <-t.ctx.Done():
			t.print(levelInfo, "pool is closed")
			return
		}
	}
}

// cleanUp 清理生命周期到期的 worker
func (t *TaskPool) cleanUp(isSafeClose bool) {
	l := t.FreeWorkerQueueLen()
	running := t.Running()
	blocking := t.Blocking()
	// 避免池子没有任务也打印日志
	if !isSafeClose && (l > 0 || running > 0 || blocking > 0) {
		t.printf(levelInfo, "sentinel clean up [running: %d, blocking: %d, freeWorkerLen: %d]", running, blocking, l)
	}
	if blocking > 0 {
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
	if !t.printLog {
		return
	}

	if level == levelError {
		t.log.Error(t.poolName, v)
		return
	}
	t.log.Info(t.poolName, v)
}

// printf
func (t *TaskPool) printf(level logLevel, format string, v ...interface{}) {
	if !t.printLog {
		return
	}

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
//  1. 每次调用完一定要释放
//  2. 局部使用推荐使用 SafeClose, 防止任务未执行完就退出
func (t *TaskPool) Close() {
	if t.closed() {
		return
	}
	atomic.StoreInt32(&t.isClosed, closed)

	if t.cancel != nil {
		t.cancel()
	}
	// t.cond.Broadcast()
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

func (t *TaskPool) deCrRunning() {
	t.rwMu.Lock()
	defer t.rwMu.Unlock()
	t.running--
}

// Running 获取运行 worker 数量
func (t *TaskPool) Running() int32 {
	t.rwMu.RLock()
	defer t.rwMu.RUnlock()
	return t.running
}

// Blocking 获取阻塞的 worker 数量
func (t *TaskPool) Blocking() int32 {
	t.rwMu.RLock()
	defer t.rwMu.RUnlock()
	return t.blocking
}

// FreeWorkerQueueLen 空闲队列池里的长度
func (t *TaskPool) FreeWorkerQueueLen() int {
	t.rwMu.RLock()
	defer t.rwMu.RUnlock()
	return len(t.freeWorkerQueue)
}

// getGoId 获取 goroutine id
func getGoId() (gid string) {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	idField := strings.Fields(strings.TrimPrefix(string(buf[:n]), "goroutine "))[0]
	return idField
}
