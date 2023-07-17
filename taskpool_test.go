package taskpool

import (
	"fmt"
	"log"
	"math/rand"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
	// pprofHandler := http.NewServeMux()
	// pprofHandler.Handle("/debug/pprof/", http.HandlerFunc(pprof.Index))
	// server := &http.Server{Addr: "8888", Handler: pprofHandler}
	// go server.ListenAndServe()
}

func TestTmp(t *testing.T) {
	a := []int{1}
	t.Log(a, len(a), cap(a))
	ptr := fmt.Sprintf("%p", a)
	a = append(a[:0], a[1:]...)
	ptr1 := fmt.Sprintf("%p", a)
	if ptr1 != ptr {
		t.Error("failed")
	}
	t.Log(a)
}

func TestGetGoId(t *testing.T) {
	t.Skip()
	defer func() {
		t.Log(getGoId())
	}()
	panic("hello")
}

func TestNewTaskPool_NoArg(t *testing.T) {
	t.Log("TestNewTaskPool_NoArg start")
	p := NewTaskPool("test", 1, WithWorkerMaxLifeCycle(2), WithPolTime(time.Minute))
	count := int32(0)
	fn := func() {
		gid := getGoId()
		fmt.Printf(">>开始执行任务的time: %v, gid: %s\n", time.Now().Format("2006-01-02 15:04:05.000"), gid)
		atomic.AddInt32(&count, 1)

		randInt := time.Duration(rand.Intn(5))
		time.Sleep(randInt * time.Second)
		fmt.Printf(">>执行任务结束的time: %v, 任务运行时间: %d sec, gid: %s\n", time.Now().Format("2006-01-02 15:04:05.000"), randInt, gid)
	}
	size := int32(3)
	for i := 0; i < int(size); i++ {
		p.Submit(fn)
	}
	p.SafeClose()

	if atomic.LoadInt32(&count) != size {
		t.Fatalf("handle failed, count: %d, size: %d", count, size)
	}
}

func TestNewTaskPool_HaveArg(t *testing.T) {
	t.Log("TestNewTaskPool_HaveArg start")
	p := NewTaskPool("test", 1)
	defer p.Close()
	log.Printf("curtime: %v\n", time.Now().Format("2006-01-02 15:04:05.000"))

	count := int32(0)
	fn := func(id int) {
		gid := getGoId()
		fmt.Printf(">>开始执行任务的time: %v, gid: %v\n", time.Now().Format("2006-01-02 15:04:05.000"), gid)
		atomic.AddInt32(&count, 1)

		s := time.Duration(rand.Intn(3))
		time.Sleep(s * time.Second)
		fmt.Printf(">>执行任务结束的time: %v, num: %d 任务运行时间: %d, gid:%v\n", time.Now().Format("2006-01-02 15:04:05.000"), id, s, gid)
	}

	size := int32(5)
	for i := 0; i < int(size); i++ {
		tmp := i
		p.Submit(func() {
			fn(tmp)
		})
	}
	time.Sleep(time.Second * 10)

	if atomic.LoadInt32(&count) != size {
		t.Fatalf("handle failed, count: %d, size: %d", count, size)
	}
}

func TestLogInfo(t *testing.T) {
	p := NewTaskPool("test", 2)
	defer p.Close()
	p.print(levelInfo, "hello")
	p.printf(levelInfo, "name: %s, age: %d", "xue", 18)
}

func TestNewTaskPool_SafeClose(t *testing.T) {
	t.Log("TestNewTaskPool_SafeClose start")
	size := 3
	p := NewTaskPool("test", size)

	a := make(map[int]struct{}, size)
	for i := 0; i < 10; i++ {
		tmp := i
		p.Submit(func() {
			a[tmp] = struct{}{}
		})
	}
	p.SafeClose()

	// 验证
	for i := 0; i < size; i++ {
		if _, ok := a[i]; !ok {
			t.Fatal("handle is failed")
		}
	}
}

func TestPanicDemo(t *testing.T) {
	defer func() {
		if err := recover(); err != nil {
			var buf [4096]byte
			n := runtime.Stack(buf[:], false)
			t.Logf("title: %s, panic: %s\n", "test", string(buf[:n]))
		}
	}()
	for i := 0; i < 10; i++ {
		if i == 5 {
			panic("5")
		}
	}
}

func TestQueueChange(t *testing.T) {
	t.Log("TestQueueChange start")
	p := NewTaskPool("test", 20)
	defer p.SafeClose()
	ptr := fmt.Sprintf("%p", p.freeWorkerQueue)

	fn := func() {
		randInt := time.Duration(rand.Intn(2))
		gid := getGoId()
		fmt.Printf(">>开始执行任务的time: %v, gid: %s\n", time.Now().Format("2006-01-02 15:04:05.000"), gid)
		time.Sleep(randInt * time.Second)
		fmt.Printf(">>执行任务结束的time: %v, 任务运行时间: %d sec, gid: %s\n", time.Now().Format("2006-01-02 15:04:05.000"), randInt, gid)
	}
	for i := 0; i < 500; i++ {
		// p.freeWorkerQueue 地址没有变
		if ptrAddr := fmt.Sprintf("%p", p.freeWorkerQueue); ptrAddr != ptr {
			t.Fatal("ptrAddr is change")
		}
		p.Submit(fn)
	}
}

// 通过 append 进行删除达到复用
func TestAppend1(t *testing.T) {
	demos := make([]int, 0, 10)
	for i := 0; i < 10; i++ {
		demos = append(demos, i)
	}
	// 初始地址
	ptr1 := fmt.Sprintf("%p", demos)
	t.Logf("demos: %v, ptr1: %v", demos, ptr1)

	// 删除第一个
	demos = append(demos[:0], demos[1:]...)
	if ptr := fmt.Sprintf("%p", demos); ptr1 != ptr {
		t.Error("re allot addr, ptr2:", ptr)
	} else {
		t.Log("删除第一个, demos:", demos)
	}
	// 追加一个
	demos = append(demos, 10)
	if ptr := fmt.Sprintf("%p", demos); ptr1 != ptr {
		t.Error("re allot addr, ptr3:", ptr)
	} else {
		t.Log("追加一个, demos:", demos)
	}

	// 删除最后一个
	demos = append(demos[:0], demos[:len(demos)-1]...)
	if ptr := fmt.Sprintf("%p", demos); ptr1 != ptr {
		t.Error("re allot addr, ptr3:", ptr)
	} else {
		t.Log("删除最后一个, demos:", demos)
	}
	// 追加一个
	demos = append(demos, 11)
	if ptr := fmt.Sprintf("%p", demos); ptr1 != ptr {
		t.Error("re allot addr, ptr3:", ptr)
	} else {
		t.Log("追加一个, demos:", demos)
	}

	// 删除第3个
	demos = append(demos[:2], demos[3:]...)
	if ptr := fmt.Sprintf("%p", demos); ptr1 != ptr {
		t.Error("re allot addr, ptr3:", ptr)
	} else {
		t.Log("删除第3个, demos:", demos)
	}
	// 追加一个
	demos = append(demos, 12)
	if ptr := fmt.Sprintf("%p", demos); ptr1 != ptr {
		t.Error("re allot addr, ptr3:", ptr)
	} else {
		t.Log("追加一个, demos:", demos)
	}
}

// 通过切片操作
func TestAppend2(t *testing.T) {
	t.Skip()
	demos := make([]int, 0, 10)
	for i := 0; i < 10; i++ {
		demos = append(demos, i)
	}
	// 初始地址
	ptr1 := fmt.Sprintf("%p", demos)
	t.Logf("demos: %v, ptr1: %v", demos, ptr1)

	// 删除第一个
	demos = demos[1:]
	if ptr := fmt.Sprintf("%p", demos); ptr1 != ptr {
		t.Error("re allot addr, ptr2:", ptr)
	} else {
		t.Log("删除第一个, demos:", demos)
	}
	// 追加一个
	demos = append(demos, 10)
	if ptr := fmt.Sprintf("%p", demos); ptr1 != ptr {
		t.Error("re allot addr, ptr3:", ptr)
	} else {
		t.Log("追加一个, demos:", demos)
	}

	// 删除最后一个
	demos = demos[:len(demos)-1]
	if ptr := fmt.Sprintf("%p", demos); ptr1 != ptr {
		t.Error("re allot addr, ptr3:", ptr)
	} else {
		t.Log("删除最后一个, demos:", demos)
	}
	// 追加一个
	demos = append(demos, 11)
	if ptr := fmt.Sprintf("%p", demos); ptr1 != ptr {
		t.Error("re allot addr, ptr3:", ptr)
	} else {
		t.Log("追加一个, demos:", demos)
	}

	// 删除第3个
	demos = append(demos[:2], demos[3:]...)
	if ptr := fmt.Sprintf("%p", demos); ptr1 != ptr {
		t.Error("re allot addr, ptr3:", ptr)
	} else {
		t.Log("删除第3个, demos:", demos)
	}
	// 追加一个
	demos = append(demos, 12)
	if ptr := fmt.Sprintf("%p", demos); ptr1 != ptr {
		t.Error("re allot addr, ptr3:", ptr)
	} else {
		t.Log("追加一个, demos:", demos)
	}
}

func BenchmarkA1(b *testing.B) {
	for i := 0; i < b.N; i++ {
		demos := make([]int, 0, 10)
		for i := 0; i < 10; i++ {
			demos = append(demos, i)
		}

		// 删除第一个
		demos = append(demos[:0], demos[1:]...)
		// 追加一个
		demos = append(demos, 10)

		// 删除最后一个
		demos = append(demos[:0], demos[:len(demos)-1]...)
		// 追加一个
		demos = append(demos, 11)

		// 删除第3个
		demos = append(demos[:2], demos[3:]...)
		// 追加一个
		demos = append(demos, 12)
	}
}

func BenchmarkA2(b *testing.B) {
	for i := 0; i < b.N; i++ {
		demos := make([]int, 0, 10)
		for i := 0; i < 10; i++ {
			demos = append(demos, i)
		}
		// 初始地址

		// 删除第一个
		demos = demos[1:]
		// 追加一个
		demos = append(demos, 10)

		// 删除最后一个
		demos = demos[:len(demos)-1]
		// 追加一个
		demos = append(demos, 11)

		// 删除第3个
		demos = append(demos[:2], demos[3:]...)
		// 追加一个
		demos = append(demos, 12)
	}
}
