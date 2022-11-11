package taskpool

import (
	"fmt"
	"log"
	"math/rand"
	"runtime"
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

func TestGetGoId(t *testing.T) {
	defer func() {
		t.Log(getGoId())
	}()
	panic("hello")
}

func TestNewTaskPool_NoArg(t *testing.T) {
	p := NewTaskPool("test", 2, WithWorkerMaxLifeCycle(2), WithPolTime(time.Second))
	defer p.SafeClose()

	fn := func() {
		randInt := time.Duration(rand.Intn(5))
		gid := getGoId()
		fmt.Printf(">>开始执行任务的time: %v, gid: %s\n", time.Now().Format("2006-01-02 15:04:05.000"), gid)
		time.Sleep(randInt * time.Second)
		fmt.Printf(">>执行任务结束的time: %v, 任务运行时间: %d sec, gid: %s\n", time.Now().Format("2006-01-02 15:04:05.000"), randInt, gid)
	}
	for i := 0; i < 5; i++ {
		p.Submit(fn)
	}
}

func TestNewTaskPool_HaveArg(t *testing.T) {
	p := NewTaskPool("test", 2)
	defer p.SafeClose()
	log.Printf("curtime: %v\n", time.Now().Format("2006-01-02 15:04:05.000"))

	fn := func(id int) {
		gid := getGoId()
		fmt.Printf(">>开始执行任务的time: %v, gid: %v\n", time.Now().Format("2006-01-02 15:04:05.000"), gid)
		s := time.Duration(rand.Intn(5))
		time.Sleep(s * time.Second)
		fmt.Printf(">>执行任务结束的time: %v, num: %d 任务运行时间: %d, gid:%v\n", time.Now().Format("2006-01-02 15:04:05.000"), id, s, gid)
	}

	for i := 0; i < 5; i++ {
		p.Submit(func() {
			func(i int) {
				fn(i)
			}(i)
		}, true)
	}
	time.Sleep(time.Second * 10)
	p.SafeClose()
}

func TestLogInfo(t *testing.T) {
	p := NewTaskPool("test", 2)
	defer p.Close()
	p.print(levelInfo, "hello")
	p.printf(levelInfo, "name: %s, age: %d", "xue", 18)
}

func TestNewTaskPool_SafeClose(t *testing.T) {
	p := NewTaskPool("test", 2)
	defer p.SafeClose()

	for i := 0; i < 10; i++ {
		time.Sleep(time.Second)
		p.Submit(func() {
			fmt.Printf("time: %v, i: %d\n", time.Now().Format("2006-01-02 15:04:05.000"), i)
		})
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
	p := NewTaskPool("test", 20)
	defer p.SafeClose()
	ptr := fmt.Sprintf("%p", p.freeWorkerQueue)

	fn := func() {
		randInt := time.Duration(rand.Intn(5))
		gid := getGoId()
		fmt.Printf(">>开始执行任务的time: %v, gid: %s\n", time.Now().Format("2006-01-02 15:04:05.000"), gid)
		time.Sleep(randInt * time.Second)
		fmt.Printf(">>执行任务结束的time: %v, 任务运行时间: %d sec, gid: %s\n", time.Now().Format("2006-01-02 15:04:05.000"), randInt, gid)
	}
	for i := 0; i < 500; i++ {
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
