package taskpool

import (
	"log"
	"math/rand"
	"runtime"
	"sync"
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

func GoId() (gid string) {
	var (
		buf     [21]byte
		idBytes [5]byte
	)
	size := runtime.Stack(buf[:], false)

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

var (
	wg sync.WaitGroup
)

func testTask() {
	randInt := time.Duration(rand.Intn(5))
	gid := GoId()
	log.Printf("开始执行任务的time: %v, gid: %s\n", time.Now().Format("2006-01-02 15:04:05.000"), gid)
	time.Sleep(randInt * time.Second)
	log.Printf("执行任务结束的time: %v, task hello world, 任务运行时间: %d, gid: %s\n", time.Now().Format("2006-01-02 15:04:05.000"), randInt, gid)
	wg.Done()
}

func testTaskArg(id int) {
	log.Printf("开始执行任务的time: %v", time.Now().Format("2006-01-02 15:04:05.000"))
	s := time.Duration(rand.Intn(5))
	time.Sleep(s * time.Second)
	log.Printf("执行任务结束的time: %v, num: %d 任务运行时间: %d\n", time.Now().Format("2006-01-02 15:04:05.000"), id, s)
	wg.Done()
}

func TestGetGoId(t *testing.T) {
	defer func() {
		t.Log(GoId())
	}()
	panic("hello")
}

func TestNewTaskPool_NoArg(t *testing.T) {
	p := NewTaskPool("test", 2, WithWorkerMaxLifeCycle(2), WithPolTime(time.Second))
	// p := NewTaskPool("test", 2, WithWorkerMaxLifeCycle(2))
	defer p.Close()
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go p.Submit(testTask)
	}
	wg.Wait()
}

func TestNewTaskPool_HaveArg(t *testing.T) {
	p := NewTaskPool("test", 2)
	defer p.Close()
	log.Printf("curtime: %v\n", time.Now().Format("2006-01-02 15:04:05.000"))
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go p.Submit(func() {
			func(i int) {
				testTaskArg(i)
			}(i)
		})
	}
	wg.Wait()
	time.Sleep(time.Second * 10)
}

func TestLogInfo(t *testing.T) {
	p := NewTaskPool("test", 2)
	defer p.Close()
	p.print(levelInfo, "hello")
	p.printf(levelInfo, "name: %s, age: %d", "xue", 18)
}

func TestNewTaskPool_Close(t *testing.T) {
	p := NewTaskPool("test", 2)
	defer p.Close()
	for i := 0; i < 10; i++ {
		t.Logf("i: %v\n", i)
		if i == 6 {
			p.Close()
		}
		p.Submit(func() {
			log.Println("task")
		})
	}
	time.Sleep(time.Second * 2)
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

func BenchmarkNewTaskPool_NoArg(b *testing.B) {
	p := NewTaskPool("test", 500, WithProGoWorker())
	defer p.Close()
	for i := 0; i < 5000; i++ {
		wg.Add(1)
		go p.Submit(testTask)
	}
	wg.Wait()
}
