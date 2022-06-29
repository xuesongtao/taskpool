package main

import (
	"fmt"
	"log"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	"time"

	"gitee.com/xuesongtao/taskpool"
)

func init() {
	rand.Seed(time.Now().UnixNano())
	go func() {
		log.Println(http.ListenAndServe("localhost:8888", nil))
	}()
}

func getGoId() (gid string) {
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

func submit1() {
	p := taskpool.NewTaskPool("test", 15, taskpool.WithWorkerMaxLifeCycle(2), taskpool.WithPolTime(time.Second))
	defer p.SafeClose()

	fn := func() {
		randInt := time.Duration(rand.Intn(5))
		gid := getGoId()
		fmt.Printf(">>开始执行任务的time: %v, gid: %s\n", time.Now().Format("2006-01-02 15:04:05.000"), gid)
		time.Sleep(randInt * time.Second)
		fmt.Printf(">>执行任务结束的time: %v, 任务运行时间: %d sec, gid: %s\n", time.Now().Format("2006-01-02 15:04:05.000"), randInt, gid)
	}
	for i := 0; i < 10; i++ {
		p.Submit(fn)
	}
}

func submit2() {
	p := taskpool.NewTaskPool("test", 2, taskpool.WithWorkerMaxLifeCycle(2), taskpool.WithPolTime(time.Second))
	defer p.SafeClose()

	fn := func() {
		randInt := time.Duration(rand.Intn(5))
		gid := getGoId()
		fmt.Printf(">>开始执行任务的time: %v, gid: %s\n", time.Now().Format("2006-01-02 15:04:05.000"), gid)
		time.Sleep(randInt * time.Second)
		fmt.Printf(">>执行任务结束的time: %v, 任务运行时间: %d sec, gid: %s\n", time.Now().Format("2006-01-02 15:04:05.000"), randInt, gid)
	}
	for i := 0; i < 5; i++ {
		p.Submit(fn, true)
	}
	time.Sleep(time.Second * 10)
}

func main() {
	// submit1()
	submit2()
}
