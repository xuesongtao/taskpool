package main

import (
	"context"
	"fmt"
	"time"

	"gitee.com/xuesongtao/taskpool"
)

// ErrPageDemo 分页调用错误示例
func ErrPageDemo() {
	fmt.Println("ErrPageDemo start")
	defer fmt.Println("ErrPageDemo end")

	fn := func(page int) {
		if page%2 == 0 {
			time.Sleep(1 * time.Second)
		}
		fmt.Println("handle page:", page)
	}
	taskPool := taskpool.NewTaskPool("", 10)
	for i := 0; i < 10; i++ {
		taskPool.Submit(func() {
			fn(i)
		})
	}
	taskPool.SafeClose()

	// Output:
	// handle page: 1
	// handle page: 3
	// handle page: 3
	// handle page: 5
	// handle page: 9
	// handle page: 10
	// handle page: 8
	// handle page: 0
	// handle page: 8
	// handle page: 8
}

// CorrectPageDemo 分页正确示例
func CorrectPageDemo() {
	fmt.Println("CorrectSliceDemo start")
	defer fmt.Println("CorrectSliceDemo end")

	fn := func(page int) {
		if page%2 == 0 {
			time.Sleep(1 * time.Second)
		}
		fmt.Println("handle page:", page)
	}
	taskPool := taskpool.NewTaskPool("", 10)
	for i := 0; i < 10; i++ {
		a := i // 避免闭包引用问题
		taskPool.Submit(func() {
			fn(a)
		})
	}
	taskPool.SafeClose()

	// Output:
	// handle page: 1
	// handle page: 3
	// handle page: 5
	// handle page: 7
	// handle page: 9
	// handle page: 6
	// handle page: 8
	// handle page: 2
	// handle page: 0
	// handle page: 4
}

// ErrSliceDemo 处理切片
func ErrSliceDemo() {
	fmt.Println("ErrSliceDemo start")
	defer fmt.Println("ErrSliceDemo end")

	fn := func(src []string) {
		fmt.Println(src)
	}
	tmp := []string{}
	for i := 0; i < 10; i++ {
		tmp = append(tmp, fmt.Sprint(i))
	}

	taskPool := taskpool.NewTaskPool("", 10)
	size := 2
	s := 0
	lastIndex := len(tmp)
	for s < lastIndex {
		e := s + size
		if e > lastIndex {
			e = lastIndex
		}
		taskPool.Submit(func() {
			fn(tmp[s:e])
		})
		s = e
	}
	taskPool.SafeClose()

	// Output:
	// [0 1]
	// []
	// [4 5]
	// [6 7]
	// []
}

// CorrectSliceDemo 处理切片
func CorrectSliceDemo() {
	fmt.Println("CorrectSliceDemo start")
	defer fmt.Println("CorrectSliceDemo end")

	fn := func(src []string) {
		fmt.Println(src)
	}
	tmp := []string{}
	for i := 0; i < 10; i++ {
		tmp = append(tmp, fmt.Sprint(i))
	}

	taskPool := taskpool.NewTaskPool("", 10)
	size := 2
	s := 0
	lastIndex := len(tmp)
	for s < lastIndex {
		e := s + size
		if e > lastIndex {
			e = lastIndex
		}
		a := tmp[s:e]
		taskPool.Submit(func() {
			fn(a)
		})
		s = e
	}
	taskPool.SafeClose()

	// Output:
	// [0 1]
	// [4 5]
	// [2 3]
	// [6 7]
	// [8 9]
}

func ErrMapDemo() {
	type Tmp struct {
		No int
	}
	res := map[int]*Tmp{
		1: &Tmp{1},
		2: &Tmp{2},
		3: &Tmp{3},
		4: &Tmp{4},
	}
	taskPool := taskpool.NewTaskPool("", 10, taskpool.WithPoolPrint(false))
	for k, v := range res {
		taskPool.Submit(func() {
			fmt.Printf("key: %d, value: %d\n", k, v.No)
		})
	}
	taskPool.SafeClose()
	// Output:
	// key: 1, value: 1
	// key: 3, value: 3
	// key: 3, value: 3
	// key: 4, value: 4
}

func CorrectMapDemo() {
	type Tmp struct {
		No int
	}
	res := map[int]*Tmp{
		1: &Tmp{1},
		2: &Tmp{2},
		3: &Tmp{3},
		4: &Tmp{4},
	}
	taskPool := taskpool.NewTaskPool("", 10)
	for k, v := range res {
		k1, v1 := k, v
		taskPool.Submit(func() {
			fmt.Printf("key: %d, value: %d\n", k1, v1.No)
		})
	}
	taskPool.SafeClose()
	// Output:
	// key: 1, value: 1
	// key: 2, value: 2
	// key: 3, value: 3
	// key: 4, value: 4
}

func TimeOutDemo() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	taskPool := taskpool.NewTaskPool("", 4, taskpool.WithCtx(ctx))
	for k, v := range []string{"1", "2", "3", "4", "5"} {
		k1, v1 := k, v
		taskPool.Submit(func() {
			time.Sleep(5*time.Second)
			fmt.Printf("key: %d, value: %v\n", k1, v1)
		})
	}
	taskPool.SafeClose()
}

func main() {
	// ErrPageDemo()
	// CorrectPageDemo()

	// ErrSliceDemo()
	// CorrectSliceDemo()

	// ErrMapDemo()
	// CorrectMapDemo()
	TimeOutDemo()
}
