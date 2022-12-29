#### 1. 介绍

* 支持预分配协程和用多少申请多少 
* 分配的协程都有一个生命周期，生命周期到了会被自动回收 
* 协程池最少有一个哨兵协程，最多有 maxWork + 1(哨兵)
* 如果要关闭协程池需要手动释放, 异常情况时会自动释放


#### 2. 使用

```go
go get -u gitee.com/xuesongtao/taskpool
```

```go
    pushPool := lib.NewTaskPool("poolName", 10, lib.WithProGoWorker())
    defer pushPool.SafeClose() // 局部使用需要使用这个进行 Close()

    everyTaskHandleSum := 500
    l := 50000
    pushIds := []string{xxx}
    totalPage := math.Ceil(float64(l) / float64(everyTaskHandleSum))
    for page := 1; page <= int(totalPage); page++ {
        // 根据切片分页
        startIndex, endIndex := lib.GetPapeSliceIndex(int32(page), int32(everyTaskHandleSum), l)
        tmpArr := pushIds[startIndex:endIndex]
        i.log.Infof("startIndex: %d, endIndex: %d, total: %d", startIndex, endIndex, l)

        // 阻塞式
        pushPool.Submit(func() {
            i.PushBatchMsg(tmpArr, info)
        })
    }
```

- 其他: 在使用时需要注意闭包引用问题, 在 `_example` 下添加使用示例

#### 最后

* 欢迎大佬们指正, 希望大佬给 **star**
