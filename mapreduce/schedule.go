package mapreduce

import (
	"fmt"
	"sync"
)

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files) // 有几个输入文件
		nios = mr.nReduce // 几个输出文件
	case reducePhase:
		ntasks = mr.nReduce // 输入文件 也就是map的输出
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//

	var wg sync.WaitGroup

	for i := 0;i<ntasks;i++{
		wg.Add(1)
		go func(taskNumber int, nios int, phase jobPhase){
			defer wg.Done()

			for{
				var args DoTaskArgs //需要使用这个参数进行RPC通信

				// 可以想象成一个就绪队列 代表master可以调度的worker的名称
				worker := <-mr.registerChannel //查看注册信息 这也是我们的工作之一
				//当一个worker启动时,它会给master发送Register　RPC
				args.File = mr.files[taskNumber] //文件名
				args.JobName = mr.jobName //job名
				args.NumOtherPhase = nios //输出的文件数 见串行执行的doMap第四个参数和doRecuce的第三个参数
				args.TaskNumber = taskNumber //第几个任务
				args.Phase = phase //到底执行map还是reduce

				//worker其实是一个服务器的名称
				ok := call(worker, "Worker.DoTask", &args, new(struct{}))
				if ok {
					go func() {
						// 任务执行完毕 再放回worker
						mr.registerChannel <- worker
					}()
					// 这里跑一个goroutine是必要的 因为channel不是缓冲channel,写入没有读取的话会阻塞,我们不能使得阻塞主线程
					break
				}
			}
		}(i, nios, phase)
	}
	wg.Wait()

	fmt.Printf("Schedule: %v phase done\n", phase)
}
