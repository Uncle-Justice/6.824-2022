package mr

import (
	"fmt"
	"io/ioutil"
	"strconv"
	"strings"
	"sync"

	// "go/printer"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

var (
	mu sync.Mutex
)

type Coordinator struct {
	ReducerNum        int        // 传入的参数决定需要多少个reducer
	files             []string   // 传入的文件数组
	TaskId            int        // 就是一个全局变量，因为我要给每一个worker一个唯一的id，这里就存一个起点，然后自增就行
	DistPhase         Phase      // 目前整个框架应该处于什么任务阶段
	ReduceTaskChannel chan *Task // 使用chan保证并发安全
	MapTaskChannel    chan *Task // 使用chan保证并发安全

	// 因为想先创建好所有任务，再启动服务器，所以要写个东西存一下，这里没有把map和reduce的任务区分开来，感觉分开也行
	// 不过我看两个不同的讲解都是写在一起了
	taskMetaHolder TaskMetaHolder
}

// TaskMetaInfo 保存任务的元数据
type TaskMetaInfo struct {
	taskState TaskState // 任务的状态
	StartTime time.Time // 任务的开始时间，为crash做准备
	TaskAdr   *Task     // 传入任务的指针,为的是这个任务从通道中取出来后，还能通过地址标记这个任务已经完成
}

// TaskMetaHolder 保存全部任务的元数据
// 是TaskId到TaskMetaInfo*的映射
type TaskMetaHolder struct {
	MetaMap map[int]*TaskMetaInfo // 通过下标hash快速定位
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) PollTask(args *TaskArgs, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()
	switch c.DistPhase {
	case MapPhase:
		{
			// 从map任务管道里去拿之前创建好的任务的指针，如果管道里已经空了，就说明Map任务已经全部分配出去了（和map任务全部完成不一样

			if len(c.MapTaskChannel) > 0 {
				*reply = *<-c.MapTaskChannel

				// 这里看别人额外做了个判断，如果这个task不是waiting状态要补一个输出，暂时没搞懂为什么要这样做
				// 感觉可以直接改working就完事了
				c.taskMetaHolder.MetaMap[reply.TaskId].taskState = Working
				c.taskMetaHolder.MetaMap[reply.TaskId].StartTime = time.Now()
			} else {
				// 如果管道已经空了，就返回一个Waiting的Task
				// 并且判断是不是所有map任务都结束了，如果确实全部做完了，就要切到下一个phase

				reply.TaskType = WaitTask
				if c.taskMetaHolder.checkAllTaskDone() {
					fmt.Println("to reduce phase")
					c.toNextPhase()
				}
				return nil
			}
		}
	case ReducePhase:
		{
			if len(c.ReduceTaskChannel) > 0 {
				*reply = *<-c.ReduceTaskChannel
				c.taskMetaHolder.MetaMap[reply.TaskId].taskState = Working
				c.taskMetaHolder.MetaMap[reply.TaskId].StartTime = time.Now()
			} else {
				// 如果管道已经空了，就返回一个Waiting的Task
				// 并且判断是不是所有map任务都结束了，如果确实全部做完了，就要切到下一个phase

				reply.TaskType = WaitTask
				if c.taskMetaHolder.checkAllTaskDone() {
					c.toNextPhase()
				}
				return nil
			}
		}
	case AllDone:
		{
			reply.TaskType = ExitTask
		}
	default:
		{
			panic("undefined phase")
		}
	}
	return nil
}

func (c *Coordinator) MarkFinished(args *Task, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()
	switch c.DistPhase {
	case MapPhase:
		{
			// 防止重复的worker返回同一个task，所以不能直接把哈希的对应TaskType改成Done，而是要先查，判断一下
			meta, ok := c.taskMetaHolder.MetaMap[args.TaskId]
			if ok && meta.taskState == Working {
				// fmt.Println("done")
				meta.taskState = Done
			} else {
				// fmt.Printf("duplicate map worker done in task Id[%v]", args.TaskId)
			}
		}
	case ReducePhase:
		{
			// 防止重复的worker返回同一个task，所以不能直接把哈希的对应TaskType改成Done，而是要先查，判断一下
			meta, ok := c.taskMetaHolder.MetaMap[args.TaskId]
			if ok && meta.taskState == Working {
				meta.taskState = Done
			} else {

				// fmt.Printf("duplicate reduce worker done in task Id[%v]", args.TaskId)
			}
		}
	case AllDone:{
		fmt.Printf("duplicate worker done in task Id[%v]", args.TaskId)
	}
	default:
		panic("The task type undefined ! ! !")
	}

	// fmt.Printf("task %v finished\n", args.TaskId)
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	mu.Lock()
	defer mu.Unlock()
	// Your code here.
	// 检查所有任务是否完成
	if c.DistPhase == AllDone {
		fmt.Printf("All tasks are finished, the coordinator will be exit! !")
		return true
	} else {
		return false
	}

}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		ReducerNum:        nReduce,  // 传入的参数决定需要多少个reducer
		files:             files,    // 传入的文件数组
		TaskId:            0,        // 就是一个全局变量，因为我要给每一个worker一个唯一的id，这里就存一个起点，然后自增就行
		DistPhase:         MapPhase, // 目前整个框架应该处于什么任务阶段
		MapTaskChannel:    make(chan *Task, len(files)),
		ReduceTaskChannel: make(chan *Task, nReduce),
		// Holder需要做初始化
		taskMetaHolder: TaskMetaHolder{
			make(map[int]*TaskMetaInfo, len(files)+nReduce),
		},
	}

	// Your code here.
	// 所有map任务的初始化，reduce任务的初始化留到所有Map任务完成后再去做
	c.MakeMapTasks()

	c.server()

	go c.CrashDetector()
	fmt.Println("server started")
	return &c
}

func (c *Coordinator) MakeMapTasks() {
	// for range这种格式，如果我for两个变量，前一个_就是下标，后一个就是元素
	for _, v := range c.files {
		id := c.GenerateTaskId()
		task := Task{
			TaskType:   MapTask,
			TaskId:     id,
			ReducerNum: c.ReducerNum,
			// 切片的赋值要按这个格式写
			FileSlice: []string{v},
		}
		taskMetaInfo := TaskMetaInfo{
			taskState: Waiting,
			TaskAdr:   &task,
		}
		c.taskMetaHolder.acceptMeta(&taskMetaInfo)
		c.MapTaskChannel <- &task
		// 把这个task扔到channel里去
	}
}

func (c *Coordinator) MakeReduceTasks() {
	// for range这种格式，如果我for两个变量，前一个_就是下标，后一个就是元素

	// 和MakeMapTasks不同,这里写进Task的文件名是特定reduceNum为后缀的所有文件名集合
	for i := 0; i < c.ReducerNum; i++ {
		id := c.GenerateTaskId()
		task := Task{
			TaskId:    id,
			TaskType:  ReduceTask,
			FileSlice: selectReduceName(i),
		}
		taskMetaInfo := TaskMetaInfo{
			taskState: Waiting,
			TaskAdr:   &task,
		}

		c.taskMetaHolder.acceptMeta(&taskMetaInfo)
		c.ReduceTaskChannel <- &task
		// 把这个task扔到channel里去
	}
}
func selectReduceName(reduceNum int) []string {
	var s []string
	path, _ := os.Getwd()
	files, _ := ioutil.ReadDir(path)
	for _, fi := range files {
		// 匹配对应的reduce文件
		if strings.HasPrefix(fi.Name(), "mr-tmp") && strings.HasSuffix(fi.Name(), strconv.Itoa(reduceNum)) {
			s = append(s, fi.Name())
		}
	}
	return s
}

func (c *Coordinator) GenerateTaskId() int {
	c.TaskId++
	return c.TaskId
}

func (c *Coordinator) toNextPhase() {

	if c.DistPhase == MapPhase {
		c.MakeReduceTasks()
		c.DistPhase = ReducePhase
	} else if c.DistPhase == ReducePhase {
		c.DistPhase = AllDone
	}
}

func (t *TaskMetaHolder) acceptMeta(TaskInfo *TaskMetaInfo) bool {
	taskId := TaskInfo.TaskAdr.TaskId

	// map的查询操作中，最多可以给两个变量赋值，第一个为查找到的值，第二个为bool
	// 如果没查到，第一个为相应类型的0值，如果只指定一个变量，那么该变量仅表示查到的值
	meta := t.MetaMap[taskId]
	if meta != nil {
		return false
	} else {
		t.MetaMap[taskId] = TaskInfo
	}

	return true
}

// MapTask全部Done了或者ReduceTask全部done就返回true，其他情况返回false
func (t *TaskMetaHolder) checkAllTaskDone() bool {

	// 这里用不到k，如果写for k, v会报错，就要把k改成下划线
	mapDoneNum := 0
	mapUndoneNum := 0
	reduceDoneNum := 0
	reduceUndoneNum := 0
	for _, v := range t.MetaMap {
		if v.TaskAdr.TaskType == MapTask {
			if v.taskState == Done {
				mapDoneNum++
			} else {
				mapUndoneNum++
			}
		} else if v.TaskAdr.TaskType == ReduceTask {
			if v.taskState == Done {
				reduceDoneNum++
			} else {
				reduceUndoneNum++
			}
		}

	}
	if (mapDoneNum > 0 && mapUndoneNum == 0) && (reduceDoneNum == 0 && reduceUndoneNum == 0) {
		fmt.Println("map tasks all done")
		return true
	} else if reduceDoneNum > 0 && reduceUndoneNum == 0 {
		return true
	} else {
		fmt.Println("map tasks not all done")
		return false
	}

}

func (c *Coordinator) CrashDetector() {

	// 相当于while(true)， 但是写for(true)是非法的
	for {
		time.Sleep(time.Second * 2)
		mu.Lock()
		if c.DistPhase == AllDone {
			mu.Unlock()
			break
		}

		for _, v := range c.taskMetaHolder.MetaMap {
			if v.taskState == Working && time.Since(v.StartTime) > 15*time.Second {
				fmt.Println("crash detected")
				switch v.TaskAdr.TaskType {
				case MapTask:
					c.MapTaskChannel <- v.TaskAdr
					v.taskState = Waiting
				case ReduceTask:
					c.ReduceTaskChannel <- v.TaskAdr
					v.taskState = Waiting
				}

			}
		}
		mu.Unlock()
	}

}
