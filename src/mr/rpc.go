package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	// "strings"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type TaskArgs struct{}
type Task struct {
	TaskType TaskType
	TaskId   int

	// for reduce
	ReducerNum int

	// for map
	FileSlice []string
}

type TaskType int

// 第一行iota就是0的意思，后面的值自增
const (
	MapTask TaskType = iota
	ReduceTask
	WaitTask
	ExitTask
)

// Phase 对于分配任务阶段的父类型
type Phase int

// State 任务的状态的父类型
type TaskState int

const (
	MapPhase    Phase = iota // 此阶段在分发MapTask
	ReducePhase              // 此阶段在分发ReduceTask
	AllDone                  // 此阶段已完成
)

const (
	Working TaskState = iota
	Waiting
	Done
)

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
