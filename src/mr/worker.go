package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type SortedKey []KeyValue

// Len 重写len,swap,less才能排序
func (k SortedKey) Len() int           { return len(k) }
func (k SortedKey) Swap(i, j int)      { k[i], k[j] = k[j], k[i] }
func (k SortedKey) Less(i, j int) bool { return k[i].Key < k[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	alive := true
	for alive {
		task := GetTask()
		// go默认每一个case会自动break
		switch task.TaskType {
		case MapTask:
			{
				// fmt.Print("*****************Map****************")
				DoMapTask(mapf, &task)
				CallDone(&task)
			}
		case ReduceTask:
			{
				// fmt.Print("*****************Reduce****************")
				DoReduceTask(reducef, &task)
				CallDone(&task)
			}
		case WaitTask:
			{
				// 目前所有的任务都有worker在做，所以现在就等着coordinator那边传done
				time.Sleep(time.Second * 3)
				// fmt.Println("all tasks in progress, waiting...")
			}
		case ExitTask:
			{
				time.Sleep(time.Second)
				// fmt.Println("All task done, will be exiting...")
				alive = false
			}

		}
	}
	time.Sleep(time.Second)
}

func DoMapTask(mapf func(string, string) []KeyValue, response *Task) {
	// fmt.Println("DoMapTask")
	var intermediate []KeyValue
	// 这里是一个只有第一个元素的string数组
	filename := response.FileSlice[0]
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	file.Close()
	intermediate = mapf(filename, string(content))

	// 把intermediate拆分到多个reduce中

	//initialize and loop over []KeyValue
	rn := response.ReducerNum
	// 创建一个长度为nReduce的二维切片
	HashedKV := make([][]KeyValue, rn)

	// 将intermediate中的键值对逐个加到二维切片中

	for _, kv := range intermediate {
		// 感觉这种插入写法很麻烦
		HashedKV[ihash(kv.Key)%rn] = append(HashedKV[ihash(kv.Key)%rn], kv)
	}

	for i := 0; i < rn; i++ {
		ofilename := "mr-tmp-" + strconv.Itoa(response.TaskId) + "-" + strconv.Itoa(i)
		ofile, _ := os.Create(ofilename)
		enc := json.NewEncoder(ofile)
		for _, kv := range HashedKV[i] {
			err := enc.Encode(kv)
			if err != nil {
				return
			}
		}
		ofile.Close()
	}

}
func DoReduceTask(reducef func(string, []string) string, response *Task) {
	// fmt.Println("DoReduceTask")
	// 感觉这里也可以不用taskid,用reduce的id也可以
	reduceOutputFileID := response.TaskId
	intermediate := readIntermediateAndSort(response.FileSlice)

	dir, _ := os.Getwd()
	// 创建临时文件,这里的命名后缀是*,即随机后缀,golang保证并行时这里得到的随机后缀不重复
	// 这是mapreduce论文3.3里提到的一个步骤,目的似乎保证数据只由一个reducetask最终产生,针对crash
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}

	// 来自mrsequential一模一样的写法
	// 遍历internediate，相同的key的Value堆在一个数组里，这也是map的工作范围
	// 然后把key和对应的数组扔进reduce function
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	tempFile.Close()
	// 在完全写入后进行重命名
	fn := fmt.Sprintf("mr-out-%d", reduceOutputFileID)
	err = os.Rename(tempFile.Name(), fn)
	if err !=nil{
		panic("Failed to rename temp file")
	}
}

// 把那些reduce后缀相同的intermediate文件都读进一个kv数组,按key排序之后返回
func readIntermediateAndSort(files []string) []KeyValue {
	var kva []KeyValue
	for _, filepath := range files {
		file, _ := os.Open(filepath)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	// KeyValue是自定义类型,所以要自己加一下排序规则
	sort.Sort(SortedKey(kva))
	return kva

}

func GetTask() Task {
	args := TaskArgs{}
	reply := Task{}

	ok := call("Coordinator.PollTask", &args, &reply)

	if ok {
		// fmt.Println("worker get ", reply.TaskType, "task :Id[", reply.TaskId, "]")
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply
}

func CallDone(task *Task) {
	args := task
	reply := Task{}

	ok := call("Coordinator.MarkFinished", &args, &reply)

	if ok {
		// fmt.Println("worker mark finished.")
	} else {
		fmt.Printf("call failed!\n")
	}

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		// fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
