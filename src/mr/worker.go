package mr

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		task := CallAskTask()
		//fmt.Printf("task: %v\n", task)
		if task.TaskType == 2 {
			break
		}
		if task.TaskType == 3 {
			time.Sleep(time.Second * 1)
			continue
		}
		switch task.TaskType {
		case 0:
			file, err := os.OpenFile(task.TaskFile[0], os.O_RDONLY, 0755)
			if err != nil {
				dir, _ := os.Getwd()
				fmt.Printf("open file failed: %v work dir: %v\n", err, dir)
				return
			}
			content, err := io.ReadAll(file)
			values := mapf(task.TaskFile[0], string(content))
			fileMap := make(map[int]*os.File)
			files := make(map[int]string)
			for _, result := range values {
				reduceTaskID := ihash(result.Key) % task.NReduce
				if _, ok := fileMap[reduceTaskID]; !ok {
					reduceTaskFile := fmt.Sprintf("mr-%v-%v.txt", task.TaskId, reduceTaskID)
					files[reduceTaskID] = reduceTaskFile
					reduceFile, err := os.OpenFile(reduceTaskFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0755)
					if err != nil {
						dir, _ := os.Getwd()
						fmt.Printf("open file failed: %v work dir: %v\n", err, dir)
						return
					}
					fileMap[reduceTaskID] = reduceFile
				}
				f := fileMap[reduceTaskID]

				_, err := f.WriteString(fmt.Sprintf("%v %v\n", result.Key, result.Value))
				if err != nil {
					fmt.Printf("write file failed: %v\n", err)
					return
				}
			}

			for _, f := range fileMap {
				_ = f.Close()
			}

			CallFinishTask(task.TaskId, files)
		case 1:
			kva := make(map[string][]string)
			for _, f := range task.TaskFile {
				file, err := os.OpenFile(f, os.O_RDONLY, 0755)
				if err != nil {
					dir, _ := os.Getwd()
					fmt.Printf("open file failed: %v work dir: %v\n", err, dir)
					return
				}
				scanner := bufio.NewScanner(file)
				for scanner.Scan() {
					line := scanner.Text()
					kv := strings.Split(line, " ")
					if len(kv) != 2 {
						fmt.Printf("invalid line: %v\n", line)
						continue
					}
					kva[kv[0]] = append(kva[kv[0]], kv[1])
				}
			}
			keys := make([]string, 0, len(kva))
			for k := range kva {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			reduceTaskFile := fmt.Sprintf("mr-out-%v.txt", task.TaskId)
			reduceFile, err := os.OpenFile(reduceTaskFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0755)
			if err != nil {
				fmt.Printf("open file failed: %v\n", err)
				return
			}
			for _, k := range keys {
				v := reducef(k, kva[k])
				_, err := reduceFile.WriteString(fmt.Sprintf("%v %v\n", k, v))
				if err != nil {
					fmt.Printf("write file failed: %v\n", err)
					return
				}
			}

			CallFinishTask(task.TaskId, nil)
		case 2:
			return
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

func CallFinishTask(taskId int, files map[int]string) {
	args := FinishTaskArgs{
		TaskId:        taskId,
		Intermediates: files,
	}
	reply := FinishTaskReply{}
	ok := call("Coordinator.FinishTask", &args, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
	}
}

func CallAskTask() AskTaskReply {
	args := AskTaskArgs{}
	reply := AskTaskReply{}
	ok := call("Coordinator.AskTask", &args, &reply)
	if ok {
		return reply
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
