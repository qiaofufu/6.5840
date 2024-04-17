package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	nReduce      int
	mapTask      []Task
	totalMapTask int
	mapFinished  int
	reduceTask   []Task
	reduceFinish int
	runningTask  []Task
	done         bool
	mu           sync.Mutex
	intermediate map[int][]string
}

// Your code here -- RPC handlers for the worker to call.

type Task struct {
	TaskType  int
	TaskId    int
	FileName  []string
	StartTime time.Time
}

func (c *Coordinator) AskTask(req *AskTaskArgs, resp *AskTaskReply) error {
	var task Task
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.mapTask) != 0 {
		task = c.mapTask[0]
		c.mapTask = c.mapTask[1:]
	} else if len(c.reduceTask) != 0 {
		task = c.reduceTask[0]
		c.reduceTask = c.reduceTask[1:]
	} else if len(c.runningTask) == 0 {
		resp.TaskType = 2
		return nil
	} else {
		resp.TaskType = 3
		return nil
	}
	task.StartTime = time.Now()
	resp.TaskFile = task.FileName
	resp.TaskId = task.TaskId
	resp.TaskType = task.TaskType
	resp.NReduce = c.nReduce
	c.runningTask = append(c.runningTask, task)

	return nil
}

func (c *Coordinator) FinishTask(req *FinishTaskArgs, resp *FinishTaskReply) error {
	switch req.TaskType {
	case 0:
		c.mapFinished++
		for k, v := range req.Intermediates {
			c.mu.Lock()
			c.intermediate[k] = append(c.intermediate[k], v)
			c.mu.Unlock()
		}
		// move task from running
		c.mu.Lock()
		for i, task := range c.runningTask {
			if task.TaskId == req.TaskId {
				c.runningTask = append(c.runningTask[:i], c.runningTask[i+1:]...)
				break
			}
		}
		if c.mapFinished == c.totalMapTask {
			c.reduceTask = make([]Task, c.nReduce)
			for i := 0; i < c.nReduce; i++ {
				c.reduceTask[i] = Task{TaskType: 1, TaskId: i, FileName: c.intermediate[i]}
			}
		}
		c.mu.Unlock()
	case 1:
		c.reduceFinish++
		if c.reduceFinish == c.nReduce {
			c.done = true
		}
	}

	return nil
}

func (c *Coordinator) Check() {
	go func() {
		for {
			time.Sleep(5 * time.Second)
			removeTask := make([]Task, 0)
			c.mu.Lock()
			for _, task := range c.runningTask {
				if time.Now().Sub(task.StartTime).Seconds() > 10 {
					switch task.TaskType {
					case 0:
						c.mapTask = append(c.mapTask, task)
					case 1:
						c.reduceTask = append(c.reduceTask, task)
					}
					removeTask = append(removeTask, task)
				}
			}
			for _, task := range removeTask {
				for i, t := range c.runningTask {
					if t.TaskId == task.TaskId {
						c.runningTask = append(c.runningTask[:i], c.runningTask[i+1:]...)
						break
					}
				}
			}
			c.mu.Unlock()
		}

	}()
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {

	// Your code here.

	return c.done
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nReduce = nReduce
	c.totalMapTask = len(files)
	for i, file := range files {
		//fpath, err := filepath.Abs(file)
		//if err != nil {
		//	fmt.Println("file path error")
		//	return nil
		//}
		c.mapTask = append(c.mapTask, Task{TaskId: i, FileName: []string{
			file,
		}})
	}
	c.intermediate = make(map[int][]string)
	c.Check()
	c.server()
	return &c
}
