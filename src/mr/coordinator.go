package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type TaskState int

const (
	WAITTING TaskState = 0
	ASSIGNED TaskState = 1
	FINISHED TaskState = 2
)

type Coordinator struct {
	// Your definitions here.
	MapTasks    []*MapTask
	ReduceTasks []*ReduceTask
}

type MapTask struct {
	Id    int
	File  string
	State TaskState
}

type ReduceTask struct {
	Id    int
	State TaskState
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignTask(args *FetchTaskArgs, reply *FetchTaskReply) error {
	for _, task := range c.MapTasks {
		// fmt.Printf("task=%v", task)
		if task.State == WAITTING {
			reply.MapTask = task
			task.State = ASSIGNED
			return nil
		}
	}

	for _, task := range c.ReduceTasks {
		if task.State == WAITTING {
			reply.ReduceTask = task
			task.State = ASSIGNED
			return nil
		}
	}
	reply.Done = true
	return nil
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
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
	c.MapTasks = make([]*MapTask, 0)
	for i, file := range files {
		task := &MapTask{
			Id:    i,
			File:  file,
			State: WAITTING,
		}
		c.MapTasks = append(c.MapTasks, task)
	}

	c.ReduceTasks = make([]*ReduceTask, 0)
	for i := 0; i < nReduce; i++ {
		task := &ReduceTask{
			Id:    i,
			State: WAITTING,
		}
		c.ReduceTasks = append(c.ReduceTasks, task)
	}

	c.server()

	return &c
}
