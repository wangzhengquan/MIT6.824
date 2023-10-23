package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type TaskState int

const (
	WAITTING TaskState = 0
	ASSIGNED TaskState = 1
	FINISHED TaskState = 2
)

type Coordinator struct {
	// Your definitions here.
	mutex    sync.Mutex
	cond     *sync.Cond
	MapTasks []*MapTask

	// reduceTasksMutex sync.Mutex
	// reduceTasksCond  *sync.Cond
	ReduceTasks []*ReduceTask
}

type MapTask struct {
	Id       int
	FileName string
	NReduce  int
	State    TaskState
}

type ReduceTask struct {
	Id    int
	NMap  int
	State TaskState
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) FetchTask(args *FetchTaskArgs, reply *FetchTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for _, task := range c.MapTasks {
		// fmt.Printf("task=%v", task)
		if task.State == WAITTING {
			reply.MapTask = task
			task.State = ASSIGNED

			return nil
		}
	}

	for !c.MapTasksDone() {
		c.cond.Wait()
	}

	for _, task := range c.ReduceTasks {
		if task.State == WAITTING {
			reply.ReduceTask = task
			task.State = ASSIGNED
			return nil
		}
	}

	// for !c.Done() {
	// 	c.reduceTasksCond.Wait()
	// }
	reply.Done = true

	return nil
}

func (c *Coordinator) MapTaskFinished(args *TaskFinishedArgs, reply *TaskFinishedReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.MapTasks[args.TaskId].State = FINISHED
	if c.MapTasksDone() {
		c.cond.Broadcast()
	}
	return nil
}

func (c *Coordinator) ReduceTaskFinished(args *TaskFinishedArgs, reply *TaskFinishedReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.ReduceTasks[args.TaskId].State = FINISHED
	// if c.Done() {
	// 	c.reduceTasksCond.Broadcast()
	// }
	return nil
}

// func (c *Coordinator) ReduceTasksMutex() *sync.Mutex {
// 	return &c.reduceTasksMutex
// }
// func (c *Coordinator) ReduceTasksCond() *sync.Cond {
// 	return c.reduceTasksCond
// }

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

func (c *Coordinator) MapTasksDone() bool {
	for _, task := range c.MapTasks {
		if task.State != FINISHED {
			return false
		}
	}
	return true
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.
	for _, task := range c.ReduceTasks {
		if task.State != FINISHED {
			return false
		}
	}

	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.cond = sync.NewCond(&c.mutex)
	// c.reduceTasksCond = sync.NewCond(&c.reduceTasksMutex)
	// Your code here.
	c.MapTasks = make([]*MapTask, 0)
	for i, filename := range files {
		task := &MapTask{
			Id:       i,
			FileName: filename,
			NReduce:  nReduce,
			State:    WAITTING,
		}
		c.MapTasks = append(c.MapTasks, task)
	}

	c.ReduceTasks = make([]*ReduceTask, 0)
	for i := 0; i < nReduce; i++ {
		task := &ReduceTask{
			Id:    i,
			NMap:  len(files),
			State: WAITTING,
		}
		c.ReduceTasks = append(c.ReduceTasks, task)
	}

	c.server()

	return &c
}
