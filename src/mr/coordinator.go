package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskState int

const (
	WAITTING TaskState = 0
	STARTED  TaskState = 1
	FINISHED TaskState = 2
)

type Coordinator struct {
	// Your definitions here.
	Mutex        sync.Mutex
	Cond         *sync.Cond
	mapTasksDone bool
	done         bool
	MapTasks     []*MapTask
	ReduceTasks  []*ReduceTask
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
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	for {
		if c.MapTasksDone() {
			break
		} else if task := c.fetchMapTask(); task != nil {
			reply.MapTask = task
			c.mapTaskStarted(task)
			return nil
		} else {
			c.Cond.Wait()
		}
	}

	for {
		if c.Done() {
			reply.Done = true
			break
		} else if task := c.fetchReduceTask(); task != nil {
			reply.ReduceTask = task
			c.reduceTaskStarted(task)
			return nil
		} else {
			c.Cond.Wait()
		}
	}

	return nil
}

func (c *Coordinator) fetchMapTask() *MapTask {
	for _, task := range c.MapTasks {
		if task.State == WAITTING {
			return task
		}
	}
	return nil
}

func (c *Coordinator) fetchReduceTask() *ReduceTask {
	for _, task := range c.ReduceTasks {
		if task.State == WAITTING {
			return task
		}
	}
	return nil
}

func (c *Coordinator) mapTaskStarted(task *MapTask) {
	task.State = STARTED
	// recovery function
	go func(task *MapTask) {
		timedue := time.After(10 * time.Second)
		<-timedue
		c.Mutex.Lock()
		defer c.Mutex.Unlock()
		if task.State != FINISHED {
			log.Printf("recover map task %d \n", task.Id)
			task.State = WAITTING
			c.Cond.Broadcast()
		}
	}(task)
}

func (c *Coordinator) reduceTaskStarted(task *ReduceTask) {
	task.State = STARTED
	// recovery function
	go func(task *ReduceTask) {
		timedue := time.After(10 * time.Second)
		<-timedue
		c.Mutex.Lock()
		defer c.Mutex.Unlock()
		if task.State != FINISHED {
			log.Printf("recover reduce task %d \n", task.Id)
			task.State = WAITTING
			c.Cond.Broadcast()
		}
	}(task)

}

func (c *Coordinator) MapTaskFinished(args *TaskFinishedArgs, reply *TaskFinishedReply) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	c.MapTasks[args.TaskId].State = FINISHED
	if c.MapTasksDone() {
		c.Cond.Broadcast()
	}
	return nil
}

func (c *Coordinator) ReduceTaskFinished(args *TaskFinishedArgs, reply *TaskFinishedReply) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	c.ReduceTasks[args.TaskId].State = FINISHED
	if c.Done() {
		c.Cond.Broadcast()
	}
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

func (c *Coordinator) MapTasksDone() bool {
	if c.mapTasksDone {
		return true
	}
	for _, task := range c.MapTasks {
		if task.State != FINISHED {
			return false
		}
	}
	c.mapTasksDone = true
	return true
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.
	if c.done {
		return true
	}
	for _, task := range c.ReduceTasks {
		if task.State != FINISHED {
			return false
		}
	}
	c.done = true
	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.Cond = sync.NewCond(&c.Mutex)
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
