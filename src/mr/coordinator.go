package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"slices"
	"sync"
	"time"
)

type WorkerStatus int

// Define the possible values for WorkerStatus
const (
	Idle WorkerStatus = iota
	Working
	Finished

	taskTimeout = 10
)

type Coordinator struct {
	nReduce int

	lock sync.RWMutex

	mapTasks    map[int]*MapTask
	reduceTasks map[int]*ReduceTask

	unfinishedTasks int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) RegisterReducerTask(args *RegisterReducerTaskArgs, reply *RegisterReducerTaskReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	task, ok := c.reduceTasks[args.TaskID]
	if !ok {
		task = &ReduceTask{
			Task: Task{
				TaskID: args.TaskID,
				Status: Created,
			},
			FileNames: []string{args.Filename},
		}
		c.reduceTasks[args.TaskID] = task
	} else {
		if !slices.Contains(task.FileNames, args.Filename) {
			task.FileNames = append(task.FileNames, args.Filename)
		}
	}
	// fmt.Printf("Reducer Task %d: %v\n", args.TaskID, task)
	return nil
}

func (c *Coordinator) UpdateTask(args *UpdateTaskArgs, reply *UpdateTaskReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if args.TaskType == Map {
		c.mapTasks[args.TaskID].Status = args.Status
	} else {
		c.reduceTasks[args.TaskID].Status = args.Status
		if args.Status == Completed {
			for _, fn := range c.reduceTasks[args.TaskID].FileNames {
				os.Remove(fn)
			}
		}
	}
	return nil
}

func (c *Coordinator) MapTask(args *MapTaskArgs, reply *MapTaskReply) error {
	// Your code here.
	c.lock.Lock()
	defer c.lock.Unlock()
	for _, task := range c.mapTasks {
		if task.Status == Created {
			// Set the task to started
			task.StartedAt = time.Now()
			task.Status = Started
			task.WorkerPID = args.WorkerPID

			// Set the reply
			reply.FileName = task.FileName
			reply.NReduce = c.nReduce
			reply.TaskID = task.TaskID
			return nil
		}
	}
	return nil
}

func (c *Coordinator) ReduceTask(args *ReduceTaskArgs, reply *ReduceTaskReply) error {
	c.lock.RLock()
	// wait for all map tasks to be completed
	for _, task := range c.mapTasks {
		if task.Status != Completed {
			c.lock.RUnlock()
			return nil
		}
	}
	c.lock.RUnlock()
	c.lock.Lock()
	defer c.lock.Unlock()
	for _, task := range c.reduceTasks {
		if task.Status == Created {
			// Set the task to started
			task.StartedAt = time.Now()
			task.Status = Started
			task.WorkerPID = args.WorkerPID

			// Set the reply
			reply.FileNames = task.FileNames
			reply.TaskID = task.TaskID
			return nil
		}
	}
	return nil
}

func (c *Coordinator) ScanTasks() {
	for {
		c.lock.RLock()
		unfinishedTasks := 0
		for _, task := range c.mapTasks {
			if task.Status != Completed {
				unfinishedTasks++
			}
			if task.Status == Started && time.Since(task.StartedAt).Seconds() > taskTimeout {
				task.Status = Created
			}
		}
		for _, task := range c.reduceTasks {
			if task.Status != Completed {
				unfinishedTasks++
			}
			if task.Status == Started && time.Since(task.StartedAt).Seconds() > taskTimeout {
				task.Status = Created
			}
		}
		c.lock.RUnlock()
		c.unfinishedTasks = unfinishedTasks
		time.Sleep(5 * time.Second)
	}
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
	// fmt.Printf("unfinishedTasks: %d\n", c.unfinishedTasks)
	return c.unfinishedTasks == 0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	mapTasks := make(map[int]*MapTask, len(files))
	reduceTasks := make(map[int]*ReduceTask, nReduce)
	for i, file := range files {
		mapTasks[i] = &MapTask{
			Task: Task{
				TaskID: i,
				Status: Created,
			},
			FileName: file,
		}
	}
	c := Coordinator{
		nReduce: nReduce,

		mapTasks:    mapTasks,
		reduceTasks: reduceTasks,

		unfinishedTasks: len(files),
	}

	// Your code here.
	go c.ScanTasks()

	c.server()
	return &c
}
