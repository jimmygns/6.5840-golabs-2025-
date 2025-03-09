package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
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
type MapTaskArgs struct {
	WorkerPID int
}

type MapTaskReply struct {
	TaskID   int
	FileName string
	NReduce  int
}

type ReduceTaskArgs struct {
	WorkerPID int
}

type ReduceTaskReply struct {
	TaskID    int
	FileNames []string
}

type RegisterReducerTaskArgs struct {
	WorkerPID int
	TaskID    int
	Filename  string
}

type RegisterReducerTaskReply struct {
}

type TaskType string

const (
	Map    TaskType = "Map"
	Reduce TaskType = "Reduce"
)

type UpdateTaskArgs struct {
	TaskID   int
	Status   TaskStatus
	TaskType TaskType
}

type UpdateTaskReply struct {
}

// Enum for task status
type TaskStatus int

const (
	Started TaskStatus = iota
	Created
	Completed
)

type Task struct {
	TaskID    int
	WorkerPID int
	Status    TaskStatus
	StartedAt time.Time
}

type MapTask struct {
	Task
	FileName string
}

type ReduceTask struct {
	Task
	FileNames []string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
