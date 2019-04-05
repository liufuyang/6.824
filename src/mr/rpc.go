package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"os"
)
import "strconv"

type TaskType int

const (
	MapTaskType     TaskType = 0
	ReduceTaskType  TaskType = 1
	WaitingTaskType TaskType = 2 // continue get reduce tasks
	EndTaskType     TaskType = 3 // can exit on this
)

func (e TaskType) String() string {
	switch e {
	case MapTaskType:
		return "MapTaskType"
	case ReduceTaskType:
		return "ReduceTaskType"
	case WaitingTaskType:
		return "WaitingTaskType"
	case EndTaskType:
		return "EndTaskType"
	default:
		return fmt.Sprintf("%d", int(e))
	}
}

type GetTaskArgs struct {
}

type GetTaskReply struct {
	TaskType TaskType

	// For map
	FileNumberX int
	InputFile   string
	NReduce     int

	// For reduce
	FileNumberY int
	ReduceFiles []string
}

// Finish task
type FinishTaskArgs struct {
	TaskType TaskType
	// for map
	FileNumberX int
	InputFile   string
	NReduce     int

	// for reduce
	FileNumberY int
	ReduceFiles []string
}

type FinishTaskReply struct {
	MoreTask bool
}

// ------------------------------ Examples below ----------------------------------------

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

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
