package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

type TaskType int
const (
	MapTaskType TaskType = 0
	ReduceTaskType TaskType = 1
	NoMapTaskType TaskType = 2 // continue get reduce tasks
	EndTaskType  TaskType = 3 // can exit on this
)

type GetTaskArgs struct {

}

type GetTaskReply struct {
	TaskType TaskType

	// For map
	FileNumberX int
	InputFile string
	NReduce int

	// For reduce
	ReduceFiles []string
}

// Finish task
type FinishTaskArgs struct {
	TaskType TaskType
	// for map
	FileNumberX int
	InputFile string
	NReduce int
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
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
