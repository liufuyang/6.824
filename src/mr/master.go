package mr

import (
	"log"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	// Your definitions here.
	nReduce     int
	mapTasks    []MapTask
	reduceTasks []ReduceTask
}

type TaskStatus int
const (
	Idle TaskStatus = 0
	InProgress TaskStatus = 1
	Done TaskStatus = 2
)

type MapTask struct {
	status TaskStatus
	timeStart time.Time
	file string
}

type ReduceTask struct {
	status TaskStatus
	timeStart time.Time
	ReduceFiles []string
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {

	// Do map tasks
	for X, mapTask := range m.mapTasks {
		if mapTask.status == Idle {
			reply.TaskType = MapTaskType
			reply.FileNumberX = X
			reply.InputFile = mapTask.file
			reply.NReduce = m.nReduce

			mapTask.timeStart = time.Now()
			mapTask.status = InProgress
			return nil
		}
	}

	// Do reduce tasks
	// Only when map task all done
	//for Y, reduceTask := range m.reduceTasks {
	//	if !reduceTask.done {
	//		reply.TaskType = ReduceTaskType
	//		reply.ReduceFiles = reduceTask?
	//		return nil
	//	}
	//}

	// reaching here means all done
	if m.Done() {
		reply.TaskType = EndTaskType
	} else {
		reply.TaskType = NoMapTaskType
	}
	return nil
}


func (m *Master) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	if args.TaskType == MapTaskType {
		m.mapTasks[args.FileNumberX].status = Done
		m.reduceTasks[args.NReduce].ReduceFiles = args.ReduceFiles
		reply.MoreTask = !m.Done()
	}

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	// Do map tasks
	for _, mapTask := range m.mapTasks {
		switch mapTask.status {
		case Done:
			continue
		case InProgress:
			if time.Now().After(mapTask.timeStart.Add(10*time.Second)) {
				mapTask.status = Idle
				return false
			}
		case Idle:
			return false
		}
	}

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		nReduce: nReduce,
	}

	var mapTasks []MapTask
	for _, file := range files {
		var task = MapTask{
			status: Idle,
			file: file,
		}
		mapTasks = append(mapTasks, task)
	}
	m.mapTasks = mapTasks

	var reduceTasks []ReduceTask
	for i := 0; i < nReduce; i++ {
		var task = ReduceTask{status: Idle}
		reduceTasks = append(reduceTasks, task)
	}
	m.reduceTasks = reduceTasks
	// Your code here.

	m.server()
	return &m
}

// -------------------------------- Example code below ------------------------------

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
