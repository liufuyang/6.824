package mr

import (
	"log"
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

	allDone bool
}

type MapTask struct {
	done bool
	file string
}

type ReduceTask struct {
	done bool
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {

	// Do map tasks
	for X, mapTask := range m.mapTasks {
		if !mapTask.done {
			reply.TaskType = MapTaskType
			reply.FileNumberX = X
			reply.InputFile = mapTask.file
			return nil
		}
	}

	// Do reduce tasks
	//for Y, reduceTask := range m.reduceTasks {
	//	if !reduceTask.done {
	//		reply.TaskType = ReduceTaskType
	//		reply.ReduceFiles = reduceTask?
	//		return nil
	//	}
	//}

	// reaching here means all done
	reply.TaskType = NoTaskType

	m.allDone = true
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
		allDone: false,
	}

	var mapTasks []MapTask
	for _, file := range files {
		var task = MapTask{
			done: false,
			file: file,
		}
		mapTasks = append(mapTasks, task)
	}
	m.mapTasks = mapTasks

	var reduceTasks []ReduceTask
	for i := 0; i < nReduce; i++ {
		var task = ReduceTask{done: false}
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
