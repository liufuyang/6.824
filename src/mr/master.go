package mr

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	// Your definitions here.
	mutex sync.Mutex

	nReduce     int
	mapTasks    []MapTask
	reduceTasks []ReduceTask
	reduceFiles map[string][]string
}

type TaskStatus int

const (
	Idle       TaskStatus = 0
	InProgress TaskStatus = 1
	Done       TaskStatus = 2
)

type MapTask struct {
	status    TaskStatus
	timeStart time.Time
	file      string
}

type ReduceTask struct {
	status    TaskStatus
	timeStart time.Time
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {

	m.mutex.Lock()
	defer m.mutex.Unlock()

	// Do map tasks
	for X, _ := range m.mapTasks {
		mapTask := &m.mapTasks[X] // get reference instead of copy!

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

	// Let worker wait for all map task done
	if !m.allMapDone() {
		reply.TaskType = NoMapTaskType
		return nil
	}

	// Do reduce tasks
	// Only when map task all done

	for Y, _ := range m.reduceTasks {
		reduceTask := &m.reduceTasks[Y] // get reference instead of copy!

		if reduceTask.status == Idle {
			reply.TaskType = ReduceTaskType
			reply.FileNumberY = Y
			reply.ReduceFiles = m.reduceFiles[strconv.Itoa(Y)]

			reduceTask.timeStart = time.Now()
			reduceTask.status = InProgress

			// TODO - test output
			for Y, _ := range m.reduceFiles{
				fmt.Printf("Test output using reduce files Y:%v len:%v  %v \n",
					Y, len(m.reduceFiles[Y]), m.reduceFiles[Y])
			}

			return nil
		}
	}

	// reaching here means all done
	if m.Done() {
		reply.TaskType = EndTaskType
	} else {
		reply.TaskType = NoMapTaskType
	}
	return nil

}

func (m *Master) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	switch args.TaskType {
	case MapTaskType:
		{
			m.mapTasks[args.FileNumberX].status = Done
			for _, reduceFile := range args.ReduceFiles {
				parts := strings.Split(reduceFile, "-")
				Y := parts[len(parts)-1]
				partition := m.reduceFiles[Y]
				partition = append(partition, reduceFile)
				m.reduceFiles[Y] = partition
			}

			// TODO - test output
			for Y, _ := range m.reduceFiles{
				fmt.Printf("Test output creating reduce files Y:%v len:%v  %v \n",
					Y, len(m.reduceFiles[Y]), m.reduceFiles[Y])
			}

			reply.MoreTask = !m.Done()
		}
	case ReduceTaskType:
		{
			m.reduceTasks[args.FileNumberY].status = Done
			reply.MoreTask = !m.Done()
		}
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

	// Your code here.
	// Do map tasks
	if !m.allMapDone() {
		return false
	}

	// check if all reduce is done
	for Y, _ := range m.reduceTasks {
		reduceTask := &m.reduceTasks[Y]
		switch reduceTask.status {
		case Done:
			continue
		case InProgress:
			if time.Now().After(reduceTask.timeStart.Add(10 * time.Second)) {
				reduceTask.status = Idle
			}
			return false
		case Idle:
			return false
		}
	}

	return true
}

func (m *Master) allMapDone() bool {

	for X, _ := range m.mapTasks {
		mapTask := &m.mapTasks[X]
		switch mapTask.status {
		case Done:
			continue
		case InProgress:
			if time.Now().After(mapTask.timeStart.Add(10 * time.Second)) {
				mapTask.status = Idle
			}
			return false
		case Idle:
			return false
		}
	}
	return true
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		nReduce:     nReduce,
		reduceFiles: make(map[string][]string),
		mutex:       sync.Mutex{},
	}

	var mapTasks []MapTask
	for _, file := range files {
		var task = MapTask{
			status: Idle,
			file:   file,
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
