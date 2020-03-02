package mr

import (
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
}

type MapTask struct {
	status    TaskStatus
	timeStart time.Time
	file      string
}

type ReduceTask struct {
	status    TaskStatus
	timeStart time.Time
	files     []string
}

type TaskStatus int

const (
	Idle       TaskStatus = 0
	InProgress TaskStatus = 1
	Done       TaskStatus = 2
)

/*
	Your code here -- RPC handlers for the worker to call.
	As we define the intermediate files name as `mr-X-Y` and final output file names as `mr-out-Y`

	We use
		* `X` to define the file number for map function input
		* `Y` to define the file number for reduce function grouped input

	For example:
	If after map phase, worker1 generate files [mr-0-1, mr-0-2],
	and worker2 generate files [mr-1-1, mr-1-2] (when setting nReduce=2)
	Then at reduce phase, one worker will handle files [mr-0-1, mr-1-1],
	another worker will handle files [mr-0-2, mr-1-2].
*/

func (m *Master) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {

	m.mutex.Lock()
	defer m.mutex.Unlock()

	// Do map tasks
	for X, _ := range m.mapTasks {
		mapTask := &m.mapTasks[X] // get reference instead of copy!

		if mapTask.status == Idle {
			reply.TaskType = MapTaskType
			reply.FileNumberX = X
			reply.FileNumberY = -1
			reply.InputFile = mapTask.file
			reply.NReduce = m.nReduce

			mapTask.timeStart = time.Now()
			mapTask.status = InProgress

			return nil
		}
	}

	// Let worker wait for all map tasks done
	if !m.allMapDone() {
		reply.TaskType = WaitingTaskType
		reply.FileNumberX = -2
		reply.FileNumberY = -2
		return nil
	}

	// Do reduce tasks
	// Only when map task all done (checked above)
	for Y, _ := range m.reduceTasks {
		reduceTask := &m.reduceTasks[Y] // get reference instead of copy!

		if reduceTask.status == Idle {
			reply.TaskType = ReduceTaskType
			reply.FileNumberY = Y
			reply.FileNumberX = -1
			reply.ReduceFiles = reduceTask.files

			reduceTask.timeStart = time.Now()
			reduceTask.status = InProgress

			return nil
		}
	}

	// reaching here means all done
	if m.Done() {
		reply.TaskType = EndTaskType
		reply.FileNumberX = -3
		reply.FileNumberY = -3
	} else {
		reply.TaskType = WaitingTaskType
		reply.FileNumberX = -2
		reply.FileNumberY = -2
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
				Y, err := strconv.Atoi(parts[len(parts)-1])
				if err != nil {
					log.Fatal("Map output file name not ending with int: ", err)
				}
				partition := m.reduceTasks[Y].files
				partition = append(partition, reduceFile)
				m.reduceTasks[Y].files = partition
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
		nReduce: nReduce,
		mutex:   sync.Mutex{},
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
		var task = ReduceTask{status: Idle, files: []string{}}
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
