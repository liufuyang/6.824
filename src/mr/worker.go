package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"
import "math/rand"

func init() {
	rand.Seed(time.Now().UnixNano())
}

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	id := RandStringRunes(5)

Loop:
	for {
		// uncomment to send the Example RPC to the master.
		task := GetTask(id)
		switch task.TaskType {
		case MapTaskType:
			{
				reduceFiles := doMap(task.FileNumberX, task.InputFile, task.NReduce, mapf)
				if reduceFiles == nil {
					// do map failed, continue immediately
					continue
				}
				// report map task result
				mapFinishReply := FinishTask(id, task.TaskType, task.FileNumberX, -1, reduceFiles)
				if mapFinishReply.MoreTask {
					continue
				} else {
					break
				}
			}
		case ReduceTaskType:
			{
				err := doReduce(task.FileNumberY, task.ReduceFiles, reducef)
				if err != nil {
					// do reduce failed, continue immediately
					continue
				}
				// report reduce task result
				mapFinishReply := FinishTask(id, task.TaskType, -1, task.FileNumberY, nil)
				if mapFinishReply.MoreTask {
					continue
				} else {
					break
				}
			}
		case NoMapTaskType:
			time.Sleep(5 * time.Second)
			continue
		case EndTaskType:
			fmt.Printf("Worker %v done. \n", id)
			break Loop
		}
	}
}

func GetTask(id string) GetTaskReply {
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	call("Master.GetTask", &args, &reply)
	fmt.Printf("Worker %v - TYPE-%v - [X: %v Y: %v] - map file name: %v - reduce file len: %v -  %v \n",
		id, reply.TaskType, reply.FileNumberX, reply.FileNumberY, reply.InputFile, len(reply.ReduceFiles), reply.ReduceFiles)

	return reply
}

func doMap(X int, filename string, nReduce int, mapf func(string, string) []KeyValue) []string {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))

	// split and sort
	sort.Sort(ByKey(kva))

	intermediate := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		Y := ihash(kv.Key) % nReduce
		intermediate[Y] = append(intermediate[Y], kv)
	}

	// write
	reduceFiles := []string{}

	for Y, kva := range intermediate {
		oname := fmt.Sprintf("mr-%v-%v", X, Y)
		ofile, _ := os.Create(oname)
		reduceFiles = append(reduceFiles, oname)

		enc := json.NewEncoder(ofile)
		for _, kv := range kva {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot encode json file %v k:%v v:%v", oname, kv.Key, kv.Value)
				ofile.Close()
				return nil
			}
		}
		ofile.Close()
	}


	// report back to master
	return reduceFiles
}

func doReduce(Y int, reduceFiles []string, reducef func(string, []string) string) error {
	kva := []KeyValue{}
	for _, filename := range reduceFiles {

		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("doReduce cannot open %v", filename)
			return err
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	sort.Sort(ByKey(kva))

	// Merge
	reduceMap := make(map[string][]string)
	for _, kv := range kva {
		reduceMap[kv.Key] = append(reduceMap[kv.Key], kv.Value)
	}

	// Write
	// To order the keys again
	var keys []string
	for k, _ := range reduceMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	oname := fmt.Sprintf("mr-out-%v", Y)
	ofile, err := os.Create(oname)
	if err != nil {
		return err
	}

	for _, k := range keys {
		value := reducef(k, reduceMap[k])
		fmt.Fprintf(ofile, "%v %v\n", k, value)
	}
	ofile.Close()

	return nil
}

func FinishTask(id string, taskType TaskType, X int, Y int, reduceFiles []string) FinishTaskReply {
	args := FinishTaskArgs{
		TaskType:    taskType,
		FileNumberX: X,
		FileNumberY: Y,
		ReduceFiles: reduceFiles,
	}
	reply := FinishTaskReply{}
	call("Master.FinishTask", &args, &reply)
	fmt.Printf("Worker %v - %v - finished - moreTask %v \n", id, args.TaskType, reply.MoreTask)

	return reply
}

// ------------------------------ Helper functions --------------------------------------
var letterRunes = []rune("ABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// ------------------------------ Examples below ----------------------------------------

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
