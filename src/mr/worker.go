package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

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

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	workerID, nReduce := WorkerGoOnline()
	for true {
		task := RequestTask(workerID)
		switch task.Action {
		case "exit":
			break
		case "wait":
			time.Sleep(time.Second)
		case "map":
			DoMapTask(workerID, nReduce, task, mapf)
		}
	}

}

func DoMapTask(workerID int64, nReduce int64, task RequestTaskReply, mapf func(string, string) []KeyValue) {
	file, err := os.Open(task.MapTaskFile)
	if err != nil {
		log.Fatalf("#%v:\tCannot open %v\n", workerID, task.MapTaskFile)
		return
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("#%v:\tCannot read %v\n", workerID, task.MapTaskFile)
		return
	}
	err = file.Close()
	if err != nil {
		log.Fatalf("#%v:\tCannot close %v\n", workerID, task.MapTaskFile)
		return
	}
	var writers []*os.File
	var encoders []*json.Encoder
	for i := int64(0); i < nReduce; i++ {
		intermediateFileName := fmt.Sprintf("mr-%v-%v", task.MapTaskID, i)
		file, err := os.OpenFile(intermediateFileName, os.O_TRUNC|os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			log.Fatalf("#%v:\tCannot open %v\n", workerID, intermediateFileName)
			return
		}
		enc := json.NewEncoder(file)
		encoders = append(encoders, enc)
		writers = append(writers, file)
	}

	kva := mapf(task.MapTaskFile, string(content))
	for _, kv := range kva {
		//log.Fatalf("\nv=================================%v\n\n", nReduce)
		i := ihash(kv.Key) % int(nReduce)
		err := encoders[i].Encode(&kv)
		if err != nil {
			log.Fatalf("#%v:\tCannot encode %v\n", workerID, kva)
			return
		}
	}
	for _, w := range writers {
		err := w.Close()
		if err != nil {
			log.Fatalf("#%v:\tCannot close %v\n", workerID, w)
			return
		}
	}
	ok := call("Coordinator.MapTaskDone", &MapTaskDoneArgs{workerID, task.MapTaskID}, &MapTaskDoneReply{})
	log.Printf("#%v:\tMap task done %v, ok=%v.\n", workerID, task.MapTaskID, ok)
}

func RequestTask(workerID int64) RequestTaskReply {
	args := RequestTaskArgs{}
	args.WorkerID = workerID
	reply := RequestTaskReply{}
	ok := call("Coordinator.RequestTask", &args, &reply)
	if ok {
		log.Printf("#%v:\t Request task success, %v", workerID, reply.Action)
	} else {
		panic(fmt.Sprintf("Worker go online fails, err=%v", ok))
	}
	return reply
}

func WorkerGoOnline() (int64, int64) {
	args := WorkerGoOnlineArgs{}
	reply := WorkerGoOnlineReply{}
	ok := call("Coordinator.WorkerGoOnline", &args, &reply)
	if ok {
		log.Printf("Worker go online. WorkerID=%v\n", reply.WorkerID)
	} else {
		log.Panicf("Worker go online fails, err=%v\n", ok)
	}
	return reply.WorkerID, reply.ReduceN
}

//
// example function to show how to make an RPC call to the coordinator.
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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
