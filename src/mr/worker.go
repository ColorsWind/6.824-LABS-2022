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

// from mrsequential.go, for sort
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	workerID, nMap, nReduce := WorkerGoOnline()
g:
	for true {
		task := RequestTask(workerID)
		switch task.Action {
		case "exit":
			break g
		case "wait_map", "wait_reduce":
			time.Sleep(time.Second)
		case "map":
			DoMapTask(workerID, nReduce, task, mapf)
		case "reduce":
			DoReduceTask(workerID, nMap, task, reducef)
		}
	}

}

func DoMapTask(workerID int64, nReduce int64, task RequestTaskReply, mapf func(string, string) []KeyValue) {
	file, err := os.Open(task.MapTaskFile)
	if err != nil {
		log.Fatalf("#%v:\tCannot open: %v\n", workerID, task.MapTaskFile)
		return
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("#%v:\tCannot read: %v\n", workerID, task.MapTaskFile)
		return
	}
	err = file.Close()
	if err != nil {
		log.Fatalf("#%v:\tCannot close: %v\n", workerID, task.MapTaskFile)
		return
	}
	var writers []*os.File
	var encoders []*json.Encoder
	for i := int64(0); i < nReduce; i++ {
		intermediateFileName := fmt.Sprintf("mr-%v-%v", task.MapTaskID, i)
		file, err := ioutil.TempFile("", intermediateFileName)
		if err != nil {
			log.Fatalf("#%v:\tCannot create temporary file for: %v.\n", workerID, intermediateFileName)
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
			log.Fatalf("#%v:\tCannot encode: %v\n", workerID, kva)
			return
		}
	}
	for i, w := range writers {
		intermediateFileName := fmt.Sprintf("mr-%v-%v", task.MapTaskID, i)
		err := w.Close()
		if err != nil {
			log.Fatalf("#%v:\tCannot close: %v\n", workerID, w)
			return
		}
		err = os.Rename(w.Name(), intermediateFileName)
		if err != nil {
			log.Fatalf("#%v:\tCannot rename %v to %v, err: %v.\n", workerID, w.Name(), intermediateFileName, err)
			return
		}
	}
	ok := call("Coordinator.MapTaskDone", &MapTaskDoneArgs{workerID, task.MapTaskID}, &MapTaskDoneReply{})
	log.Printf("#%v:\tMap task done %v, tell coordinator=%v.\n", workerID, task.MapTaskID, ok)
}

func DoReduceTask(workerID int64, nMap int64, task RequestTaskReply, reducef func(string, []string) string) {
	var intermediate []KeyValue
	for i := int64(0); i < nMap; i++ {
		intermediateFileName := fmt.Sprintf("mr-%v-%v", i, task.ReduceTaskID)
		file, err := os.Open(intermediateFileName)
		if err != nil {
			log.Fatalf("#%v:\tCannot open: %v, err: %v.\n", workerID, intermediateFileName, err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	// from mrsequential.go, but with atomically rename.
	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%v", task.ReduceTaskID)
	ofile, err := ioutil.TempFile("", oname)
	if err != nil {
		log.Fatalf("#%v:\tCannot open temporary file for: %v, err: %v.\n", workerID, oname, err)
	}

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		_, err = fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		if err != nil {
			log.Fatalf("#%v:\tCannot write file: %v, err: %v.\n", workerID, ofile.Name(), err)
		}
		i = j
	}

	err = ofile.Close()
	if err != nil {
		log.Fatalf("#%v:\tCannot close: %v, err: %v.\n", workerID, ofile.Name(), err)
	}
	err = os.Rename(ofile.Name(), oname)
	if err != nil {
		log.Fatalf("#%v:\tCannot rename: %v to %v, err: %v.\n", workerID, ofile.Name(), oname, err)
	}

	ok := call("Coordinator.ReduceTaskDone", &ReduceTaskDoneArgs{workerID, task.ReduceTaskID}, &ReduceTaskDoneReply{})
	log.Printf("#%v:\tReduce task done %v, tell coordinator=%v.\n", workerID, task.ReduceTaskID, ok)
}

func RequestTask(workerID int64) RequestTaskReply {
	args := RequestTaskArgs{}
	args.WorkerID = workerID
	reply := RequestTaskReply{}
	ok := call("Coordinator.RequestTask", &args, &reply)
	if ok {
		log.Printf("#%v:\tRequest task success, %v", workerID, reply.Action)
	} else {
		panic(fmt.Sprintf("Worker go online fails, err=%v", ok))
	}
	return reply
}

func WorkerGoOnline() (int64, int64, int64) {
	args := WorkerGoOnlineArgs{}
	reply := WorkerGoOnlineReply{}
	ok := call("Coordinator.WorkerGoOnline", &args, &reply)
	if ok {
		log.Printf("Worker go online. WorkerID=%v\n", reply.WorkerID)
	} else {
		log.Panicf("Worker go online fails, err=%v\n", ok)
	}
	return reply.WorkerID, reply.MapN, reply.ReduceN
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
