package mr

import (
	"log"
	"sync"
	"sync/atomic"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	workerCount int64
	mutex       sync.Mutex
	nMap        int64
	nReduce     int64

	inputFiles   []string
	planMapTasks []int64
	currMapTasks map[int64]int64

	planReduceTasks []int64
	currReduceTasks map[int64]int64
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) WorkerGoOnline(args *WorkerGoOnlineArgs, reply *WorkerGoOnlineReply) error {
	workerID := atomic.AddInt64(&c.workerCount, 1) - 1
	reply.WorkerID = workerID
	reply.MapN = c.nMap
	reply.ReduceN = c.nReduce
	log.Printf("#%v:\tAssigned worker id.\n", workerID)
	return nil
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	workerID := args.WorkerID
	if len(c.planMapTasks) > 0 {
		// send map task
		mapTaskID := c.planMapTasks[0]
		c.planMapTasks = c.planMapTasks[1:]
		reply.Action = "map"
		reply.MapTaskID = mapTaskID
		reply.MapTaskFile = c.inputFiles[mapTaskID]
		c.currMapTasks[workerID] = mapTaskID
		log.Printf("#%v:\tAssigned map task: %v.\n", workerID, mapTaskID)
		go func() {
			time.Sleep(10 * time.Second)
			c.mutex.Lock()
			defer c.mutex.Unlock()
			if element, present := c.currMapTasks[workerID]; present && element == mapTaskID {
				// timeout
				c.planMapTasks = append(c.planMapTasks, mapTaskID)
				delete(c.currMapTasks, workerID)
				log.Printf("#%v:\tMap task timeout: %v.\n", workerID, mapTaskID)
			}
		}()
	} else if len(c.currMapTasks) > 0 {
		// wait all map tasks end
		reply.Action = "wait_map"
	} else if len(c.planReduceTasks) > 0 {
		// send reduce task
		reduceTaskID := c.planReduceTasks[0]
		c.planReduceTasks = c.planReduceTasks[1:]
		reply.Action = "reduce"
		reply.ReduceTaskID = reduceTaskID
		c.currReduceTasks[workerID] = reduceTaskID
		log.Printf("#%v:\tAssigned reduce task: %v.\n", workerID, reduceTaskID)
		go func() {
			time.Sleep(10 * time.Second)
			c.mutex.Lock()
			defer c.mutex.Unlock()
			if element, present := c.currReduceTasks[workerID]; present && element == reduceTaskID {
				// timeout
				c.planReduceTasks = append(c.planReduceTasks, reduceTaskID)
				delete(c.currReduceTasks, workerID)
				log.Printf("#%v:\tReduce task timeout: %v.\n", workerID, reduceTaskID)
			}
		}()
	} else if len(c.currReduceTasks) > 0 {
		reply.Action = "wait_reduce"
	} else {
		reply.Action = "exit"
	}
	return nil
}

func (c *Coordinator) MapTaskDone(args *MapTaskDoneArgs, reply *MapTaskDoneReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	workerID := args.WorkerID
	if currentMapTaskID, present := c.currMapTasks[workerID]; present && currentMapTaskID == args.MapTaskID {
		delete(c.currMapTasks, workerID)
		log.Printf("#%v:\tReceived successful map task: %v.\n", workerID, args.MapTaskID)
	} else {
		log.Printf("#%v:\tReceived timeout map task: %v.\n", workerID, args.MapTaskID)
	}

	return nil
}

func (c *Coordinator) ReduceTaskDone(args *ReduceTaskDoneArgs, reply *ReduceTaskDoneReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	workerID := args.WorkerID
	if currentReduceTaskID, present := c.currReduceTasks[workerID]; present && currentReduceTaskID == args.ReduceTaskID {
		delete(c.currReduceTasks, workerID)
		log.Printf("#%v:\tReceived successful reduce task: %v.\n", workerID, args.ReduceTaskID)
	} else {
		log.Printf("#%v:\tReceived timeout reduce task: %v.\n", workerID, args.ReduceTaskID)
	}

	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.mutex.Lock()
	c.nReduce = int64(nReduce)
	c.nMap = int64(len(files))

	c.inputFiles = files
	c.currMapTasks = make(map[int64]int64)
	for i := 0; i < len(files); i++ {
		c.planMapTasks = append(c.planMapTasks, int64(i))
	}

	c.currReduceTasks = make(map[int64]int64)
	for i := 0; i < nReduce; i++ {
		c.planReduceTasks = append(c.planReduceTasks, int64(i))
	}

	// Your code here.
	c.mutex.Unlock()
	c.server()
	return &c
}
