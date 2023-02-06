package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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

type WorkerGoOnlineArgs struct {
}

type WorkerGoOnlineReply struct {
	WorkerID int64
	ReduceN  int64
}

type RequestTaskArgs struct {
	WorkerID int64
}

type RequestTaskReply struct {
	Action string

	// map only
	MapTaskID   int64
	MapTaskFile string

	// reduce only
	ReduceTaskID      int64
	AssociatedMapTask []int64
}

type MapTaskDoneArgs struct {
	WorkerID  int64
	MapTaskID int64
}

type MapTaskDoneReply struct {
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
