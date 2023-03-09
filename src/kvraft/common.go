package kvraft

import (
	"6.824/raft"
	"fmt"
	"sync"
)

const (
	OK             = "OK"
	WaitComplete   = "WaitComplete"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"

	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	UniqueId   int64
	PrevRecvId int64
}

func (args PutAppendArgs) String() string {
	return fmt.Sprintf("{key=%v, value=%v, op=%v, id=%v, prevId=%v}", args.Key, raft.ToStringLimited(args.Value, 10), args.Op, args.UniqueId, args.PrevRecvId)
}

type PutAppendReply struct {
	Err Err
}

func (reply PutAppendReply) String() string {
	return fmt.Sprintf("{err=%v}", reply.Err)
}

type GetArgs struct {
	Key string

	// You'll have to add definitions here.
	UniqueId   int64
	PrevRecvId int64
}

func (args GetArgs) String() string {
	return fmt.Sprintf("{key=%v}", args.Key)
}

type GetReply struct {
	Err   Err
	Value string
}

func (reply GetReply) String() string {
	return fmt.Sprintf("{err=%v, value=%v}", reply.Err, raft.ToStringLimited(reply.Value, 10))
}

type OpCache struct {
	mu       sync.Mutex
	cond     *sync.Cond
	opType   OpType
	key      string
	value    string
	term     int
	index    int
	uniqueId int64
	result   string
	err      Err
}

func (oc *OpCache) String() string {
	return fmt.Sprintf("{opType=%v, key=%v, value=%v, term=%v, id=%v, result=%v, err=%v}", oc.opType, oc.key, oc.value, oc.term, oc.uniqueId, oc.result, oc.err)
}

func NewOpCache(opType OpType, key string, value string, term int, index int, uniqueId int64) *OpCache {
	oc := OpCache{}
	oc.cond = sync.NewCond(&oc.mu)
	oc.opType = opType
	oc.key = key
	oc.value = value
	oc.term = term
	oc.index = index
	oc.uniqueId = uniqueId
	oc.err = WaitComplete
	return &oc
}

func (oc *OpCache) complete(result string, err Err) {
	oc.mu.Lock()
	defer oc.mu.Unlock()
	oc.result = result
	oc.err = err
	oc.cond.Broadcast()
}

func (oc *OpCache) completeIfUnfinished(result string, err Err) bool {
	oc.mu.Lock()
	defer oc.mu.Unlock()
	if oc.err == WaitComplete {
		oc.result = result
		oc.err = err
		oc.cond.Broadcast()
		return true
	}
	return false
}

func (oc *OpCache) get() (string, Err) {
	oc.mu.Lock()
	defer oc.mu.Unlock()
	for oc.err == WaitComplete {
		oc.cond.Wait()
	}
	return oc.result, oc.err
}
