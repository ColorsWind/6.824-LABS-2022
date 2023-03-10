package kvraft

import (
	"6.824/labgob"
	"6.824/raft"
	"fmt"
	"log"
	"sync"
)

const (
	OK           = "OK"
	WaitComplete = "WaitComplete"
	//ErrNoKey       = "ErrNoKey"
	ErrWrongLeader   = "ErrWrongLeader"
	ErrOutdatedRPC   = "ErrOutdatedRPC"
	ErrApplySnapshot = "ErrApplySnapshot"
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
	ClientId  int64
	CommandId int64
}

func (args PutAppendArgs) String() string {
	return fmt.Sprintf("{Key=%v, Value=%v, op=%v, client_id=%v, command_id=%v}", args.Key, raft.ToStringLimited(args.Value, 10), args.Op, args.ClientId, args.CommandId)
}

type PutAppendReply struct {
	Err Err
}

func (reply PutAppendReply) String() string {
	return fmt.Sprintf("{Err=%v}", reply.Err)
}

type GetArgs struct {
	Key string

	// You'll have to add definitions here.
	ClientId  int64
	CommandId int64
}

func (args GetArgs) String() string {
	return fmt.Sprintf("{Key=%v, client_id=%v, command_id=%v}", args.Key, args.ClientId, args.CommandId)
}

type GetReply struct {
	Err   Err
	Value string
}

func (reply GetReply) String() string {
	return fmt.Sprintf("{Err=%v, Value=%v}", reply.Err, raft.ToStringLimited(reply.Value, 10))
}

type OpCache struct {
	mu   sync.Mutex
	cond *sync.Cond

	clientId  int64
	CommandId int64

	OpType OpType
	Key    string
	Value  string
	term   int
	Index  int
	Result string
	Err    Err
}

func (oc *OpCache) String() string {
	if oc.mu.TryLock() {
		defer oc.mu.Unlock()
	}
	s := fmt.Sprintf("{clientId=%v, cmdId=%v, OpType=%v, Key=%v, Value=%v, term=%v, Index=%v, Result=%v, Err=%v}", oc.clientId, oc.CommandId, oc.OpType, oc.Key, oc.Value, oc.term, oc.Index, oc.Result, oc.Err)
	return raft.ToStringLimited(s, 200)
}

func NewOpCache(clientId int64, commandId int64, opType OpType, key string, value string, term int, index int) *OpCache {
	oc := OpCache{}
	oc.mu.Lock()
	defer oc.mu.Unlock()
	oc.cond = sync.NewCond(&oc.mu)
	oc.clientId = clientId
	oc.CommandId = commandId
	oc.OpType = opType
	oc.Key = key
	oc.Value = value
	oc.term = term
	oc.Index = index
	oc.Err = WaitComplete
	return &oc
}

func ReadOpCacheFromByte(decoder *labgob.LabDecoder, me int) map[int64]*OpCache {
	var opCacheMap map[int64]*OpCache
	err := decoder.Decode(&opCacheMap)
	if err != nil {
		log.Panicf("%v fail to decode opCacheMap.", me)
	}
	for _, opCache := range opCacheMap {
		opCache.cond = sync.NewCond(&opCache.mu)
	}
	return opCacheMap
}

func (oc *OpCache) complete(result string, err Err) {
	oc.mu.Lock()
	defer oc.mu.Unlock()
	if oc.Err != WaitComplete {
		log.Panicf("already finished: %v.", oc)
	}
	oc.Result = result
	oc.Err = err
	oc.cond.Broadcast()
}

func (oc *OpCache) ensureFinished() {
	oc.mu.Lock()
	defer oc.mu.Unlock()
	if oc.Err == WaitComplete {
		log.Panicf("not finished: %v.", oc)
	}
}

func (oc *OpCache) finished() bool {
	oc.mu.Lock()
	defer oc.mu.Unlock()
	return oc.Err != WaitComplete
}

func (oc *OpCache) completeIfUnfinished(result string, err Err) bool {
	oc.mu.Lock()
	defer oc.mu.Unlock()
	if oc.Err == WaitComplete {
		oc.Result = result
		oc.Err = err
		oc.cond.Broadcast()
		return true
	}
	return false
}

func (oc *OpCache) get() (string, Err) {
	oc.mu.Lock()
	defer oc.mu.Unlock()
	for oc.Err == WaitComplete {
		oc.cond.Wait()
	}
	return oc.Result, oc.Err
}

func (oc *OpCache) merge(newer *OpCache) *OpCache {
	oc.mu.Lock()
	defer oc.mu.Unlock()
	newer.mu.Lock()
	defer newer.mu.Unlock()
	if oc.Err != WaitComplete {
		return newer
	}
	if oc.CommandId == newer.CommandId {
		if newer.Result != WaitComplete {
			oc.Result = newer.Result
			oc.Err = newer.Err
			oc.cond.Broadcast()
		}
	} else if oc.CommandId < newer.CommandId {
		oc.Err = ErrApplySnapshot
		oc.cond.Broadcast()
	} else {
		log.Panicf("%v is newer than %v.", oc, newer)
	}
	return newer
}
