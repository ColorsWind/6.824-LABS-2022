package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type OpType string

const (
	OpType_GET    = "OpType_GET"
	OpType_PUT    = "OpType_PUT"
	OpType_APPEND = "OpType_APPEND"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	UniqueId int64
	Type     OpType
	Key      string
	Value    string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	stateMachine map[string]string
	opCachesById map[int64]*OpCache
	opCachesList []*OpCache
}

func (kv *KVServer) createOpCacheWithLock(op Op, term int, index int) *OpCache {
	opCache := NewOpCache(op.Type, op.Value, op.Key, term, index, op.UniqueId)
	kv.opCachesById[opCache.uniqueId] = opCache
	kv.opCachesList = append(kv.opCachesList, opCache)
	return opCache
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()

	// prevId reveal already accept op
	delete(kv.opCachesById, args.PrevRecvId)

	opCache, present := kv.opCachesById[args.UniqueId]
	kv.mu.Unlock()
	if present {
		result, err := opCache.get()
		reply.Value = result
		reply.Err = err
		return
	}
	// is safe to Start first then add event handle to opCachesById, because Get hold mu
	kv.mu.Lock()
	op := Op{args.UniqueId, OpType_GET, args.Key, ""}
	index, currentTerm, isLeader := kv.rf.Start(op)
	if isLeader {
		opCache = kv.createOpCacheWithLock(op, currentTerm, index)
	}
	kv.mu.Unlock()

	if isLeader {
		result, err := opCache.get()
		reply.Value = result
		reply.Err = err
	} else {
		reply.Err = ErrWrongLeader
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	opCache, present := kv.opCachesById[args.UniqueId]

	// prevId reveal already accept op
	delete(kv.opCachesById, args.PrevRecvId)
	log.Printf("opCachesById Size: %v, opCachesList size: %v\n", len(kv.opCachesById), len(kv.opCachesList))
	kv.mu.Unlock()
	if present {
		_, err := opCache.get()
		reply.Err = err
		return
	}

	var opType OpType
	if args.Op == "Put" {
		opType = OpType_PUT
	} else if args.Op == "Append" {
		opType = OpType_APPEND
	} else {
		log.Panicf("Op is not Put or Append: %v.", args)
	}

	kv.mu.Lock()
	op := Op{args.UniqueId, opType, args.Key, args.Value}
	index, currentTerm, isLeader := kv.rf.Start(op)
	if isLeader {
		opCache = kv.createOpCacheWithLock(op, currentTerm, index)
	}
	kv.mu.Unlock()

	if isLeader {
		_, err := opCache.get()
		reply.Err = err
	} else {
		reply.Err = ErrWrongLeader
	}

}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.stateMachine = make(map[string]string)
	kv.opCachesById = make(map[int64]*OpCache)
	go func() {
		for !kv.killed() {
			select {
			case msg := <-kv.applyCh:
				kv.mu.Lock()
				command := msg.Command.(Op)
				opCache, present := kv.opCachesById[command.UniqueId]
				if !present {
					kv.mu.Unlock()
					return
				}
				result := ""

				switch command.Type {
				case OpType_GET:
					result = kv.stateMachine[command.Key]
				case OpType_PUT:
					kv.stateMachine[command.Key] = command.Value
				case OpType_APPEND:
					kv.stateMachine[command.Key] = kv.stateMachine[command.Key] + command.Value
				default:
					log.Panicf("Could not identify OpType: %v.", command.Type)
				}
				//log.Printf("!!!%v, %v, %v\n", result, opCache.opType, kv.stateMachine)
				// a msg's term smaller than the current applied ones, or equal term but smaller index,
				// indicate that the msg (and any msg below it), will never be completed.
				i := 0
				for ; i < len(kv.opCachesList); i++ {
					if kv.opCachesList[i].term > opCache.term || kv.opCachesList[i].term == opCache.term && kv.opCachesList[i].index > opCache.index {
						break
					}
				}
				failedOpCaches := kv.opCachesList[:i]
				kv.opCachesList = kv.opCachesList[i:]
				kv.mu.Unlock()

				opCache.complete(result, OK)
				for _, failedOpCache := range failedOpCaches {
					failedOpCache.completeIfUnfinished("", ErrWrongLeader)
				}
			default:
				time.Sleep(50 * time.Millisecond)
			}
		}
	}()
	return kv
}
