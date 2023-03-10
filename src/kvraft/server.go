package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"fmt"
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
	ClientId  int64
	CommandId int64
	Type      OpType
	Key       string
	Value     string
}

func (op Op) String() string {
	return fmt.Sprintf("client_id=%v, cmd_id=%v, type=%v, Key=%v, Value=%v", op.ClientId, op.CommandId, op.Type, op.Key, op.Value)
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	stateMachine     map[string]string
	opCachesList     []*OpCache
	opCacheMap       map[int64]*OpCache
	lastApplied      map[int64]int64
	lastAppliedIndex int
}

func (kv *KVServer) createOpCacheWithLock(op Op, term int, index int) *OpCache {
	opCache := NewOpCache(op.ClientId, op.CommandId, op.Type, op.Key, op.Value, term, index)
	kv.opCacheMap[opCache.clientId] = opCache
	//kv.opCachesList = append(kv.opCachesList, opCache)
	go func() {
		for !opCache.finished() {
			time.Sleep(250 * time.Millisecond)
			currentTerm, isLeader := kv.rf.GetState()
			kv.mu.Lock()
			//log.Printf("%v: wait, opCache=%v, currentTerm=%v, isLeader=%v, lastApplied=%v, rf=%v\n", kv.me, opCache, currentTerm, isLeader, kv.lastApplied[opCache.clientId], kv.rf)
			kv.mu.Unlock()
			if !isLeader || opCache.term != currentTerm {
				if opCache.completeIfUnfinished("", ErrWrongLeader) {
					log.Printf("%v: %v timeout.\n", kv.me, opCache)
				}
				return

			}
		}

	}()
	return opCache
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	opCache, present := kv.opCacheMap[args.ClientId]
	if !present || args.CommandId > opCache.CommandId {
		// new command
		op := Op{args.ClientId, args.CommandId, OpType_GET, args.Key, ""}
		index, currentTerm, isLeader := kv.rf.Start(op)
		if isLeader {
			opCache = kv.createOpCacheWithLock(op, currentTerm, index)
			kv.mu.Unlock()
			result, err := opCache.get()
			reply.Value = result
			reply.Err = err
			kv.mu.Lock()
			if err != OK {
				delete(kv.opCacheMap, args.ClientId)
			}
		} else {
			reply.Err = ErrWrongLeader
		}
	} else if args.CommandId == opCache.CommandId {
		// duplicate command
		kv.mu.Unlock()
		result, err := opCache.get()
		reply.Value = result
		reply.Err = err
		kv.mu.Lock()
		if err != OK {
			delete(kv.opCacheMap, args.ClientId)
		}
	} else {
		log.Panicf("kvserver %v receive invalid id (opCache.id=%v). args=%v, opCache=%v.", kv.me, args, opCache.CommandId, opCache)
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	opCache, present := kv.opCacheMap[args.ClientId]

	var opType OpType
	if args.Op == "Put" {
		opType = OpType_PUT
	} else if args.Op == "Append" {
		opType = OpType_APPEND
	} else {
		log.Panicf("Op is not Put or Append: %v.", args)
	}

	if !present || args.CommandId > opCache.CommandId {
		// new command
		op := Op{args.ClientId, args.CommandId, opType, args.Key, args.Value}
		index, currentTerm, isLeader := kv.rf.Start(op)
		if isLeader {
			opCache = kv.createOpCacheWithLock(op, currentTerm, index)
			kv.mu.Unlock()
			_, err := opCache.get()
			reply.Err = err
			kv.mu.Lock()
			if err != OK {
				delete(kv.opCacheMap, args.ClientId)
			}
		} else {
			reply.Err = ErrWrongLeader
		}
	} else if args.CommandId == opCache.CommandId {
		// duplicate command
		kv.mu.Unlock()
		_, err := opCache.get()
		reply.Err = err
		kv.mu.Lock()
		if err != OK {
			delete(kv.opCacheMap, args.ClientId)
		}
	} else {
		log.Panicf("kvserver %v receive invalid id. args=%v, opCache=%v.", kv.me, args, opCache)
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

func (kv *KVServer) encodeState(kvMap map[string]string, opCacheMap map[int64]*OpCache) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	var err error
	err = e.Encode(kvMap)
	if err != nil {
		log.Panicf("%v fail to encode kvmap: %v.", kv.me, kvMap)
	}
	err = e.Encode(opCacheMap)
	if err != nil {
		log.Panicf("%v fail to encode opCacheMap: %v.", kv.me, opCacheMap)
	}
	return w.Bytes()
}

func (kv *KVServer) decodeState(data []byte) (map[string]string, map[int64]*OpCache) {
	r := bytes.NewBuffer(data)
	kvMap := make(map[string]string)
	opCacheMap := make(map[int64]*OpCache)
	d := labgob.NewDecoder(r)
	err := d.Decode(&kvMap)
	if err != nil {
		log.Panicf("%v fail to decode kvmap.", kv.me)
	}
	opCacheMap = ReadOpCacheFromByte(d, kv.me)
	return kvMap, opCacheMap
}

// StartKVServer
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant Key/Value service.
// me is the Index of the current server in servers[].
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
	kv.opCacheMap = make(map[int64]*OpCache)
	kv.lastApplied = make(map[int64]int64)
	go func() {
		for !kv.killed() {
			msg := <-kv.applyCh
			kv.mu.Lock()
			log.Printf("%v: Receive msg: %v.\n", kv.me, msg)
			if msg.SnapshotValid {
				if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.CommandIndex, msg.Snapshot) {
					// receive snapshot indicate is not leader
					for _, opCache := range kv.opCacheMap {
						opCache.completeIfUnfinished("", ErrApplySnapshot)
					}
					kvMap, opCacheMap := kv.decodeState(msg.Snapshot)
					lastAppliedMap := make(map[int64]int64)
					for clientId, opCache := range opCacheMap {
						if opCache.finished() {
							lastAppliedMap[clientId] = opCache.CommandId
						} else {
							lastAppliedMap[clientId] = opCache.CommandId - 1
						}
					}
					kv.lastApplied = lastAppliedMap
					kv.opCacheMap = opCacheMap
					kv.stateMachine = kvMap
					kv.lastAppliedIndex = msg.SnapshotIndex
					log.Printf("%v\n", kvMap)
				}

			}
			if msg.CommandValid {
				command := msg.Command.(Op)

				// update state machine
				result := ""
				lastApplied := kv.lastApplied[command.ClientId]
				if command.CommandId != lastApplied && command.CommandId != lastApplied+1 {
					log.Panicf("%v receive msg with invalid command id. msg=%v, lastApplied=%v.", me, msg, lastApplied)
				}
				if command.CommandId == lastApplied+1 {
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
				} else {
					switch command.Type {
					case OpType_GET:
						result = kv.stateMachine[command.Key]
					case OpType_APPEND, OpType_PUT:
						// ignore
					default:
						log.Panicf("Could not identify OpType: %v.", command.Type)
					}
				}
				kv.lastApplied[command.ClientId] = command.CommandId

				opCache, present := kv.opCacheMap[command.ClientId]
				if !present || command.CommandId > opCache.CommandId {
					// new command
					if present {
						opCache.ensureFinished()
					}
					opCache = kv.createOpCacheWithLock(command, -1, -1)
					opCache.complete(result, OK)
				} else if opCache.CommandId == command.CommandId {
					opCache.completeIfUnfinished(result, OK)
				} else {
					log.Printf("%v: receive outdated msg, just update state but not change opCache. msg=%v, opCache=%v.", kv.me, msg, opCache)
				}
				kv.lastAppliedIndex = msg.CommandIndex
			}
			if maxraftstate > 0 && persister.RaftStateSize()*8/10 > maxraftstate {
				snapshot := kv.encodeState(kv.stateMachine, kv.opCacheMap)
				kv.rf.Snapshot(kv.lastAppliedIndex, snapshot)
			}
			kv.mu.Unlock()

		}
	}()
	return kv
}
