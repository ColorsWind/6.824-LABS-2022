package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"fmt"
	"log"
	"os"
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
	return fmt.Sprintf("{client_id=%v, cmd_id=%v, type=%v, Key=%v, Value=%v}", op.ClientId, op.CommandId, op.Type, op.Key, op.Value)
}

type ExecutedOp struct {
	Op
	Result string
}

func (ec ExecutedOp) String() string {
	return fmt.Sprintf("op=%v, result=%v", ec.Op, raft.ToStringLimited(ec.Result, 100))
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvMap                 map[string]string
	lastAppliedCommandMap map[int64]ExecutedOp
	clients               map[int64]*ClientHandler
	//lastAppliedMap     map[int64]int64
	lastAppliedIndex int
	logger           *log.Logger
}

type ClientHandler struct {
	mu        sync.Mutex
	cond      *sync.Cond
	clientId  int64
	commandId int64
	finished  bool
	result    string
	err       Err
}

func (handler *ClientHandler) String() string {
	if handler.mu.TryLock() {
		defer handler.mu.Unlock()
	}
	return fmt.Sprintf("{client_id=%v, cmd_id=%v, finished=%v, result=%v, err=%v}", handler.clientId, handler.commandId, handler.finished, handler.result, handler.err)
}

func newClientHandler(clientId int64, commandId int64) *ClientHandler {
	handler := ClientHandler{
		clientId:  clientId,
		commandId: commandId,
		err:       ErrNotStarted,
		finished:  true,
	}
	handler.cond = sync.NewCond(&handler.mu)
	return &handler
}

func (kv *KVServer) handleRequest(clientId int64, commandId int64, opType OpType, key string, value string) (result string, err Err) {
	kv.mu.Lock()
	handler, present := kv.clients[clientId]
	if !present || handler.commandId < commandId {
		handler = newClientHandler(clientId, commandId)
		kv.clients[clientId] = handler
	}

	// if already finish
	handler.mu.Lock()
	defer handler.mu.Unlock()
	lastAppliedCommand, present := kv.lastAppliedCommandMap[clientId]
	if present && lastAppliedCommand.CommandId == commandId {
		handler.result = lastAppliedCommand.Result
		handler.err = OK
		handler.finished = true
		handler.cond.Broadcast()
		kv.mu.Unlock()
		return handler.result, handler.err
	} else {
		kv.mu.Unlock()
	}

	// if invalid command id
	if handler.commandId > commandId {
		kv.logger.Panicf("kvserver %v receive invalid id (handler.id=%v). client_id=%v, cmd_id=%v, op_type=%v, key=%v, value=%v, handler=%v.", kv.me, handler.commandId, clientId, commandId, opType, key, value, handler)
	}

	// start command
	if handler.finished && handler.err != OK {
		// should retry
		op := Op{clientId, commandId, opType, key, value}
		_, currentTerm, isLeader := kv.rf.Start(op)
		if !isLeader {
			return "", ErrWrongLeader
		}
		handler.finished = false
		handler.err = WaitComplete

		finishCh := make(chan bool)
		go func() {
			handler.mu.Lock()
			for !handler.finished {
				handler.cond.Wait()
			}
			handler.mu.Unlock()
			finishCh <- true
		}()
		go func() {
			for i := 0; !kv.killed(); i++ {
				select {
				case <-finishCh:
					return
				case <-time.After(250 * time.Millisecond):
					currentTerm1, _ := kv.rf.GetState()
					if currentTerm == currentTerm1 {
						kv.mu.Lock()
						kv.logger.Printf("%v: client handler continue to wait, currentTerm=%v, i=%v, lastAppliedCmd_id=%v, client_id=%v, cmd_id=%v, op_type=%v, key=%v, value=%v, handler=%v.\n",
							kv.me, currentTerm1, i, kv.lastAppliedCommandMap[clientId].CommandId, clientId, commandId, opType, key, value, handler)
						kv.mu.Unlock()
					} else {
						kv.mu.Lock()
						kv.logger.Printf("%v: client handler timeout, currentTerm=%v, i=%v, lastAppliedCmd_id=%v, client_id=%v, cmd_id=%v, op_type=%v, key=%v, value=%v, handler=%v.\n",
							kv.me, currentTerm1, i, kv.lastAppliedCommandMap[clientId].CommandId, clientId, commandId, opType, key, value, handler)
						kv.mu.Unlock()
						handler.mu.Lock()
						handler.err = ErrApplySnapshot
						handler.finished = true
						handler.cond.Broadcast()
						handler.mu.Unlock()
						return
					}
				}
			}
			if kv.killed() {
				handler.mu.Lock()
				if handler.finished == false {
					kv.logger.Printf("%v: client handler killed, client_id=%v, cmd_id=%v, op_type=%v, key=%v, value=%v, handler=%v.\n",
						kv.me, clientId, commandId, opType, key, value, handler)
					handler.err = ErrShutdown
					handler.finished = true
					handler.cond.Broadcast()
				}
				handler.mu.Unlock()
			}
		}()
	}

	// wait result
	for !handler.finished {
		handler.cond.Wait()
	}
	return handler.result, handler.err
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	result, err := kv.handleRequest(args.ClientId, args.CommandId, OpType_GET, args.Key, "VALUE_GET")
	reply.Value = result
	reply.Err = err
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	var opType OpType
	if args.Op == "Put" {
		opType = OpType_PUT
	} else if args.Op == "Append" {
		opType = OpType_APPEND
	} else {
		kv.logger.Panicf("Op is not Put or Append: %v.", args)
	}
	_, err := kv.handleRequest(args.ClientId, args.CommandId, opType, args.Key, args.Value)
	reply.Err = err

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
	//kv.applyCh <- raft.ApplyMsg{
	//	CommandValid:  false,
	//	Command:       "Shutdown",
	//	CommandIndex:  -1,
	//	SnapshotValid: false,
	//	Snapshot:      nil,
	//	SnapshotTerm:  -1,
	//	SnapshotIndex: -1,
	//}

}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) encodeState(kvMap map[string]string, lastAppliedCommand map[int64]ExecutedOp) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(kvMap); err != nil {
		kv.logger.Panicf("%v fail to encode kvmap: %v.", kv.me, kvMap)
	}
	if err := e.Encode(lastAppliedCommand); err != nil {
		kv.logger.Panicf("%v fail to encode lastAppliedCommand: %v.", kv.me, lastAppliedCommand)
	}
	return w.Bytes()
}

func (kv *KVServer) decodeState(data []byte) (kvMap map[string]string, lastAppliedCommand map[int64]ExecutedOp) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if err := d.Decode(&kvMap); err != nil {
		kv.logger.Panicf("%v fail to decode kvmap.", kv.me)
	}

	if err := d.Decode(&lastAppliedCommand); err != nil {
		kv.logger.Panicf("%v fail to decode lastAppliedCommandMap.", kv.me)
	}
	return
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
	kv.logger = log.New(os.Stdout, "", log.Lshortfile|log.Lmicroseconds)
	//kv.logger.SetOutput(ioutil.Discard)

	// You may need initialization code here.
	kv.kvMap = make(map[string]string)
	kv.clients = make(map[int64]*ClientHandler)
	kv.lastAppliedCommandMap = make(map[int64]ExecutedOp)
	kv.lastAppliedIndex = 0
	go func() {
		for true {
			msg := <-kv.applyCh
			kv.mu.Lock()
			kv.logger.Printf("%v: Receive msg: %v.\n", kv.me, msg)
			if msg.SnapshotValid {
				for {
					if msg.SnapshotValid && kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.CommandIndex, msg.Snapshot) {
						break
					}
					msg = <-kv.applyCh
				}
				// receive snapshot indicate is not leader
				kvMap, lastAppliedCommandMap := kv.decodeState(msg.Snapshot)
				kv.lastAppliedCommandMap = lastAppliedCommandMap
				kv.kvMap = kvMap
				kv.lastAppliedIndex = msg.SnapshotIndex
				for _, command := range lastAppliedCommandMap {
					handler, present := kv.clients[command.ClientId]
					if present && handler.commandId == command.CommandId {
						handler.mu.Lock()
						handler.result = command.Result
						handler.err = OK
						handler.finished = true
						handler.cond.Broadcast()
						handler.mu.Unlock()
					}
				}
				kv.logger.Printf("%v: update lastAppliedIndex: %v\n", me, lastAppliedCommandMap)
			} else if msg.CommandValid {
				command := msg.Command.(Op)

				// update state machine
				result := ""
				lastAppliedCommand := kv.lastAppliedCommandMap[command.ClientId]
				if command.CommandId != lastAppliedCommand.CommandId && command.CommandId != lastAppliedCommand.CommandId+1 {
					kv.logger.Panicf("%v receive msg with invalid command id. msg=%v, lastAppliedMap=%v.", me, msg, lastAppliedCommand.CommandId)
				}
				if command.CommandId == lastAppliedCommand.CommandId+1 {
					switch command.Type {
					case OpType_GET:
						result = kv.kvMap[command.Key]
					case OpType_PUT:
						kv.kvMap[command.Key] = command.Value
					case OpType_APPEND:
						kv.kvMap[command.Key] = kv.kvMap[command.Key] + command.Value
					default:
						kv.logger.Panicf("Could not identify OpType: %v.", command.Type)
					}
					// linearizability: for duplicated command, only the first result is acceptable
					// because other client may invoke PUT/APPEND command
					ec := ExecutedOp{command, result}
					kv.logger.Printf("%v: update %v.\n", me, ec)
					kv.lastAppliedCommandMap[command.ClientId] = ec
				} else {
					result = lastAppliedCommand.Result
				}
				if result == "" && command.Type == OpType_GET {
					//log.Printf("dump: rf=%v, kv=%v\n", kv.rf, kv)
				}

				handler, present := kv.clients[command.ClientId]
				if present && handler.commandId == command.CommandId {
					handler.mu.Lock()
					handler.result = result
					handler.err = OK
					handler.finished = true
					handler.cond.Broadcast()
					handler.mu.Unlock()
				}
				kv.lastAppliedIndex = msg.CommandIndex
			} else {
				kv.logger.Printf("%v: detect applyCh close, return, msg=%v.\n", me, msg)
				kv.mu.Unlock()
				return
			}
			if maxraftstate > 0 && persister.RaftStateSize() > maxraftstate {
				kv.logger.Printf("%v: raft state size greater maxraftstate(%v > %v), trim log.\n", kv.me, persister.RaftStateSize(), maxraftstate)
				snapshot := kv.encodeState(kv.kvMap, kv.lastAppliedCommandMap)
				kv.rf.Snapshot(kv.lastAppliedIndex, snapshot)
			}
			kv.mu.Unlock()
		}
	}()
	return kv
}
