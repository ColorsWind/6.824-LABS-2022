package shardkv

import (
	"6.824/labrpc"
	"6.824/shardctrler"
	"bytes"
	"fmt"
	"log"
	"os"
	"sync/atomic"
	"time"
)
import "6.824/raft"
import "sync"
import "6.824/labgob"

type OpType string

const (
	OpType_GET         = "OpType_GET"
	OpType_PUT         = "OpType_PUT"
	OpType_APPEND      = "OpType_APPEND"
	OpType_RECONFIGURE = "OpType_RECONFIGURE"
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
	Num       int
}

func (op Op) String() string {
	return fmt.Sprintf("{client_id=%v, cmd_id=%v, type=%v, Key=%v, Value=%v, Num=%v}", op.ClientId, op.CommandId, op.Type, op.Key, op.Value, op.Num)
}

type ExecutedOp struct {
	Op
	Result string
}

func (ec ExecutedOp) String() string {
	return fmt.Sprintf("op=%v, result=%v", ec.Op, raft.ToStringLimited(ec.Result, 100))
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

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	persister *raft.Persister
	mck       *shardctrler.Clerk
	logger    *log.Logger
	dead      int32

	// Your definitions here.
	kvMap                 map[string]string
	lastAppliedCommandMap map[int64]ExecutedOp
	handlerByClientId     map[int64]*ClientHandler                      // only handleRequest will be able to modify
	handlerByShard        [shardctrler.NShards]map[int64]*ClientHandler // only handleRequest will be able to modify

	lastAppliedIndex int
	config           shardctrler.Config
}

func (kv *ShardKV) handleConfigurationPoll() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		return
	}
	cf := kv.mck.Query(-1)
	if cf.Num != kv.config.Num {
		kv.rf.Start(Op{-1, -1, OpType_RECONFIGURE, "", "", cf.Num})
	}
	// TODO: save config num to prevent duplicate `Start`
}

func (kv *ShardKV) handleRequest(clientId int64, commandId int64, opType OpType, key string, value string) (result string, err Err) {
	kv.mu.Lock()
	if kv.config.Shards[key2shard(key)] != kv.gid {
		kv.mu.Unlock()
		return "", ErrWrongGroup
	}
	handler, present := kv.handlerByClientId[clientId]
	if !present || handler.commandId < commandId {
		handler = &ClientHandler{
			clientId:  clientId,
			commandId: commandId,
			err:       ErrNotStarted,
			finished:  true,
		}
		handler.cond = sync.NewCond(&handler.mu)
		kv.handlerByClientId[clientId] = handler
		kv.handlerByShard[key2shard(key)][clientId] = handler
	}
	lastAppliedCommand, present := kv.lastAppliedCommandMap[clientId]
	kv.mu.Unlock()
	// if already finish
	handler.mu.Lock()
	defer handler.mu.Unlock()

	if present && lastAppliedCommand.CommandId == commandId {
		handler.result = lastAppliedCommand.Result
		handler.err = OK
		handler.finished = true
		handler.cond.Broadcast()
		return handler.result, handler.err
	}

	//// if invalid command id
	//if handler.commandId > commandId {
	//	kv.logger.Panicf("kvserver %v receive invalid id (handler.id=%v). client_id=%v, cmd_id=%v, op_type=%v, key=%v, value=%v, handler=%v.", kv.me, handler.commandId, clientId, commandId, opType, key, value, handler)
	//}
	// if outdate rpc
	if commandId < handler.commandId || commandId < lastAppliedCommand.CommandId {
		return "", ErrOutdatedRPC
	}

	// if should retry
	if handler.finished && handler.err != OK {
		op := Op{clientId, commandId, opType, key, value, -1}
		_, currentTerm, isLeader := kv.rf.Start(op)
		if !isLeader {
			return "", ErrWrongLeader
		}
		handler.finished = false
		handler.err = WaitComplete

		// check timeout
		go func() {
			for i := 0; ; i++ {
				time.Sleep(250 * time.Millisecond)
				handler.mu.Lock()
				finished := handler.finished
				handler.mu.Unlock()
				if finished == true {
					return
				}
				if kv.killed() {
					handler.mu.Lock()
					kv.logger.Printf("%v: client handler killed, op=%v, handler=%v.\n",
						kv.me, op, handler)
					handler.err = ErrShutdown
					handler.finished = true
					handler.cond.Broadcast()
					handler.mu.Unlock()
				}
				currentTerm1, _ := kv.rf.GetState()
				if currentTerm == currentTerm1 {
					kv.mu.Lock()
					kv.logger.Printf("%v: client handler continue to wait, currentTerm=%v, i=%v, lastAppliedCmd_id=%v, op=%v, handler=%v.\n",
						kv.me, currentTerm1, i, kv.lastAppliedCommandMap[clientId].CommandId, op, handler)
					kv.mu.Unlock()
				} else {
					kv.mu.Lock()
					kv.logger.Printf("%v: client handler timeout, term changed(%v -> %v), i=%v, lastAppliedCmd_id=%v, op=%v, handler=%v.\n",
						kv.me, currentTerm, currentTerm1, i, kv.lastAppliedCommandMap[clientId].CommandId, op, handler)
					kv.mu.Unlock()
					handler.mu.Lock()
					handler.err = ErrWrongLeader
					handler.finished = true
					handler.cond.Broadcast()
					handler.mu.Unlock()
					return
				}

			}
		}()
	}

	// wait result
	for !handler.finished {
		handler.cond.Wait()
	}
	return handler.result, handler.err
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	result, err := kv.handleRequest(args.ClientId, args.CommandId, OpType_GET, args.Key, "VALUE_GET")
	reply.Value = result
	reply.Err = err
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
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
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&kv.dead, 1)
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) encodeState(kvMap map[string]string, lastAppliedCommand map[int64]ExecutedOp) []byte {
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

func (kv *ShardKV) decodeState(data []byte) (kvMap map[string]string, lastAppliedCommand map[int64]ExecutedOp) {
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

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.config = kv.mck.Query(-1)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.logger = log.New(os.Stdout, "", log.Lshortfile|log.Lmicroseconds)
	//kv.logger.SetOutput(ioutil.Discard)

	// You may need initialization code here.
	kv.kvMap = make(map[string]string)
	kv.handlerByClientId = make(map[int64]*ClientHandler)
	kv.lastAppliedCommandMap = make(map[int64]ExecutedOp)
	kv.lastAppliedIndex = 0
	kv.persister = persister

	for k := range kv.handlerByShard {
		kv.handlerByShard[k] = make(map[int64]*ClientHandler)
	}

	go func() {
		for !kv.killed() {
			msg := <-kv.applyCh
			kv.handleApplyMsg(msg)
		}
	}()
	go func() {
		for !kv.killed() {
			t1 := time.Now().UnixMilli()
			kv.handleConfigurationPoll()
			t2 := time.Now().UnixMilli()
			time.Sleep(time.Duration(100-(t2-t1)) * time.Millisecond)
		}
	}()
	return kv
}

func (kv *ShardKV) handleApplyMsg(msg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
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
			handler, present := kv.handlerByClientId[command.ClientId]
			if present && handler.commandId == command.CommandId {
				handler.mu.Lock()
				handler.result = command.Result
				handler.err = OK
				handler.finished = true
				handler.cond.Broadcast()
				handler.mu.Unlock()
			}
		}
		kv.logger.Printf("%v: update lastAppliedIndex: %v\n", kv.me, lastAppliedCommandMap)
	} else if msg.CommandValid {
		command := msg.Command.(Op)

		// update state machine
		result := ""
		lastAppliedCommand := kv.lastAppliedCommandMap[command.ClientId]
		if command.Type == OpType_RECONFIGURE && command.Num != kv.config.Num {
			kv.logger.Printf("%v: try to reconfigure %v\n", kv.me, command.Num)
			cf := kv.mck.Query(command.Num)
			for shard := 0; shard < shardctrler.NShards; shard++ {
				if kv.config.Shards[shard] == kv.gid && cf.Shards[shard] != kv.gid {
					// lost ownership of `shard`, stop process immediately
					kv.logger.Printf("%v: lost ownership of %v, remove: %v.\n", kv.me, shard, kv.handlerByShard)
					for _, handler := range kv.handlerByShard[shard] {
						handler.mu.Lock()
						if !handler.finished || handler.err != OK {
							handler.finished = true
							handler.err = ErrWrongGroup
						}
						handler.cond.Broadcast()
						handler.mu.Unlock()
					}
					kv.handlerByShard[shard] = make(map[int64]*ClientHandler)
				} else if kv.config.Shards[shard] != kv.gid && cf.Shards[shard] == kv.gid {
					// gain ownership of `shard`, get
					kv.logger.Printf("%v: gain ownership of %v.\n", kv.me, shard)

					// TODO: get state from `kv.config.Shards[shard]`
				}
			}
			kv.config = cf
		} else if command.CommandId > lastAppliedCommand.CommandId {
			switch command.Type {
			case OpType_GET:
				result = kv.kvMap[command.Key]
			case OpType_PUT:
				kv.kvMap[command.Key] = command.Value
			case OpType_APPEND:
				kv.kvMap[command.Key] = kv.kvMap[command.Key] + command.Value
			case OpType_RECONFIGURE:
				break
			default:
				kv.logger.Panicf("Could not identify OpType: %v.", command.Type)
			}
			// linearizability: for duplicated command, only the first result is acceptable
			// because other client may invoke PUT/APPEND command
			ec := ExecutedOp{command, result}
			kv.logger.Printf("%v: update %v.\n", kv.me, ec)
			kv.lastAppliedCommandMap[command.ClientId] = ec
		} else if command.CommandId == lastAppliedCommand.CommandId {
			result = lastAppliedCommand.Result
		} else {
			kv.logger.Printf("%v receive msg with invalid command id. msg=%v, lastAppliedMap=%v.", kv.me, msg, lastAppliedCommand.CommandId)
		}

		handler, present := kv.handlerByClientId[command.ClientId]
		if present && handler.commandId <= command.CommandId {
			handler.mu.Lock()
			if handler.commandId == command.CommandId {
				handler.result = result
				handler.err = OK
			} else {
				handler.result = ""
				handler.err = ErrOutdatedRPC
			}
			handler.finished = true
			kv.logger.Printf("%v: make client handler finished, handler=%v.\n", kv.me, handler)
			handler.cond.Broadcast()
			handler.mu.Unlock()
		}
		kv.lastAppliedIndex = msg.CommandIndex
	} else {
		kv.logger.Printf("%v: detect applyCh close, return, msg=%v.\n", kv.me, msg)
		return
	}
	if kv.maxraftstate > 0 && kv.persister.RaftStateSize() > kv.maxraftstate {
		kv.logger.Printf("%v: raft state size greater maxraftstate(%v > %v), trim log.\n", kv.me, kv.persister.RaftStateSize(), kv.maxraftstate)
		snapshot := kv.encodeState(kv.kvMap, kv.lastAppliedCommandMap)
		kv.rf.Snapshot(kv.lastAppliedIndex, snapshot)
	}
}
