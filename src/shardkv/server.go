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
	OpType_GET           = "OpType_GET"
	OpType_PUT           = "OpType_PUT"
	OpType_APPEND        = "OpType_APPEND"
	OpType_RECONFIGURING = "OpType_RECONFIGURING"
	OpType_RECONFIGURED  = "OpType_RECONFIGURED"
	OpType_GET_STATE     = "OpType_GET_STATE"
)

// ExtraArgs actual type list
// KeyValue: OpType_GET, OpType_PUT, OpType_APPEND
// shardctrler.Config: OpType_RECONFIGURING
// State: OpType_RECONFIGURED
// [shardctrler.NShards]int: OpType_GET_STATE
type ExtraArgs interface{}

// ExtraReply actual type list
// string: OpType_GET
// shardctrler.Config: OpType_RECONFIGURING
// State: OpType_GET_STATE
type ExtraReply interface{}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Identity
	Type      OpType
	ExtraArgs ExtraArgs
}

func (op Op) String() string {
	return fmt.Sprintf("{%v, type=%v, extra={%v}}", op.Identity, op.Type, op.ExtraArgs)
}

type ExecutedOp struct {
	Op
	Result ExtraReply
	Err    Err
}

func NewExecuteOp(op Op, result ExtraReply) ExecutedOp {
	err, ok := result.(Err)
	if ok {
		return ExecutedOp{op, nil, err}
	} else {
		return ExecutedOp{op, result, OK}
	}
}

func (ec ExecutedOp) String() string {
	return fmt.Sprintf("{op=%v, result=%v}", ec.Op, raft.ToStringLimited(ec.Result, 100))
}

type ClientHandler struct {
	mu        sync.Mutex
	cond      *sync.Cond
	clientId  int64
	commandId int64
	finished  bool
	result    interface{}
	Err       Err
}

func (handler *ClientHandler) String() string {
	if handler.mu.TryLock() {
		defer handler.mu.Unlock()
	}
	return fmt.Sprintf("{client_id=%v, cmd_id=%v, finished=%v, result=%v, Err=%v}", handler.clientId, handler.commandId, handler.finished, handler.result, handler.Err)
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
	State
	handlerByClientId map[int64]*ClientHandler // only handleRequest will be able to modify
	//handlerByShard    [shardctrler.NShards]map[int64]*ClientHandler // only handleRequest will be able to modify

	lastAppliedIndex int

	config shardctrler.Config
}

// Only handle OpType_GET, OpType_PUT, OpType_APPEND, OpType_GET_STATE
func (kv *ShardKV) handleRequest(identity Identity, opType OpType, extraArgs ExtraArgs) (extraReply ExtraReply, err Err) {
	kv.mu.Lock()

	handler, present := kv.handlerByClientId[identity.ClientId]
	lastAppliedCommand := kv.LastAppliedCommandMap[identity.ClientId]

	// check if request will eventually complete
	if (!present || handler.commandId != identity.CommandId) && identity.CommandId < lastAppliedCommand.CommandId {
		kv.mu.Unlock()
		switch opType {
		case OpType_GET, OpType_PUT, OpType_APPEND, OpType_GET_STATE:
			// clerk will only send one request at a time
			kv.logger.Panicf("%v-%v: request will not complete, identity=%v, opType=%v, extraArgs=%v, lastAppliedCommand=%v, handler=%v.\n", kv.gid, kv.me, identity, opType, extraArgs, lastAppliedCommand, handler)
		case OpType_RECONFIGURED, OpType_RECONFIGURING:
			//kv.logger.Printf("%v-%v: configure request outdated, identity=%v, opType=%v, extraArgs=%v, lastAppliedCommand=%v, handler=%v.\n", kv.gid, kv.me, identity, opType, extraArgs, lastAppliedCommand, handler)
		}
		err = ErrOutdatedRPC
		return
	}

	if !present || handler.commandId < identity.CommandId {
		handler = &ClientHandler{
			clientId:  identity.ClientId,
			commandId: identity.CommandId,
			Err:       ErrNotStarted,
			finished:  true,
		}
		handler.cond = sync.NewCond(&handler.mu)
		kv.handlerByClientId[identity.ClientId] = handler
	}

	kv.mu.Unlock()
	// if already finish
	handler.mu.Lock()
	defer handler.mu.Unlock()

	if lastAppliedCommand.CommandId == handler.commandId && handler.Err != OK {
		handler.result = lastAppliedCommand.Result
		handler.Err = lastAppliedCommand.Err
		handler.finished = true
		handler.cond.Broadcast()

	}

	// if should retry
	if handler.finished && handler.Err != OK {
		op := Op{
			Identity:  identity,
			Type:      opType,
			ExtraArgs: extraArgs,
		}
		_, currentTerm, isLeader := kv.rf.Start(op)
		if !isLeader {
			err = ErrWrongLeader
			return
		}
		handler.finished = false
		handler.Err = WaitComplete

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
					kv.logger.Printf("%v-%v: client handler killed, op=%v, handler=%v.\n",
						kv.gid, kv.me, op, handler)
					handler.Err = ErrShutdown
					handler.finished = true
					handler.cond.Broadcast()
					handler.mu.Unlock()
				}
				currentTerm1, _ := kv.rf.GetState()
				if currentTerm == currentTerm1 {
					kv.mu.Lock()
					kv.logger.Printf("%v-%v: client handler continue to wait, currentTerm=%v, i=%v, lastAppliedCmd_id=%v, op=%v, handler=%v.\n",
						kv.gid, kv.me, currentTerm1, i, kv.LastAppliedCommandMap[identity.ClientId].CommandId, op, handler)
					kv.mu.Unlock()
				} else {
					kv.mu.Lock()
					kv.logger.Printf("%v-%v: client handler timeout, term changed(%v -> %v), i=%v, lastAppliedCmd_id=%v, op=%v, handler=%v.\n",
						kv.gid, kv.me, currentTerm, currentTerm1, i, kv.LastAppliedCommandMap[identity.ClientId].CommandId, op, handler)
					kv.mu.Unlock()
					handler.mu.Lock()
					handler.Err = ErrWrongLeader
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
	extraReply = handler.result
	err = handler.Err
	return
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	result, err := kv.handleRequest(args.Identity, OpType_GET, KeyValue{args.Key, "Value_GET"})
	reply.Err = err
	if err == OK {
		keyValue := result.(KeyValue)
		reply.Value = keyValue.Value
	}
	kv.logger.Printf("%v-%v: Get, args=%v, reply=%v.\n", kv.gid, kv.me, args, reply)
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
	_, err := kv.handleRequest(args.Identity, opType, args.KeyValue)
	reply.Err = err
	kv.logger.Printf("%v-%v: PutAppend, args=%v, reply=%v.\n", kv.gid, kv.me, args, reply)
}

func (kv *ShardKV) GetState(args *GetStateArgs, reply *GetStateReply) {
	// Your code here.
	result, err := kv.handleRequest(args.Identity, OpType_GET_STATE, args.Shards)
	reply.Err = err
	if err == OK {
		reply.State = result.(State)
	}
	kv.logger.Printf("%v-%v: GetState, args=%v, reply=%v.\n", kv.gid, kv.me, args, reply)
}

func (kv *ShardKV) ReConfiguring(args *ReConfiguringArgs, reply *ReConfiguringReply) {
	result, err := kv.handleRequest(args.Identity, OpType_RECONFIGURING, args.Config)
	reply.Err = err
	//fmt.Printf("%v, %v\n", result, err)
	if err == OK {
		reply.LastConfig = result.(shardctrler.Config)
	}
	if reply.Err != ErrOutdatedRPC {
		kv.logger.Printf("%v-%v: ReConfiguring, args=%v, reply=%v.\n", kv.gid, kv.me, args, reply)
	}

}

func (kv *ShardKV) ReConfigured(args *ReConfiguredArgs, reply *ReConfiguredReply) {
	_, err := kv.handleRequest(args.Identity, OpType_RECONFIGURED, args.State)
	reply.Err = err
	kv.logger.Printf("%v-%v: ReConfigured, args=%v, reply=%v.\n", kv.gid, kv.me, args, reply)
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
	go func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()

	}()

}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) encodeState(config shardctrler.Config, state State) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(config); err != nil {
		kv.logger.Panicf("%v-%v: fail to encode config: %v.", kv.gid, kv.me, config)
	}
	if err := e.Encode(state); err != nil {
		kv.logger.Panicf("%v-%v: fail to encode state: %v.", kv.gid, kv.me, state)
	}
	return w.Bytes()
}

func (kv *ShardKV) decodeState(data []byte) (config shardctrler.Config, state State) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if err := d.Decode(&config); err != nil {
		kv.logger.Panicf("%v-%v: fail to decode config, Err=%v", kv.gid, kv.me, err)
	}
	if err := d.Decode(&state); err != nil {
		kv.logger.Panicf("%v-%v: fail to decode state, Err=%v", kv.gid, kv.me, err)
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
	labgob.Register(shardctrler.Config{})
	labgob.Register(State{})
	labgob.Register(KeyValue{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	atomic.StoreInt32(&kv.dead, 0)
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.ConfigNum = 0
	kv.config = kv.mck.Query(kv.ConfigNum)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.logger = log.New(os.Stdout, "", log.Lshortfile|log.Lmicroseconds)
	//kv.logger.SetOutput(ioutil.Discard)

	// You may need initialization code here.
	kv.KVMap = make(map[string]string)
	kv.handlerByClientId = make(map[int64]*ClientHandler)
	kv.LastAppliedCommandMap = make(map[int64]ExecutedOp)
	kv.lastAppliedIndex = 0
	kv.persister = persister

	//for k := range kv.handlerByShard {
	//	kv.handlerByShard[k] = make(map[int64]*ClientHandler)
	//}

	kv.logger.Printf("%v-%v: shardkv startup.\n", kv.gid, kv.me)
	go func() {
		for !kv.killed() {
			msg := <-kv.applyCh
			kv.onApplyMsg(msg)
		}
	}()
	go func() {
		cck := MakeConfigureClerk(kv)
		for !kv.killed() {
			t1 := time.Now().UnixMilli()
			cck.onPollConfiguration()
			t2 := time.Now().UnixMilli()
			time.Sleep(time.Duration(100-(t2-t1)) * time.Millisecond)
		}
	}()
	return kv
}

func (kv *ShardKV) onApplyMsg(msg raft.ApplyMsg) {
	kv.logger.Printf("%v-%v: Receive msg: %v.\n", kv.gid, kv.me, msg)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if msg.SnapshotValid {
		for {
			if msg.SnapshotValid && kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.CommandIndex, msg.Snapshot) {
				break
			}
			msg = <-kv.applyCh
		}
		// receive snapshot indicate is not leader
		config, state := kv.decodeState(msg.Snapshot)
		kv.State = state
		kv.config = config
		kv.lastAppliedIndex = msg.SnapshotIndex
		for _, command := range state.LastAppliedCommandMap {
			handler, present := kv.handlerByClientId[command.ClientId]
			if present && handler.commandId == command.CommandId {
				handler.mu.Lock()
				handler.result = command.Result
				handler.Err = command.Err
				handler.finished = true
				handler.cond.Broadcast()
				handler.mu.Unlock()
			}
		}
		kv.logger.Printf("%v-%v: update lastAppliedIndex: %v\n", kv.gid, kv.me, state.LastAppliedCommandMap)
	} else if msg.CommandValid {
		command := msg.Command.(Op)
		lastAppliedCommand := kv.LastAppliedCommandMap[command.ClientId]
		// update state machine
		var result ExtraReply
		if command.CommandId > lastAppliedCommand.CommandId {
			switch command.Type {
			case OpType_GET, OpType_PUT, OpType_APPEND:
				keyValue := command.ExtraArgs.(KeyValue)
				keyShard := key2shard(keyValue.Key)
				if kv.ConfigNum != kv.config.Num {
					// TODO: make a list of affected Shard
					result = ErrNotAvailableYet
					kv.logger.Printf("%v-%v: current configuring, unable to handle command: %v.\n", kv.gid, kv.me, command)
				} else if configGid := kv.config.Shards[keyShard]; configGid != kv.gid {
					result = ErrWrongGroup
					kv.logger.Printf("%v-%v: wrong group, unable to handle command: %v, keyShard=%v, configGid=%v.\n", kv.gid, kv.me, command, keyShard, configGid)
				} else {
					switch command.Type {
					case OpType_GET:
						result = KeyValue{"Key_GET", kv.KVMap[keyValue.Key]}
					case OpType_PUT:
						kv.KVMap[keyValue.Key] = keyValue.Value
					case OpType_APPEND:
						kv.KVMap[keyValue.Key] = kv.KVMap[keyValue.Key] + keyValue.Value
					}
				}
			case OpType_GET_STATE:
				// check lose shard ownership
				shards := command.ExtraArgs.([]int)
				getStateKVMap := make(map[string]string)
				getStateAppliedMap := make(map[int64]ExecutedOp)
				shardMap := make(map[int]int)
				noRace := true
				for _, shard := range shards {
					if kv.config.Shards[shard] == kv.me {
						log.Printf("%v-%v: receive GetState msg, but it still owns shard %v, ctrlerConfig=%v.", kv.gid, kv.me, shard, kv.config)
						noRace = false
						break
					}
					shardMap[shard] = 1
				}
				if noRace {
					for key, value := range kv.KVMap {
						if _, present := shardMap[key2shard(key)]; present {
							getStateKVMap[key] = value
						}
					}
					for key, value := range kv.LastAppliedCommandMap {
						switch value.Type {
						case OpType_GET:
							if _, present := shardMap[key2shard(value.ExtraArgs.(string))]; present {
								getStateAppliedMap[key] = value
							}
						case OpType_PUT, OpType_APPEND:
							if _, present := shardMap[key2shard(value.ExtraArgs.(KeyValue).Key)]; present {
								getStateAppliedMap[key] = value
							}
						}

					}
					result = State{
						KVMap:                 getStateKVMap,
						LastAppliedCommandMap: getStateAppliedMap,
					}
				} else {
					result = ErrGetStateRacing
				}
			case OpType_RECONFIGURING:
				newConfig := command.ExtraArgs.(shardctrler.Config)
				if kv.config.Num < newConfig.Num {
					result = kv.config
					kv.config = newConfig
					kv.config = command.ExtraArgs.(shardctrler.Config)
					kv.logger.Printf("%v-%v: update current config, curr=%v, args=%v.\n", kv.gid, kv.me, kv.config, newConfig)
				} else {
					kv.logger.Printf("%v-%v: current config is as least up-to-date, curr=%v, args=%v.\n", kv.gid, kv.me, kv.config, newConfig)
					result = ErrMatchConfiguring
				}
			case OpType_RECONFIGURED:
				state := command.ExtraArgs.(State)
				if kv.config.Num == state.ConfigNum {
					kv.ConfigNum = state.ConfigNum
					for key, value := range state.KVMap {
						kv.KVMap[key] = value
					}
					for key, value := range state.LastAppliedCommandMap {
						kv.LastAppliedCommandMap[key] = value
					}
					kv.logger.Printf("%v-%v: update configNum to %v.\n", kv.gid, kv.me, state.ConfigNum)
				} else {
					kv.logger.Printf("%v-%v: config not match. state=%v, config=%v.\n", kv.gid, kv.me, state, kv.config)
					result = ErrMatchConfigured
				}

			}
			lastAppliedCommand = NewExecuteOp(command, result)
			kv.LastAppliedCommandMap[command.ClientId] = lastAppliedCommand
		} else if command.CommandId == lastAppliedCommand.CommandId {
			result = lastAppliedCommand.Result
		} else {
			kv.logger.Panicf("%v-%v: receive msg with invalid command id. msg=%v, lastAppliedMap=%v.", kv.gid, kv.me, msg, lastAppliedCommand.CommandId)
		}

		handler, present := kv.handlerByClientId[command.ClientId]
		if present && handler.commandId >= command.CommandId {
			handler.mu.Lock()
			if handler.commandId == command.CommandId {
				handler.result = lastAppliedCommand.Result
				handler.Err = lastAppliedCommand.Err
			} else {
				handler.result = nil
				handler.Err = ErrOutdatedRPC
				kv.logger.Printf("%v-%v: outdated RPC, handler=%v, op=%v", kv.gid, kv.me, handler, command)
			}
			handler.finished = true
			kv.logger.Printf("%v-%v: make client handler finished, handler=%v.\n", kv.gid, kv.me, handler)
			handler.cond.Broadcast()
			handler.mu.Unlock()
		}
		kv.lastAppliedIndex = msg.CommandIndex

	} else {
		kv.logger.Printf("%v-%v: detect applyCh close, return, msg=%v.\n", kv.gid, kv.me, msg)
		return
	}
	if kv.maxraftstate > 0 && kv.persister.RaftStateSize() > kv.maxraftstate {
		kv.logger.Printf("%v-%v: raft state size greater maxraftstate(%v > %v), trim log.\n", kv.gid, kv.me, kv.persister.RaftStateSize(), kv.maxraftstate)
		snapshot := kv.encodeState(kv.config, kv.State)
		kv.rf.Snapshot(kv.lastAppliedIndex, snapshot)
	}
}
