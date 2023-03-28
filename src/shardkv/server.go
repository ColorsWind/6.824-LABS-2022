package shardkv

import (
	"6.824/labrpc"
	"6.824/shardctrler"
	"bytes"
	"fmt"
	"log"
	"runtime"
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
// KVState: OpType_RECONFIGURED
// [shardctrler.NShards]int: OpType_GET_STATE
type ExtraArgs interface{}

// ExtraReply actual type list
// string: OpType_GET
// shardctrler.Config: OpType_RECONFIGURING
// KVState: OpType_GET_STATE
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

	clientHandler map[int64]*ClientHandler // only handleRequest will be able to modify

	lastAppliedIndex int

	// persistent state start will Capital
	ConfiguredConfig shardctrler.Config
	CurrState        KVState
	PreConfig        shardctrler.Config
	GetStateMap      map[int]KVState
	AffectShards     [shardctrler.NShards]bool
}

func goroutineName() string {
	buf := make([]byte, 100)
	runtime.Stack(buf, true)
	buf = bytes.Split(buf, []byte{'\n'})[0]
	buf = buf[:len(buf)-1]
	return string(bytes.TrimSuffix(buf, []byte("[running]")))
}

// Only handle OpType_GET, OpType_PUT, OpType_APPEND, OpType_GET_STATE
func (kv *ShardKV) handleRequest(identity Identity, opType OpType, extraArgs ExtraArgs) (extraReply ExtraReply, err Err) {
	kv.mu.Lock()

	handler, present := kv.clientHandler[identity.ClientId]
	lastAppliedCommand := kv.CurrState.LastAppliedCommandMap[identity.ClientId]

	// check if request will eventually complete
	if (!present || handler.commandId != identity.CommandId) && identity.CommandId < lastAppliedCommand.CommandId {
		kv.mu.Unlock()
		switch opType {
		case OpType_GET, OpType_PUT, OpType_APPEND, OpType_GET_STATE:
			// clerk will only send one request at a time
			kv.logger.Panicf("%v-%v: request will not complete, identity=%v, opType=%v, extraArgs=%v, lastAppliedCommand=%v, handler=%v.\n", kv.gid, kv.me, identity, opType, extraArgs, lastAppliedCommand, handler)
		case OpType_RECONFIGURED, OpType_RECONFIGURING:
			kv.logger.Printf("%v-%v: configure request outdated, identity=%v, opType=%v, extraArgs=%v, lastAppliedCommand=%v, handler=%v.\n", kv.gid, kv.me, identity, opType, extraArgs, lastAppliedCommand, handler)
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
		kv.clientHandler[identity.ClientId] = handler
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
						kv.gid, kv.me, currentTerm1, i, kv.CurrState.LastAppliedCommandMap[identity.ClientId].CommandId, op, handler)
					kv.mu.Unlock()
				} else {
					kv.mu.Lock()
					kv.logger.Printf("%v-%v: client handler timeout, term changed(%v -> %v), i=%v, lastAppliedCmd_id=%v, op=%v, handler=%v.\n",
						kv.gid, kv.me, currentTerm, currentTerm1, i, kv.CurrState.LastAppliedCommandMap[identity.ClientId].CommandId, op, handler)
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
	result, err := kv.handleRequest(args.Identity, OpType_GET_STATE, args.GetState)
	reply.Err = err
	if err == OK {
		reply.KVState = result.(KVState)
	}
	kv.logger.Printf("%v-%v: GetState, args=%v, reply=%v.\n", kv.gid, kv.me, args, reply)
}

func (kv *ShardKV) ReConfiguring(args *ReConfiguringArgs, reply *ReConfiguringReply) {
	result, err := kv.handleRequest(args.Identity, OpType_RECONFIGURING, args.Config)
	reply.Err = err
	if err == OK {
		reply.ConfigState = result.(ConfigState)
	}
	kv.logger.Printf("%v-%v: ReConfiguring, args=%v, reply=%v.\n", kv.gid, kv.me, args, reply)

}

func (kv *ShardKV) ReConfigured(args *ReConfiguredArgs, reply *ReConfiguredReply) {
	missingShards, err := kv.handleRequest(args.Identity, OpType_RECONFIGURED, args.PartialConfiguration)
	reply.Err = err
	if err == OK {
		reply.MissingShards = missingShards.([]int)
	}
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

}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) encodeState(preConfig shardctrler.Config, currConfig shardctrler.Config, currState KVState, getStateMap map[int]KVState, affectShards [shardctrler.NShards]bool) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(preConfig); err != nil {
		kv.logger.Panicf("%v-%v: fail to encode preConfig: %v, err=%v.", kv.gid, kv.me, preConfig, err)
	}
	if err := e.Encode(currConfig); err != nil {
		kv.logger.Panicf("%v-%v: fail to encode currConfig: %v, err=%v.", kv.gid, kv.me, currConfig, err)
	}
	if err := e.Encode(currState); err != nil {
		kv.logger.Panicf("%v-%v: fail to encode currState: %v, err=%v.", kv.gid, kv.me, currState, err)
	}
	if err := e.Encode(getStateMap); err != nil {
		kv.logger.Panicf("%v-%v: fail to encode getStateMap: %v, err=%v.", kv.gid, kv.me, getStateMap, err)
	}
	if err := e.Encode(affectShards); err != nil {
		kv.logger.Panicf("%v-%v: fail to encode affectShards: %v, err=%v.", kv.gid, kv.me, getStateMap, err)
	}
	return w.Bytes()
}

func (kv *ShardKV) decodeState(data []byte) (preConfig shardctrler.Config, currConfig shardctrler.Config, currState KVState, getStateMap map[int]KVState, affectShards [shardctrler.NShards]bool) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if err := d.Decode(&preConfig); err != nil {
		kv.logger.Panicf("%v-%v: fail to decode preConfig, Err=%v", kv.gid, kv.me, err)
	}
	if err := d.Decode(&currConfig); err != nil {
		kv.logger.Panicf("%v-%v: fail to decode currConfig, Err=%v", kv.gid, kv.me, err)
	}
	if err := d.Decode(&currState); err != nil {
		kv.logger.Panicf("%v-%v: fail to decode currState, Err=%v", kv.gid, kv.me, err)
	}
	if err := d.Decode(&getStateMap); err != nil {
		kv.logger.Panicf("%v-%v: fail to decode getStateMap, Err=%v", kv.gid, kv.me, err)
	}
	if err := d.Decode(&affectShards); err != nil {
		kv.logger.Panicf("%v-%v: fail to decode affectShards, Err=%v", kv.gid, kv.me, err)
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
// ConfiguredConfig.Groups[gid][i] into a labrpc.ClientEnd on which you can
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
	labgob.Register(KVState{})
	labgob.Register(PartialConfiguration{})
	labgob.Register(KeyValue{})
	labgob.Register(GetState{})
	labgob.Register(ConfigState{})

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

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.logger = log.New(getLogOutput(), "", log.Lshortfile|log.Lmicroseconds)

	// You may need initialization code here.

	kv.clientHandler = make(map[int64]*ClientHandler)
	kv.lastAppliedIndex = 0
	kv.persister = persister
	kv.CurrState = KVState{
		ConfiguredNum:         0,
		KVMap:                 make(map[string]string),
		LastAppliedCommandMap: make(map[int64]ExecutedOp),
	}
	kv.PreConfig = shardctrler.Config{}
	kv.ConfiguredConfig = shardctrler.Config{}
	kv.GetStateMap = make(map[int]KVState)

	//for k := range kv.handlerByShard {
	//	kv.handlerByShard[k] = make(map[int64]*ClientHandler)
	//}

	kv.logger.Printf("%v-%v: shardkv startup, name=%v.\n", kv.gid, kv.me, goroutineName())
	go func() {
		kv.logger.Printf("%v-%v: onApplyMsg start, name=%v.\n", kv.gid, kv.me, goroutineName())
		for !kv.killed() {
			msg := <-kv.applyCh
			kv.onApplyMsg(msg)
		}
	}()
	go func() {
		cck := MakeConfigureClerk(kv)
		kv.logger.Printf("%v-%v: onPollConfiguration start, name=%v.\n", kv.gid, kv.me, goroutineName())
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
	if msg.SnapshotValid {
		for {
			if !msg.SnapshotValid && !msg.CommandValid {
				kv.logger.Printf("%v-%v: detect applyCh close while receive snapshot, return, msg=%v.\n", kv.gid, kv.me, msg)
				return
			}
			if msg.SnapshotValid && kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.CommandIndex, msg.Snapshot) {
				break
			}
			msg = <-kv.applyCh
		}
		kv.mu.Lock()
		defer kv.mu.Unlock()
		// receive snapshot indicate is not leader
		kv.PreConfig, kv.ConfiguredConfig, kv.CurrState, kv.GetStateMap, kv.AffectShards = kv.decodeState(msg.Snapshot)
		kv.lastAppliedIndex = msg.SnapshotIndex
		for _, command := range kv.CurrState.LastAppliedCommandMap {
			handler, present := kv.clientHandler[command.ClientId]
			if present && handler.commandId == command.CommandId {
				handler.mu.Lock()
				handler.result = command.Result
				handler.Err = command.Err
				handler.finished = true
				handler.cond.Broadcast()
				handler.mu.Unlock()
			}
		}
		kv.logger.Printf("%v-%v: update lastAppliedIndex: %v\n", kv.gid, kv.me, kv.CurrState.LastAppliedCommandMap)
	} else if msg.CommandValid {
		kv.mu.Lock()
		command := msg.Command.(Op)
		lastAppliedCommand := kv.CurrState.LastAppliedCommandMap[command.ClientId]
		// update state machine
		if command.CommandId > lastAppliedCommand.CommandId {
			var result ExtraReply
			switch command.Type {
			case OpType_GET, OpType_PUT, OpType_APPEND:
				keyValue := command.ExtraArgs.(KeyValue)
				keyShard := key2shard(keyValue.Key)
				if kv.ConfiguredConfig.Num != kv.PreConfig.Num && kv.AffectShards[keyShard] == true {
					result = ErrNotAvailableYet
					kv.logger.Printf("%v-%v: current configuring, curr=%v, pre=%v, affected=%v, shard=%v, unable to handle command: %v.\n", kv.gid, kv.me, kv.ConfiguredConfig, kv.PreConfig, kv.AffectShards, keyShard, command)
				} else if configGid := kv.PreConfig.Shards[keyShard]; configGid != kv.gid {
					result = ErrWrongGroup
					kv.logger.Printf("%v-%v: wrong group, unable to handle command: %v, keyShard=%v, configGid=%v.\n", kv.gid, kv.me, command, keyShard, configGid)
				} else {
					switch command.Type {
					case OpType_GET:
						result = KeyValue{"Key_GET", kv.CurrState.KVMap[keyValue.Key]}
					case OpType_PUT:
						kv.CurrState.KVMap[keyValue.Key] = keyValue.Value
					case OpType_APPEND:
						kv.CurrState.KVMap[keyValue.Key] = kv.CurrState.KVMap[keyValue.Key] + keyValue.Value
					}
				}
			case OpType_GET_STATE:
				// confirm get state
				getState := command.ExtraArgs.(GetState)
				if getState.Confirm {
					for _, shard := range getState.Shards {
						state, present := kv.GetStateMap[shard]
						if !present {
							kv.logger.Printf("%v-%v: unable to confirm get, not exist shard %v, getStateMap=%v.\n", kv.gid, kv.me, shard, kv.GetStateMap)
							result = ErrShardDelete
						} else if state.ConfiguredNum != getState.ConfigNum {
							kv.logger.Panicf("%v-%v: unable to confirm get, configure num != %v, getStateMap=%v.\n", kv.gid, kv.me, getState.ConfigNum, kv.GetStateMap)
							result = ErrShardDelete
						} else {
							delete(kv.GetStateMap, shard)
							kv.logger.Printf("%v-%v: delete state shard %v, getStateMap=%v.\n", kv.gid, kv.me, shard, kv.GetStateMap)
						}
					}
					if result == nil {
						result = KVState{}
					}
				} else if kv.PreConfig.Num < getState.ConfigNum+1 {
					// get state, but config fall behind, state not created
					result = ErrShardCreate
				} else {
					getStateKVMap := make(map[string]string)
					getStateAppliedMap := make(map[int64]ExecutedOp)
					for _, shard := range getState.Shards {
						state, present := kv.GetStateMap[shard]
						if !present {
							kv.logger.Printf("%v-%v: unable to get state, not exist shard %v, getStateMap=%v, currConfig=%v.\n", kv.gid, kv.me, shard, kv.GetStateMap, kv.ConfiguredConfig)
							result = ErrShardDelete
							break
						} else if state.ConfiguredNum != getState.ConfigNum {
							kv.logger.Printf("%v-%v: unable to get state, configure num != %v, getStateMap=%v, currConfig=%v.\n", kv.gid, kv.me, getState.ConfigNum, kv.GetStateMap, kv.ConfiguredConfig)
							result = ErrShardDelete
							break
						} else {
							for key, value := range state.KVMap {
								getStateKVMap[key] = value
							}
							for key, value := range state.LastAppliedCommandMap {
								getStateAppliedMap[key] = value
							}
						}
					}
					if result == nil {
						result = KVState{
							ConfiguredNum:         getState.ConfigNum,
							KVMap:                 getStateKVMap,
							LastAppliedCommandMap: getStateAppliedMap,
						}
					}
				}
			case OpType_RECONFIGURING:
				newConfig := command.ExtraArgs.(shardctrler.Config)
				kv.logger.Printf("%v-%v: reconfiguring request, command=%v, currConfig=%v, preConfig=%v.\n", kv.gid, kv.me, command, kv.ConfiguredConfig, kv.PreConfig)
				if kv.PreConfig.Num == kv.ConfiguredConfig.Num && newConfig.Num == kv.ConfiguredConfig.Num+1 {
					result = ConfigState{true, false, kv.ConfiguredConfig}
					for shard := 0; shard < shardctrler.NShards; shard++ {
						lastGid := kv.ConfiguredConfig.Shards[shard]
						currGid := newConfig.Shards[shard]
						if lastGid != 0 && lastGid != kv.gid && currGid == kv.gid {
							kv.AffectShards[shard] = true
							kv.logger.Printf("%v-%v: gain ownership of shard %v, currConfig=%v, gid=%v.\n", kv.gid, kv.me, shard, kv.ConfiguredConfig, lastGid)
						} else if lastGid == kv.gid && currGid != kv.gid {
							element, present := kv.GetStateMap[shard]
							if present {
								kv.logger.Printf("%v-%v: try to make a map of shard %v, but already exist, command=%v, element={%v}, pre_config=%v, curr_config=%v.\n", kv.gid, kv.me, shard, command, element, kv.PreConfig, kv.ConfiguredConfig)
							}
							getStateKVMap := make(map[string]string)
							getStateAppliedMap := make(map[int64]ExecutedOp)
							// TODO fast iteration, reduce time complexity
							for key, value := range kv.CurrState.KVMap {
								if key2shard(key) == shard {
									getStateKVMap[key] = value
									delete(kv.CurrState.KVMap, key)
								}
							}
							for key, value := range kv.CurrState.LastAppliedCommandMap {
								if keyValue, ok := value.ExtraArgs.(KeyValue); ok && key2shard(keyValue.Key) == shard {
									getStateAppliedMap[key] = value
									delete(kv.CurrState.LastAppliedCommandMap, key)
								}
							}
							state := KVState{
								ConfiguredNum:         kv.ConfiguredConfig.Num,
								KVMap:                 getStateKVMap,
								LastAppliedCommandMap: getStateAppliedMap,
							}
							kv.GetStateMap[shard] = state
							kv.AffectShards[shard] = false
							kv.logger.Printf("%v-%v: lost ownership of shard %v, currConfig=%v, make map, new_gid=%v, make_state={%v}.\n", kv.gid, kv.me, shard, kv.ConfiguredConfig, currGid, state)
						} else {
							kv.AffectShards[shard] = false
						}
					}
					kv.PreConfig = newConfig
					kv.logger.Printf("%v-%v: update PreConfig.Num to %v.\n", kv.gid, kv.me, kv.PreConfig.Num)
				} else {
					if kv.PreConfig.Num != kv.ConfiguredConfig.Num {
						kv.logger.Printf("%v-%v: already in configuring state, command=%v, pre_config=%v, curr_config=%v.\n", kv.gid, kv.me, command, kv.PreConfig, kv.ConfiguredConfig)
					} else if kv.ConfiguredConfig.Num == newConfig.Num {
						kv.logger.Printf("%v-%v: configuring already submitted, command=%v, pre_config=%v, curr_config=%v.\n", kv.gid, kv.me, command, kv.PreConfig, kv.ConfiguredConfig)
					} else if kv.ConfiguredConfig.Num > newConfig.Num {
						kv.logger.Printf("%v-%v: configuring is outdated, command=%v, pre_config=%v, curr_config=%v.\n", kv.gid, kv.me, command, kv.PreConfig, kv.ConfiguredConfig)
					} else if newConfig.Num > kv.ConfiguredConfig.Num+1 {
						kv.logger.Printf("%v-%v: configuring is too new, command=%v, pre_config=%v, curr_config=%v.\n", kv.gid, kv.me, command, kv.PreConfig, kv.ConfiguredConfig)
					} else {
						kv.logger.Panicf("%v-%v: unknown status, may be a bug, command=%v, pre_config=%v, curr_config=%v.\n", kv.gid, kv.me, command, kv.PreConfig, kv.ConfiguredConfig)
					}
					result = ConfigState{false, kv.PreConfig.Num == kv.ConfiguredConfig.Num, kv.ConfiguredConfig}
				}
			case OpType_RECONFIGURED:
				partialState := command.ExtraArgs.(PartialConfiguration)
				if kv.ConfiguredConfig.Num == kv.PreConfig.Num && kv.ConfiguredConfig.Num == partialState.ConfiguredNum {
					kv.logger.Printf("%v-%v: configured accept(duplicated). partialState={%v}, curr_config=%v, pre_config=%v.\n", kv.gid, kv.me, partialState, kv.ConfiguredConfig, kv.PreConfig)
					result = []int{}
				} else if kv.ConfiguredConfig.Num == kv.PreConfig.Num && kv.ConfiguredConfig.Num != partialState.ConfiguredNum {
					kv.logger.Printf("%v-%v: current is not configuring status. partialState={%v}, curr_config=%v, pre_config=%v.\n", kv.gid, kv.me, partialState, kv.ConfiguredConfig, kv.PreConfig)
					result = ErrMatchConfigured
				} else if kv.PreConfig.Num != partialState.ConfiguredNum {
					kv.logger.Printf("%v-%v: configured not matched. partialState={%v}, curr_config=%v, pre_config=%v.\n", kv.gid, kv.me, partialState, kv.ConfiguredConfig, kv.PreConfig)
					result = ErrMatchConfigured
				} else {
					var duplicated bool
					if len(partialState.Shards) > 0 {
						fetched := kv.AffectShards[partialState.Shards[0]]
						for _, shard := range partialState.Shards {
							if kv.AffectShards[shard] != fetched {
								kv.logger.Panicf("%v-%v: inconsistent configured state. partialState={%v}, curr_config=%v, pre_config=%v, affected=%v.\n", kv.gid, kv.me, partialState, kv.ConfiguredConfig, kv.PreConfig, kv.AffectShards)
							}
							kv.AffectShards[shard] = false
						}
						duplicated = !fetched
					} else {
						duplicated = false
					}

					var missingShards []int
					for shard, isAffected := range kv.AffectShards {
						if isAffected {
							missingShards = append(missingShards, shard)
						}
					}
					if duplicated {
						kv.logger.Printf("%v-%v: receive duplicated partialState, already updated. partialState={%v}, curr_config=%v, pre_config=%v, affected_shards=%v.\n", kv.gid, kv.me, partialState, kv.ConfiguredConfig, kv.PreConfig, kv.AffectShards)
					} else {
						for key, value := range partialState.KVMap {
							kv.CurrState.KVMap[key] = value
						}
						for key, value := range partialState.LastAppliedCommandMap {
							kv.CurrState.LastAppliedCommandMap[key] = value
						}
						if len(missingShards) == 0 {
							kv.ConfiguredConfig = kv.PreConfig
							kv.logger.Printf("%v-%v: missingShards is empty, update ConfiguredConfig.Num to %v.\n", kv.gid, kv.me, kv.ConfiguredConfig.Num)
						} else {
							kv.logger.Printf("%v-%v: still wait for state, missingShards=%v, partialState=%v.\n", kv.gid, kv.me, missingShards, partialState)
						}
					}
					result = missingShards
				}

			}
			lastAppliedCommand = NewExecuteOp(command, result)
			switch command.Type {
			case OpType_PUT, OpType_GET, OpType_APPEND:

				if _, containsErr := result.(Err); !containsErr {
					// only successfully command need to deduplicate
					kv.CurrState.LastAppliedCommandMap[command.ClientId] = lastAppliedCommand
				}
			default:
				// other operation use ConfigNum to detect duplicate
				break
			}

		} else if command.CommandId == lastAppliedCommand.CommandId {
			// ignore, use lastAppliedCommand
			// result = lastAppliedCommand.Result
		} else {
			kv.logger.Panicf("%v-%v: receive msg with invalid command id. msg=%v, lastAppliedMap=%v.", kv.gid, kv.me, msg, lastAppliedCommand.CommandId)
		}

		kv.lastAppliedIndex = msg.CommandIndex
		handler, present := kv.clientHandler[command.ClientId]

		if present && command.CommandId >= handler.commandId {
			handler.mu.Lock()
			if !handler.finished {
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
			}
			handler.mu.Unlock()
		}

		if kv.maxraftstate > 0 && kv.persister.RaftStateSize() > kv.maxraftstate {
			kv.logger.Printf("%v-%v: raft state size greater maxraftstate(%v > %v), trim log.\n", kv.gid, kv.me, kv.persister.RaftStateSize(), kv.maxraftstate)
			snapshot := kv.encodeState(kv.PreConfig, kv.ConfiguredConfig, kv.CurrState, kv.GetStateMap, kv.AffectShards)
			kv.rf.Snapshot(kv.lastAppliedIndex, snapshot)
		}
		kv.mu.Unlock()
	} else {
		kv.logger.Printf("%v-%v: detect applyCh close, return, msg=%v.\n", kv.gid, kv.me, msg)
		return
	}

}
