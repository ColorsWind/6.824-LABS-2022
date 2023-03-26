package shardkv

import (
	"6.824/raft"
	"6.824/shardctrler"
	"fmt"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK                  = Err("OK")
	ErrNoKey            = Err("ErrNoKey")
	ErrWrongLeader      = Err("ErrWrongLeader")
	ErrMatchConfiguring = Err("ErrMatchConfiguring")
	ErrMatchConfigured  = Err("ErrMatchConfigured")

	WaitComplete      = Err("WaitComplete")
	ErrOutdatedRPC    = Err("ErrOutdatedRPC")
	ErrGetStateRacing = Err("ErrGetStateRacing")
	ErrApplySnapshot  = Err("ErrApplySnapshot")
	ErrTimeout        = Err("ErrTimeout")
	ErrNotStarted     = Err("ErrNotStarted")
	ErrShutdown       = Err("ErrShutdown")

	ErrWrongGroup      = Err("ErrWrongGroup")
	ErrNotAvailableYet = Err("ErrNotAvailableYet")

	ErrShardDelete = Err("ErrShardDelete")
)

type Err string

type Identity struct {
	ClientId  int64
	CommandId int64
}

func (id Identity) String() string {
	return fmt.Sprintf("client_id=%v, cmd_id=%v", id.ClientId, id.CommandId)
}

type KeyValue struct {
	Key   string
	Value string
}

func (kv KeyValue) String() string {
	return fmt.Sprintf("key=%v, value=%v", kv.Key, raft.ToStringLimited(kv.Value, 10))
}

type State struct {
	ConfiguredNum         int
	KVMap                 map[string]string
	LastAppliedCommandMap map[int64]ExecutedOp
}

func (st State) String() string {
	return fmt.Sprintf("num=%v, kvMap=%v, lastAppliedCommandMap=%v", st.ConfiguredNum, st.KVMap, st.LastAppliedCommandMap)
}

//type GetStateOK struct {
//	ConfiguredNum int
//	Shards        []int
//}
//
//func (gs GetStateOK) String() string {
//	return fmt.Sprintf("num=%v, shards=%v", gs.ConfiguredNum, gs.Shards)
//}

type GetState struct {
	ConfigNum int
	Shards    []int
	Confirm   bool
}

func (gs GetState) String() string {
	return fmt.Sprintf("num=%v, shards=%v, confirm=%v", gs.ConfigNum, gs.Shards, gs.Confirm)
}

type ConfigState struct {
	Update     bool
	Completed  bool
	LastConfig shardctrler.Config
}

func (cs ConfigState) String() string {
	return fmt.Sprintf("update=%v, configuring=%v, last_config=%v", cs.Update, cs.Completed, cs.LastConfig)
}

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Identity
	KeyValue
	Op string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

func (args PutAppendArgs) String() string {
	return fmt.Sprintf("{%v, %v, op=%v}", args.Identity, args.KeyValue, args.Op)
}

type PutAppendReply struct {
	Err Err
}

func (reply PutAppendReply) String() string {
	return fmt.Sprintf("{Err=%v}", reply.Err)
}

type GetArgs struct {
	Identity
	Key string
	// You'll have to add definitions here.
}

func (args GetArgs) String() string {
	return fmt.Sprintf("{%v, key=%v}", args.Identity, args.Key)
}

type GetReply struct {
	Err   Err
	Value string
}

func (reply GetReply) String() string {
	return fmt.Sprintf("{Err=%v, Value=%v}", reply.Err, raft.ToStringLimited(reply.Value, 10))
}

type GetStateArgs struct {
	Identity
	GetState
}

func (args GetStateArgs) String() string {
	return fmt.Sprintf("{%v, %v}", args.Identity, args.GetState)
}

type GetStateReply struct {
	State
	Err Err
}

func (reply GetStateReply) String() string {
	return fmt.Sprintf("{%v, Err=%v}", reply.State, reply.Err)
}

type ReConfiguringArgs struct {
	Identity
	shardctrler.Config
}

func (args ReConfiguringArgs) String() string {
	return fmt.Sprintf("{%v, config=%v}", args.Identity, args.Config)
}

type ReConfiguringReply struct {
	ConfigState
	Err Err
}

func (reply ReConfiguringReply) String() string {
	return fmt.Sprintf("{config_state=%v, Err=%v}", reply.ConfigState, reply.Err)
}

type ReConfiguredArgs struct {
	Identity
	State
}

func (args ReConfiguredArgs) String() string {
	return fmt.Sprintf("{%v, %v}", args.Identity, args.State)
}

type ReConfiguredReply struct {
	Err Err
}

func (reply ReConfiguredReply) String() string {
	return fmt.Sprintf("{Err=%v}", reply.Err)
}
