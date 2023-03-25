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
	ErrWrongGroup       = Err("ErrWrongGroup")
	ErrWrongLeader      = Err("ErrWrongLeader")
	ErrNotAvailableYet  = Err("ErrNotAvailableYet")
	ErrMatchConfiguring = Err("ErrMatchConfiguring")
	ErrMatchConfigured  = Err("ErrMatchConfigured")

	WaitComplete      = Err("WaitComplete")
	ErrOutdatedRPC    = Err("ErrOutdatedRPC")
	ErrGetStateRacing = Err("ErrGetStateRacing")
	ErrApplySnapshot  = Err("ErrApplySnapshot")
	ErrTimeout        = Err("ErrTimeout")
	ErrNotStarted     = Err("ErrNotStarted")
	ErrShutdown       = Err("ErrShutdown")
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
	ConfigNum             int
	KVMap                 map[string]string
	LastAppliedCommandMap map[int64]ExecutedOp
}

func (st State) String() string {
	return fmt.Sprintf("kvMap=%v, lastAppliedCommandMap=%v", st.KVMap, st.LastAppliedCommandMap)
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
	Shards []int
}

func (args GetStateArgs) String() string {
	return fmt.Sprintf("{%v, shards=%v}", args.Identity, args.Shards)
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
	Config shardctrler.Config
}

func (args ReConfiguringArgs) String() string {
	return fmt.Sprintf("{%v, config=%v}", args.Identity, args.Config)
}

type ReConfiguringReply struct {
	LastConfig shardctrler.Config
	Err        Err
}

func (reply ReConfiguringReply) String() string {
	return fmt.Sprintf("{config=%v, Err=%v}", reply.LastConfig, reply.Err)
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
