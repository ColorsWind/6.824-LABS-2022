package shardkv

import (
	"6.824/raft"
	"6.824/shardctrler"
	"fmt"
	"log"
	"os"
	"sync"
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
	ErrShardCreate = Err("ErrShardCreate")
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

type KVState struct {
	ConfiguredNum         int
	KVMap                 map[string]string
	LastAppliedCommandMap map[int64]ExecutedOp
}

func (st KVState) String() string {
	return fmt.Sprintf("num=%v, kvMap=%v, lastAppliedCommandMap=%v", st.ConfiguredNum, st.KVMap, st.LastAppliedCommandMap)
}

type PartialConfiguration struct {
	Shards []int
	KVState
}

func (pc PartialConfiguration) String() string {
	return fmt.Sprintf("shards=%v, %v", pc.Shards, pc.KVState)
}

type GetState struct {
	ConfigNum int
	Shards    []int
	Confirm   bool
}

func (gs GetState) String() string {
	return fmt.Sprintf("num=%v, shards=%v, confirm=%v", gs.ConfigNum, gs.Shards, gs.Confirm)
}

type ConfigState struct {
	Update           bool
	Completed        bool
	ConfiguredConfig shardctrler.Config
}

func (cs ConfigState) String() string {
	return fmt.Sprintf("update=%v, configuring=%v, configured_config=%v", cs.Update, cs.Completed, cs.ConfiguredConfig)
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
	KVState
	Err Err
}

func (reply GetStateReply) String() string {
	return fmt.Sprintf("{%v, Err=%v}", reply.KVState, reply.Err)
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
	PartialConfiguration
}

func (args ReConfiguredArgs) String() string {
	return fmt.Sprintf("{%v, %v}", args.Identity, args.KVState)
}

type ReConfiguredReply struct {
	MissingShards []int
	Err           Err
}

func (reply ReConfiguredReply) String() string {
	return fmt.Sprintf("{MissingShards=%v, Err=%v}", reply.MissingShards, reply.Err)
}

type GlobalLogOutput struct {
	mu sync.Mutex
	fi *os.File
}

var globalLog = GlobalLogOutput{}

func getLogOutput() *os.File {
	if output := os.Getenv("shardkv_output"); output != "" {
		globalLog.mu.Lock()
		defer globalLog.mu.Unlock()
		if globalLog.fi == nil {
			fi, err := os.OpenFile(output, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
			if err != nil {
				log.Panicf("Error open log file: %v.", output)
			}
			globalLog.fi = fi
		}
		return globalLog.fi
	} else {
		return os.Stdout
	}
}
