package shardkv

import (
	"6.824/raft"
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
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"

	WaitComplete     = "WaitComplete"
	ErrOutdatedRPC   = "ErrOutdatedRPC"
	ErrApplySnapshot = "ErrApplySnapshot"
	ErrTimeout       = "ErrTimeout"
	ErrNotStarted    = "ErrNotStarted"
	ErrShutdown      = "ErrShutdown"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	ClientId  int64
	CommandId int64

	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

func (args PutAppendArgs) String() string {
	return fmt.Sprintf("{Key=%v, Value=%v, op=%v, client_id=%v, cmd_id=%v}", args.Key, raft.ToStringLimited(args.Value, 10), args.Op, args.ClientId, args.CommandId)
}

type PutAppendReply struct {
	Err Err
}

func (reply PutAppendReply) String() string {
	return fmt.Sprintf("{Err=%v}", reply.Err)
}

type GetArgs struct {
	ClientId  int64
	CommandId int64

	Key string
	// You'll have to add definitions here.
}

func (args GetArgs) String() string {
	return fmt.Sprintf("{Key=%v, client_id=%v, cmd_id=%v}", args.Key, args.ClientId, args.CommandId)
}

type GetReply struct {
	Err   Err
	Value string
}

func (reply GetReply) String() string {
	return fmt.Sprintf("{Err=%v, Value=%v}", reply.Err, raft.ToStringLimited(reply.Value, 10))
}

type GetStateArgs struct {
	ClientId  int64
	CommandId int64
	Shards    []int
}

func (args GetStateArgs) String() string {
	return fmt.Sprintf("{shards=%v, client_id=%v, cmd_id=%v}", args.Shards, args.ClientId, args.CommandId)
}

type GetStateReply struct {
	KVMap                 map[string]string
	LastAppliedCommandMap map[int64]ExecutedOp
	Err                   Err
}

func (reply GetStateReply) String() string {
	return fmt.Sprintf("{kvMap=%v, lastAppliedCommandMap=%v, err=%v}", reply.KVMap, reply.LastAppliedCommandMap, reply.Err)
}
