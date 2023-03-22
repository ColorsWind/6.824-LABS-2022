package kvraft

import (
	"6.824/raft"
	"fmt"
)

const (
	OK           = "OK"
	WaitComplete = "WaitComplete"
	//ErrNoKey       = "ErrNoKey"
	ErrWrongLeader   = "ErrWrongLeader"
	ErrOutdatedRPC   = "ErrOutdatedRPC"
	ErrApplySnapshot = "ErrApplySnapshot"
	ErrTimeout       = "ErrTimeout"
	ErrNotStarted    = "ErrNotStarted"
	ErrShutdown      = "ErrShutdown"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"

	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId  int64
	CommandId int64
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
	Key string

	// You'll have to add definitions here.
	ClientId  int64
	CommandId int64
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
