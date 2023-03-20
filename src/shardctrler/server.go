package shardctrler

import (
	"6.824/raft"
	"fmt"
	"log"
	"os"
	"sync/atomic"
	"time"
)
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	logger                *log.Logger
	configs               []Config // indexed by config num
	lastAppliedCommandMap map[int64]ExecutedOp
	lastAppliedIndex      int
	clients               map[int64]*ClientHandler
	dead                  int32
}

func (sc *ShardCtrler) String() string {
	if sc.mu.TryLock() {
		defer sc.mu.Unlock()
	}
	return fmt.Sprintf("{me=%v, rf=%v, configs=%v, lastAppliedCommandMap=%v, lastAppliedIndex=%v}", sc.me, sc.rf, sc.configs, sc.lastAppliedCommandMap, sc.lastAppliedIndex)
}

type ClientHandler struct {
	mu        sync.Mutex
	cond      *sync.Cond
	clientId  int64
	commandId int64
	finished  bool
	result    Config
	err       Err
}

type Op struct {
	// Your data here.
	ClientId  int64
	CommandId int64
	Type      OpType
	Servers   map[int][]string // join
	GIDs      []int            // leave
	Shard     int              // move
	GID       int              // move
	Num       int              // query
}

func (op Op) String() string {
	return fmt.Sprintf("{client_id=%v, cmd_id=%v, type=%v, servers=%v, gids=%v, shard=%v, gid=%v, num=%v}", op.ClientId, op.CommandId, op.Type, op.Servers, op.GIDs, op.Shard, op.GID, op.Num)
}

type ExecutedOp struct {
	Op
	Result Config
}

func (ec ExecutedOp) String() string {
	return fmt.Sprintf("op=%v, result=%v", ec.Op, raft.ToStringLimited(ec.Result, 100))
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.logger.Printf("%v: receive join request, args=%v.\n", sc.me, args)
	_, err := sc.handleRequest(args.ClientId, args.CommandId, OpType_JOIN, args.Servers, nil, -1, -1, -1)
	reply.Err = err
	reply.WrongLeader = err == ErrWrongLeader
	sc.logger.Printf("%v: receive join request, reply=%v.\n", sc.me, reply)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.logger.Printf("%v: receive leave request, args=%v.\n", sc.me, args)
	_, err := sc.handleRequest(args.ClientId, args.CommandId, OpType_JOIN, nil, args.GIDs, -1, -1, -1)
	reply.Err = err
	reply.WrongLeader = err == ErrWrongLeader
	sc.logger.Printf("%v: receive leave request, reply=%v.\n", sc.me, reply)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.logger.Printf("%v: receive move request, args=%v.\n", sc.me, args)
	_, err := sc.handleRequest(args.ClientId, args.CommandId, OpType_JOIN, nil, nil, args.Shard, args.GID, -1)
	reply.Err = err
	reply.WrongLeader = err == ErrWrongLeader
	sc.logger.Printf("%v: finish move request, reply=%v.\n", sc.me, reply)

}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.logger.Printf("%v: receive query request, args=%v.\n", sc.me, args)
	result, err := sc.handleRequest(args.ClientId, args.CommandId, OpType_JOIN, nil, nil, -1, -1, args.Num)
	reply.Err = err
	reply.WrongLeader = err == ErrWrongLeader
	reply.Config = result
	sc.logger.Printf("%v: finish query request, reply=%v.\n", sc.me, reply)
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&sc.dead, 1)
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.logger = log.New(os.Stdout, "", log.Lshortfile|log.Lmicroseconds)
	sc.clients = make(map[int64]*ClientHandler)
	sc.lastAppliedCommandMap = make(map[int64]ExecutedOp)
	sc.lastAppliedIndex = 0
	go func() {
		for !sc.killed() {
			msg := <-sc.applyCh
			sc.logger.Printf("%v: Receive msg: %v.\n", sc.me, msg)
			sc.handleReceiveMsg(msg)
		}
	}()
	return sc
}

func (sc *ShardCtrler) handleReceiveMsg(msg raft.ApplyMsg) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if msg.CommandValid {
		command := msg.Command.(Op)
		// update state machine
		var result Config
		lastAppliedCommand := sc.lastAppliedCommandMap[command.ClientId]
		if command.CommandId != lastAppliedCommand.CommandId && command.CommandId != lastAppliedCommand.CommandId+1 {
			sc.logger.Panicf("%v receive msg with invalid command id. msg=%v, lastAppliedMap=%v.", sc.me, msg, lastAppliedCommand.CommandId)
		}
		if command.CommandId == lastAppliedCommand.CommandId+1 {
			cf := sc.configs[len(sc.configs)-1]
			switch command.Type {
			case OpType_JOIN:
				// The Join RPC is used by an administrator to add new replica groups. Its argument is a set of mappings
				// from unique, non-zero replica group identifiers (GIDs) to lists of server names.
				// The shardctrler should react by creating a new configuration that includes the new replica groups.
				// The new configuration should divide the shards as evenly as possible among the full set of groups,
				// and should move as few shards as possible to achieve that goal.
				// The shardctrler should allow re-use of a GID if it's not part of the current configuration
				// (i.e. a GID should be allowed to Join, then Leave, then Join again).

				if cf.Num == 0 {
					var gids []int
					for gid, _ := range command.Servers {
						gids = append(gids, gid)
					}
					cf = Config{
						Num:    1,
						Shards: initBalance(gids),
						Groups: command.Servers,
					}
				} else {
					servers := make(map[int][]string)
					for gid, group := range cf.Groups {
						servers[gid] = group
					}
					for gid, group := range command.Servers {
						element, present := servers[gid]
						if present {
							sc.logger.Panicf("%v: duplicate join, server already exist: servers[%v]=%v.", sc.me, gid, element)
						}
						servers[gid] = group
					}
					gil := shardToGroupItemList(cf.Shards, len(cf.Groups))
					gil = reBalance(gil)
					shardsBalanced, _ := groupItemListToShard(gil)
					cf = Config{
						Num:    cf.Num + 1,
						Shards: shardsBalanced,
						Groups: servers,
					}
				}
				sc.configs = append(sc.configs, cf)
			case OpType_LEAVE:
				// The Leave RPC's argument is a list of GIDs of previously joined groups.
				// The shardctrler should create a new configuration that does not include those groups, and that
				// assigns those groups' shards to the remaining groups.
				// The new configuration should divide the shards as evenly as possible among the groups,
				// and should move as few shards as possible to achieve that goal.
				servers := make(map[int][]string)
				for gid, group := range cf.Groups {
					servers[gid] = group
				}
				for gid := range command.GIDs {
					if _, present := servers[gid]; !present {
						sc.logger.Panicf("%v: leave non-exist server %v: servers=%v.", sc.me, gid, servers)
					}
					delete(servers, gid)
				}
				gil := shardToGroupItemList(cf.Shards, len(cf.Groups))
				gil = reBalance(gil)
				shardsBalanced, _ := groupItemListToShard(gil)
				cf = Config{
					Num:    cf.Num + 1,
					Shards: shardsBalanced,
					Groups: servers,
				}
				sc.configs = append(sc.configs, cf)
			case OpType_MOVE:
				// The Move RPC's arguments are a shard number and a GID.
				// The shardctrler should create a new configuration in which the shard is assigned to the group.
				// The purpose of Move is to allow us to test your software.
				// A Join or Leave following a Move will likely un-do the Move, since Join and Leave re-balance.
				servers := make(map[int][]string)
				for gid, group := range cf.Groups {
					servers[gid] = group
				}
				shards := cf.Shards
				shards[command.Shard] = command.GID
				cf = Config{
					Num:    cf.Num + 1,
					Shards: shards,
					Groups: servers,
				}
				sc.configs = append(sc.configs, cf)
			case OpType_QUERY:
				// The Query RPC's argument is a configuration number.
				// The shardctrler replies with the configuration that has that number.
				// If the number is -1 or bigger than the biggest known configuration number,
				// the shardctrler should reply with the latest configuration.
				// The result of Query(-1) should reflect every Join, Leave,
				// or Move RPC that the shardctrler finished handling before it received the Query(-1) RPC.
				if command.Num == -1 || command.Num >= len(sc.configs) {
					result = cf
				} else {
					result = sc.configs[command.Num]
				}
			}
		} else {
			result = lastAppliedCommand.Result
		}

		handler, present := sc.clients[command.ClientId]
		if present && handler.commandId == command.CommandId {
			handler.mu.Lock()
			handler.result = result
			handler.err = OK
			handler.finished = true
			handler.cond.Broadcast()
			handler.mu.Unlock()
		}
		sc.lastAppliedIndex = msg.CommandIndex
	} else {
		sc.logger.Printf("%v: detect applyCh close, return, msg=%v.\n", sc, msg)
	}
}

func (sc *ShardCtrler) handleRequest(clientId int64, commandId int64, opType OpType, servers map[int][]string, gids []int, shard int, gid int, num int) (result Config, err Err) {
	sc.mu.Lock()
	handler, present := sc.clients[clientId]
	if !present || handler.commandId < commandId {
		handler = &ClientHandler{
			clientId:  clientId,
			commandId: commandId,
			err:       ErrNotStarted,
			finished:  true,
		}
		handler.cond = sync.NewCond(&handler.mu)
		sc.clients[clientId] = handler
	}

	// if already finish
	handler.mu.Lock()
	defer handler.mu.Unlock()
	lastAppliedCommand, present := sc.lastAppliedCommandMap[clientId]
	if present && lastAppliedCommand.CommandId == commandId {
		handler.result = lastAppliedCommand.Result
		handler.err = OK
		handler.finished = true
		handler.cond.Broadcast()
		sc.mu.Unlock()
		return handler.result, handler.err
	} else {
		sc.mu.Unlock()
	}
	op := Op{clientId, commandId, opType, servers, gids, shard, gid, num}
	// if invalid command id
	if handler.commandId > commandId {
		sc.logger.Panicf("shardctrler %v receive invalid id (handler.id=%v). op=%v, handler=%v.", sc.me, handler.commandId, op, handler)
	}

	// if not started
	if handler.finished && handler.err != OK {
		// should retry
		_, currentTerm, isLeader := sc.rf.Start(op)
		if !isLeader {
			return Config{}, ErrWrongLeader
		}
		handler.finished = false
		handler.err = WaitComplete

		go func() {
			for i := 0; true; i++ {
				time.Sleep(250 * time.Millisecond)
				if sc.killed() {
					handler.mu.Lock()
					if handler.finished == false {
						sc.logger.Printf("%v: client handler killed, op=%v, handler=%v.\n",
							sc.me, op, handler)
						handler.err = ErrShutdown
						handler.finished = true
						handler.cond.Broadcast()
					}
					handler.mu.Unlock()
					return
				}
				currentTerm1, _ := sc.rf.GetState()
				if currentTerm == currentTerm1 {
					sc.mu.Lock()
					sc.logger.Printf("%v: client handler continue to wait, currentTerm=%v, i=%v, lastAppliedCmd_id=%v, op=%v, handler=%v.\n",
						sc.me, currentTerm1, i, sc.lastAppliedCommandMap[clientId].CommandId, op, handler)
					sc.mu.Unlock()
				} else {
					sc.mu.Lock()
					sc.logger.Printf("%v: client handler timeout, currentTerm=%v, i=%v, lastAppliedCmd_id=%v, op=%v, handler=%v.\n",
						sc.me, currentTerm1, i, sc.lastAppliedCommandMap[clientId].CommandId, op, handler)
					sc.mu.Unlock()
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
