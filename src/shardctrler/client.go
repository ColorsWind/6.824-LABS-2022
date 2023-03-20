package shardctrler

//
// Shardctrler clerk.
//

import (
	"6.824/labrpc"
	"log"
	"os"
	"sync/atomic"
)
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	lastKnownLeader int
	clientId        int64 // random id, should be unique globally
	commandId       int64 // for a client, monotonically increase from 0
	logger          *log.Logger
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.clientId = nrand()
	atomic.StoreInt64(&ck.commandId, 0)
	ck.logger = log.New(os.Stdout, "", log.Lshortfile|log.Lmicroseconds)
	return ck
}

func (ck *Clerk) Query(num int) Config {
	// Your code here.
	args := QueryArgs{ck.clientId, atomic.AddInt64(&ck.commandId, 1), num}
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", &args, &reply)
			if ok && reply.WrongLeader == false {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	// Your code here.
	args := JoinArgs{ck.clientId, atomic.AddInt64(&ck.commandId, 1), servers}
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", &args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	// Your code here.
	args := LeaveArgs{ck.clientId, atomic.AddInt64(&ck.commandId, 1), gids}
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", &args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	// Your code here.
	args := MoveArgs{ck.clientId, atomic.AddInt64(&ck.commandId, 1), shard, gid}
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", &args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
