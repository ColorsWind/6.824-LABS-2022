package kvraft

import (
	"6.824/labrpc"
	"log"
	"os"
	"sync/atomic"
	"time"
)
import "crypto/rand"
import mathRand "math/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
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
	ck.lastKnownLeader = mathRand.Intn(len(ck.servers))
	ck.clientId = nrand()
	atomic.StoreInt64(&ck.commandId, 0)
	ck.logger = log.New(os.Stdout, "", log.Lshortfile|log.Lmicroseconds)
	//ck.logger.SetOutput(ioutil.Discard)
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := GetArgs{key, ck.clientId, atomic.AddInt64(&ck.commandId, 1)}
	for {
		reply := GetReply{}
		ck.logger.Printf("c -> %v: Call Get. args=%v.\n", ck.lastKnownLeader, args)
		ok := ck.servers[ck.lastKnownLeader].Call("KVServer.Get", &args, &reply)
		if ok {
			switch reply.Err {
			case OK:
				ck.logger.Printf("c -> %v: Successfully finished Get, args=%v, reply=%v.\n", ck.lastKnownLeader, args, reply)
				return reply.Value
			default:
				ck.logger.Printf("c -> %v: Call GET, args=%v, return error=%v.\n", ck.lastKnownLeader, args, reply.Err)
			}
		} else {
			ck.logger.Printf("c -> %v: Fail to call GET. args=%v.\n", ck.lastKnownLeader, args)
		}
		ck.lastKnownLeader = mathRand.Intn(len(ck.servers))
		time.Sleep(10 * time.Millisecond)
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{key, value, op, ck.clientId, atomic.AddInt64(&ck.commandId, 1)}
	for {
		reply := PutAppendReply{}
		ck.logger.Printf("c -> %v: Call PutAppend. args=%v.\n", ck.lastKnownLeader, args)
		ok := ck.servers[ck.lastKnownLeader].Call("KVServer.PutAppend", &args, &reply)
		if ok {
			switch reply.Err {
			case OK:
				ck.logger.Printf("c -> %v: Successfully finished PutAppend, args=%v, reply=%v.\n", ck.lastKnownLeader, args, reply)
				return
			default:
				ck.logger.Printf("c -> %v: Call PUT_APPEND, args=%v, return error=%v.\n", ck.lastKnownLeader, args, reply.Err)
			}
		} else {
			ck.logger.Printf("c -> %v: Fail to call PutAppend. args=%v.\n", ck.lastKnownLeader, args)
		}
		ck.lastKnownLeader = mathRand.Intn(len(ck.servers))
		time.Sleep(10 * time.Millisecond)
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
