package kvraft

import (
	"6.824/labrpc"
	"log"
	"time"
)
import "crypto/rand"
import mathRand "math/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastKnownLeader int
	clientId        int64 // should be unique globally
	commandId       int64 // for a client, monotonically increase from 0
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
	ck.commandId = 0
	log.SetFlags(log.Lshortfile | log.Lmicroseconds)
	//log.SetOutput(ioutil.Discard)
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
	ck.commandId += 1
	args := GetArgs{key, ck.clientId, ck.commandId}
	for {
		reply := GetReply{}
		log.Printf("c -> %v: Call Get. args=%v.\n", ck.lastKnownLeader, args)
		ok := ck.servers[ck.lastKnownLeader].Call("KVServer.Get", &args, &reply)
		if ok {
			switch reply.Err {
			case OK:
				log.Printf("c -> %v: Successfully finished Get, args=%v, reply=%v.\n", ck.lastKnownLeader, args, reply)
				return reply.Value
			default:
				log.Printf("c -> %v: Call GET, return error=%v.\n", ck.lastKnownLeader, reply.Err)
			}
		} else {
			log.Printf("c -> %v: Fail to call GET. args=%v.\n", ck.lastKnownLeader, args)
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
	ck.commandId += 1
	args := PutAppendArgs{key, value, op, ck.clientId, ck.commandId}
	for {
		reply := PutAppendReply{}
		log.Printf("c -> %v: Call PutAppend. args=%v.\n", ck.lastKnownLeader, args)
		ok := ck.servers[ck.lastKnownLeader].Call("KVServer.PutAppend", &args, &reply)
		if ok {
			switch reply.Err {
			case OK:
				log.Printf("c -> %v: Successfully finished PutAppend, args=%v, reply=%v.\n", ck.lastKnownLeader, args, reply)
				return
			default:
				log.Printf("c -> %v: Call PUT_APPEND, return error=%v.\n", ck.lastKnownLeader, reply.Err)
			}
		} else {
			log.Printf("c -> %v: Fail to call PutAppend. args=%v.\n", ck.lastKnownLeader, args)
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
