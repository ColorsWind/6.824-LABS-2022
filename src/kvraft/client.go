package kvraft

import (
	"6.824/labrpc"
	"log"
	"time"
)
import "crypto/rand"
import math_rand "math/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
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
	args := GetArgs{key}
	for {
		reply := GetReply{}
		i := math_rand.Intn(len(ck.servers))
		log.Printf("c -> %v: Call Get. args=%v.\n", i, args)
		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
		if !ok {
			log.Printf("c -> %v: Fail to call GET.\n", i)
			continue
		}
		switch reply.Err {
		case OK:
			log.Printf("c -> %v: Successfully finished Get, reply=%v.\n", i, reply)
			return reply.Value
		case ErrNoKey:
			return ""
		default:
			log.Printf("c -> %v: Call GET, return error=%v.\n", i, reply.Err)
			time.Sleep(100 * time.Millisecond)
			continue
		}
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
	args := PutAppendArgs{key, value, op}
	for {
		reply := PutAppendReply{}
		i := math_rand.Intn(len(ck.servers))
		log.Printf("c -> %v: Call PutAppend. args=%v.\n", i, args)
		ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		if !ok {
			log.Printf("c -> %v: Fail to call PutAppend.\n", i)
			continue
		}
		if reply.Err == OK {
			log.Printf("c -> %v: Successfully finished PutAppend, reply=%v.\n", i, reply)
			return
		} else {
			log.Printf("c -> %v: Call PutAppend, return error=%v.\n", i, reply.Err)
			continue
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
