package shardkv

import (
	"6.824/labrpc"
	"6.824/shardctrler"
	"log"
	"os"
	"sync/atomic"
	"time"
)

// ConfigureClerk
// poll configuration periodically from shardctrler and send `re-configuring` RPC
// get state from other group and send `re-configured` RPC
type ConfigureClerk struct {
	mck      *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	clientId  int64 // random id, should be unique globally
	commandId int64 // for a client, monotonically increase from 0
	logger    *log.Logger
	kv        *ShardKV
	me        int
	gid       int
}

func MakeConfigureClerk(kv *ShardKV) *ConfigureClerk {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ck := new(ConfigureClerk)
	ck.kv = kv
	ck.mck = shardctrler.MakeClerk(kv.ctrlers)
	ck.make_end = kv.make_end
	ck.clientId = nrand()
	atomic.StoreInt64(&ck.commandId, 0)
	ck.logger = log.New(os.Stdout, "", log.Lshortfile|log.Lmicroseconds)
	ck.me = kv.me
	ck.gid = kv.gid
	return ck
}

func (ck *ConfigureClerk) onPollConfiguration() {
	config := ck.mck.Query(-1)
	lastConfig, err := ck.sendReConfiguring(config)
	if err != OK {
		if err != ErrOutdatedRPC {
			ck.logger.Printf("%v-%v: do Re-Configuring fail, Err=%v.\n", ck.gid, ck.me, err)
		}
		return
	}

	gid2shards := make(map[int][]int)
	for shard := range config.Shards {
		lastGid := lastConfig.Shards[shard]
		currGid := config.Shards[shard]
		if lastGid != 0 && lastGid != ck.me && currGid == ck.me {
			gid2shards[lastGid] = append(gid2shards[lastGid], shard)
			ck.logger.Printf("%v-%v: gain ownership of shard %v, gid=%v.\n", ck.gid, ck.me, shard, lastGid)
		} else if lastGid == ck.me && currGid != ck.me {
			ck.logger.Printf("%v-%v: lost ownership of shard %v, new_gid=%v.\n", ck.gid, ck.me, shard, currGid)
		}
	}

	getStateKVMap := make(map[string]string)
	getStateLastAppliedMap := make(map[int64]ExecutedOp)
	for gid, shards := range gid2shards {
		ck.logger.Printf("%v-%v: GetState. gid=%v, shards=%v.\n", ck.gid, ck.me, gid, shards)
		args := GetStateArgs{Identity{ck.clientId, atomic.AddInt64(&ck.commandId, 1)}, shards}
		for {
			//gid := kv.ctrlerConfig.Shards[shard]
			if servers, ok := lastConfig.Groups[gid]; ok {
				// try each server for the shard.
				for si := 0; si < len(servers); si++ {
					srv := ck.make_end(servers[si])
					var reply GetStateReply
					ok := srv.Call("ShardKV.GetState", &args, &reply)
					ck.logger.Printf("%v-%v call get state %v finish, reply=%v, ok=%v.\n", ck.gid, ck.me, servers[si], reply, ok)
					if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
						// successfully get state
						ck.logger.Printf("%v-%v: finish get state, kvMap=%v, lastAppliedCommandMap=%v.", ck.gid, ck.me, reply.KVMap, reply.LastAppliedCommandMap)

						for key, value := range reply.KVMap {
							getStateKVMap[key] = value
						}
						for key, value := range reply.LastAppliedCommandMap {
							getStateLastAppliedMap[key] = value
						}
						return
					}
					if ok && (reply.Err == ErrWrongGroup) {
						args.CommandId = atomic.AddInt64(&ck.commandId, 1)
						break
					}
					// ... not ok, or ErrWrongLeader
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

	err = ck.sendReConfigured(config, State{config.Num, getStateKVMap, getStateLastAppliedMap})
	if err != OK {
		ck.logger.Printf("%v-%v: do Re-Configured fail, Err=%v.\n", ck.gid, ck.me, err)
		return
	}
}

func (ck *ConfigureClerk) sendReConfiguring(config shardctrler.Config) (lastConfig shardctrler.Config, err Err) {
	args := ReConfiguringArgs{
		Identity: Identity{int64(ck.gid), int64(1 + config.Num*2)},
		Config:   config,
	}
	reply := ReConfiguringReply{}
	ck.kv.ReConfiguring(&args, &reply)
	return reply.LastConfig, reply.Err
}

func (ck *ConfigureClerk) sendReConfigured(config shardctrler.Config, state State) (err Err) {
	args := ReConfiguredArgs{
		Identity: Identity{int64(ck.gid), int64(1 + config.Num*2 + 1)},
		State:    state,
	}
	reply := ReConfiguredReply{}
	ck.kv.ReConfigured(&args, &reply)
	return reply.Err
}
