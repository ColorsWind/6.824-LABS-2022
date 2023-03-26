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
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	getStateClientId  int64 // random id, should be unique globally
	configureClientId int64
	commandId         int64 // for a client, monotonically increase from 0
	logger            *log.Logger
	kv                *ShardKV
	me                int
	gid               int
	configuringNum    int
	configuredNum     int
}

func MakeConfigureClerk(kv *ShardKV) *ConfigureClerk {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ck := new(ConfigureClerk)
	ck.kv = kv
	ck.mck = shardctrler.MakeClerk(kv.ctrlers)
	ck.make_end = kv.make_end
	ck.getStateClientId = nrand()
	ck.configureClientId = nrand()
	atomic.StoreInt64(&ck.commandId, 0)
	ck.logger = log.New(os.Stdout, "", log.Lshortfile|log.Lmicroseconds)
	ck.me = kv.me
	ck.gid = kv.gid
	ck.configuringNum = 0
	ck.configuredNum = 0
	return ck
}

func (ck *ConfigureClerk) onPollConfiguration() {
	queryConfig := ck.mck.Query(ck.configuringNum + 1)
	//ck.logger.Printf("%v-%v: queried queryConfig: %v.\n", ck.gid, ck.me, queryConfig)
	if queryConfig.Num < ck.configuringNum {
		ck.logger.Panicf("%v-%v: queryConfig.Num < ck.conf.LastConfig.Num, something went wrong, query_config=%v, conf=%v\n", ck.gid, ck.me, queryConfig, ck.configuringNum)
	}
	if queryConfig.Num == ck.configuringNum && queryConfig.Num == ck.configuredNum {
		// if queryConfig.Num == ck.configuringNum, maybe still in re-configuring status (re-configured fail)
		return
	}

	configState, err := ck.sendReConfiguring(queryConfig)
	if err != OK {
		if err != ErrOutdatedRPC {
			ck.logger.Printf("%v-%v: do Re-Configuring fail, Err=%v.\n", ck.gid, ck.me, err)
		}
		return
	}

	if configState.Accept {
		ck.configuringNum = queryConfig.Num
		ck.logger.Printf("%v-%v: accpet re-configuring, configNum=%v, configState=%v, queryConfig=%v.\n", ck.gid, ck.me, ck.configuringNum, configState, queryConfig)

	} else {
		ck.logger.Printf("%v-%v: deny re-configuring, configNum=%v, configState=%v, queryConfig=%v.\n", ck.gid, ck.me, ck.configuringNum, configState, queryConfig)
		ck.configuringNum = configState.LastConfig.Num
	}
	lastConfig := configState.LastConfig

	gid2shards := make(map[int][]int)
	for shard := range lastConfig.Shards {
		lastGid := lastConfig.Shards[shard]
		currGid := queryConfig.Shards[shard]
		if lastGid != 0 && lastGid != ck.gid && currGid == ck.gid {
			gid2shards[lastGid] = append(gid2shards[lastGid], shard)
			ck.logger.Printf("%v-%v: gain ownership of shard %v, gid=%v.\n", ck.gid, ck.me, shard, lastGid)
		} else if lastGid == ck.gid && currGid != ck.gid {
			ck.logger.Printf("%v-%v: lost ownership of shard %v, new_gid=%v.\n", ck.gid, ck.me, shard, currGid)
		}
	}

	getStateKVMap := make(map[string]string)
	getStateLastAppliedMap := make(map[int64]ExecutedOp)
	for gid, shards := range gid2shards {
		state, err := ck.sendGetState(lastConfig, gid, shards, false)
		if err == ErrShardDelete {
			ck.logger.Printf("%v-%v: send get state, but get err=%v, state=%v, maybe other shardkv already updated.\n", ck.gid, ck.me, err, state)
			return
		} else if err != OK {
			ck.logger.Panicf("%v-%v: send get state, but get err=%v, state=%v, maybe is a bug.\n", ck.gid, ck.me, err, state)
		} else {
			for key, value := range state.KVMap {
				getStateKVMap[key] = value
			}
			for key, value := range state.LastAppliedCommandMap {
				getStateLastAppliedMap[key] = value
			}
		}
	}
	err = ck.sendReConfigured(queryConfig, State{queryConfig.Num, getStateKVMap, getStateLastAppliedMap})
	if err != OK {
		if err != ErrOutdatedRPC {
			ck.logger.Printf("%v-%v: do Re-Configured fail, Err=%v.\n", ck.gid, ck.me, err)
		}
		return
	}
	ck.configuredNum = queryConfig.Num
	ck.logger.Printf("%v-%v: update last known configured queryConfig num to %v.\n", ck.gid, ck.me, ck.configuredNum)

	for gid, shards := range gid2shards {
		state, err := ck.sendGetState(lastConfig, gid, shards, true)
		ck.logger.Printf("%v-%v: send state confirm, get err=%v, state=%v.\n", ck.gid, ck.me, err, state)
	}
}

func (ck *ConfigureClerk) sendGetState(lastConfig shardctrler.Config, gid int, shards []int, confirm bool) (state State, err Err) {
	ck.logger.Printf("%v-%v: GetStateOK. gid=%v, shards=%v.\n", ck.gid, ck.me, gid, shards)
	args := GetStateArgs{Identity{ck.getStateClientId, atomic.AddInt64(&ck.commandId, 1)}, GetState{lastConfig.Num, shards, confirm}}
	for {
		//gid := kv.ctrlerConfig.Shards[shard]
		if servers, ok := lastConfig.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply GetStateReply
				ok := srv.Call("ShardKV.GetState", &args, &reply)
				ck.logger.Printf("%v-%v call get state ok %v finish, reply=%v, ok=%v.\n", ck.gid, ck.me, servers[si], reply, ok)
				if ok && (reply.Err == OK || reply.Err == ErrShardDelete) {
					// successfully get state
					ck.logger.Printf("%v-%v: finish get state ok, kvMap=%v, lastAppliedCommandMap=%v.", ck.gid, ck.me, reply.KVMap, reply.LastAppliedCommandMap)
					return reply.State, reply.Err
				}
				if ok && reply.Err == ErrWrongGroup {
					//args.CommandId = atomic.AddInt64(&ck.commandId, 1)
					ck.logger.Panicf("%v-%v: send get state receive %v, args=%v, reply=%v.\n", ck.gid, ck.me, err, args, reply)
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *ConfigureClerk) sendReConfiguring(config shardctrler.Config) (configState ConfigState, err Err) {
	args := ReConfiguringArgs{
		Identity: Identity{ck.configureClientId, int64(1 + config.Num*2)},
		Config:   config,
	}
	reply := ReConfiguringReply{}
	ck.kv.ReConfiguring(&args, &reply)
	return reply.ConfigState, reply.Err
}

func (ck *ConfigureClerk) sendReConfigured(config shardctrler.Config, state State) (err Err) {
	args := ReConfiguredArgs{
		Identity: Identity{ck.configureClientId, int64(1 + config.Num*2 + 1)},
		State:    state,
	}
	reply := ReConfiguredReply{}
	ck.kv.ReConfigured(&args, &reply)
	return reply.Err
}
