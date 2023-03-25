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
	config := ck.mck.Query(ck.configuringNum + 1)
	//ck.logger.Printf("%v-%v: queried config: %v.\n", ck.gid, ck.me, config)
	if config.Num < ck.configuringNum {
		ck.logger.Panicf("%v-%v: config.Num < ck.conf.LastConfig.Num, something went wrong, query_config=%v, conf=%v\n", ck.gid, ck.me, config, ck.configuringNum)
	}
	if config.Num == ck.configuringNum && config.Num == ck.configuredNum {
		// if config.Num == ck.configuringNum, maybe still in re-configuring status (re-configured fail)
		return
	}

	lastConfig, err := ck.sendReConfiguring(config)
	if err != OK {
		if err != ErrOutdatedRPC {
			ck.logger.Printf("%v-%v: do Re-Configuring fail, Err=%v.\n", ck.gid, ck.me, err)
		}
		return
	}
	if lastConfig.Num > config.Num {
		ck.logger.Printf("%v-%v: do Re-Configuring done nothing, shardkv is newer, lastConfig=%v, queryConfig=%v.\n", ck.gid, ck.me, lastConfig, config)
		ck.configuringNum = lastConfig.Num
		return
	} else if lastConfig.Num == config.Num {
		ck.logger.Printf("%v-%v: do Re-Configuring done nothing, shardkv is up-to-date, try Re-Configured, lastConfig=%v, queryConfig=%v.\n", ck.gid, ck.me, lastConfig, config)
	} else if lastConfig.Num+1 == config.Num {
		ck.logger.Printf("%v-%v: update last known configuring config num to %v.\n", ck.gid, ck.me, ck.configuringNum)
		ck.configuringNum = config.Num
	} else {
		// lastConfig.Num < config.Num
		ck.configuringNum = lastConfig.Num
		ck.logger.Panicf("%v-%v: do Re-Configuring done nothing, shardkv is too old, maybe there is a bug. lastConfig=%v, queryConfig=%v.\n", ck.gid, ck.me, lastConfig, config)
		return
	}

	gid2shards := make(map[int][]int)
	for shard := range lastConfig.Shards {
		lastGid := lastConfig.Shards[shard]
		currGid := config.Shards[shard]
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
		func() {
			ck.logger.Printf("%v-%v: GetState. gid=%v, shards=%v.\n", ck.gid, ck.me, gid, shards)
			args := GetStateArgs{Identity{ck.getStateClientId, atomic.AddInt64(&ck.commandId, 1)}, shards}
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
						if ok && reply.Err.isDeterministic() {
							//args.CommandId = atomic.AddInt64(&ck.commandId, 1)
							break
						}
						// ... not ok, or ErrWrongLeader
					}
				}
				time.Sleep(100 * time.Millisecond)
			}
		}()
	}

	err = ck.sendReConfigured(config, State{config.Num, getStateKVMap, getStateLastAppliedMap})
	if err != OK {
		if err != ErrOutdatedRPC {
			ck.logger.Printf("%v-%v: do Re-Configured fail, Err=%v.\n", ck.gid, ck.me, err)
		}

		return
	}
	ck.configuredNum = config.Num
	ck.logger.Printf("%v-%v: update last known configured config num to %v.\n", ck.gid, ck.me, ck.configuredNum)
}

func (ck *ConfigureClerk) sendReConfiguring(config shardctrler.Config) (lastConfig shardctrler.Config, err Err) {
	args := ReConfiguringArgs{
		Identity: Identity{ck.configureClientId, int64(1 + config.Num*2)},
		Config:   config,
	}
	reply := ReConfiguringReply{}
	ck.kv.ReConfiguring(&args, &reply)
	return reply.LastConfig, reply.Err
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
