package shardkv

import (
	"6.824/labrpc"
	"6.824/shardctrler"
	"log"
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
	mainClientId     int64 // random id, should be unique globally
	mainCommandId    int64 // for a client, monotonically increase from 0
	mutipleClientId  [shardctrler.NShards]int64
	mutipleCommandId [shardctrler.NShards]int64
	logger           *log.Logger
	kv               *ShardKV
	me               int
	gid              int
	preConfigNum     int
	configuredNum    int
}

func MakeConfigureClerk(kv *ShardKV) *ConfigureClerk {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ck := new(ConfigureClerk)
	ck.kv = kv
	ck.mck = shardctrler.MakeClerk(kv.ctrlers)
	ck.make_end = kv.make_end
	ck.mainClientId = nrand()
	for shard := range ck.mutipleClientId {
		ck.mutipleClientId[shard] = nrand()
	}

	ck.logger = kv.logger
	ck.me = kv.me
	ck.gid = kv.gid
	ck.preConfigNum = 0
	ck.configuredNum = 0
	return ck
}

func (ck *ConfigureClerk) onPollConfiguration() {
	// !!Important: carefully check assumption, queryConfig2 may change quickly (concurrent re-configure)
	// !!Important: carefully reuse clientId, as we assume one time, one RPC per client

	queryConfig := ck.mck.Query(-1)
	//ck.logger.Printf("%v-%v: query config = %v.\n", ck.gid, ck.me, queryConfig)
	if queryConfig.Num < ck.preConfigNum {
		ck.logger.Panicf("%v-%v: queryConfig.Num < ck.conf.ConfiguredConfig.Num, something went wrong, queryConfig=%v, conf=%v\n", ck.gid, ck.me, queryConfig, ck.preConfigNum)
	}

	if queryConfig.Num == ck.preConfigNum && queryConfig.Num == ck.configuredNum {
		return
	}

	configState, err := ck.sendReConfiguring(ck.mainClientId, atomic.AddInt64(&ck.mainCommandId, 1), queryConfig)
	configuredConfig := configState.ConfiguredConfig
	if err != OK {
		ck.logger.Printf("%v-%v: do Re-Completed fail, Err=%v.\n", ck.gid, ck.me, err)
		//if err == ErrWrongLeader {
		//	time.Sleep(100 * time.Millisecond)
		//}
		return
	}

	// guess shardkv preConfig
	var alreadyCompleted bool
	var preConfig shardctrler.Config
	for i := 0; true; i++ {

		if configState.Update {
			ck.logger.Printf("%v-%v: update re-configuring, configNum=%v, configState=%v, queryConfig=%v.\n", ck.gid, ck.me, ck.preConfigNum, configState, queryConfig)
			preConfig = queryConfig
			alreadyCompleted = false
			break
		} else if !configState.Completed {
			preConfig = ck.mck.Query(configuredConfig.Num + 1)
			alreadyCompleted = false
			break
		} else {
			// not update, completed, should configure a new one
			if configuredConfig.Num == queryConfig.Num-1 {
				// queryConfig - 1 is completed, but update to queryConfig fail ???
				ck.logger.Panicf("%v-%v: during re-configuring, unknown situation #1, i=%v, queryConfig=%v, configState=%v, configuredConfig=%v, alreadyCompleted=%v.\n", ck.gid, ck.me, i, queryConfig, configState, configuredConfig, alreadyCompleted)
			} else if configuredConfig.Num < queryConfig.Num-1 {
				queryConfig = ck.mck.Query(configuredConfig.Num + 1)
				configState, err = ck.sendReConfiguring(ck.mainClientId, atomic.AddInt64(&ck.mainCommandId, 1), queryConfig)
				configuredConfig = configState.ConfiguredConfig
				ck.logger.Printf("%v-%v: query config is too new to update, retry with old one, i=%v, queryConfig=%v, configState=%v, configuredConfig=%v, alreadyCompleted=%v.\n", ck.gid, ck.me, i, queryConfig, configState, configuredConfig, alreadyCompleted)
				continue
			} else if configuredConfig.Num == queryConfig.Num {
				// queryConfig is completed, other client has already done our job
				alreadyCompleted = true
				preConfig = queryConfig
				ck.logger.Printf("%v-%v: already submit and complete, i=%v, queryConfig=%v, configState=%v, configuredConfig=%v, alreadyCompleted=%v.\n", ck.gid, ck.me, i, queryConfig, configState, configuredConfig, alreadyCompleted)
				break
			} else if configuredConfig.Num > queryConfig.Num {
				queryConfig = ck.mck.Query(-1)
				configState, err = ck.sendReConfiguring(ck.mainClientId, atomic.AddInt64(&ck.mainCommandId, 1), queryConfig)
				configuredConfig = configState.ConfiguredConfig
				ck.logger.Printf("%v-%v: concurrent configuration change, i=%v, queryConfig=%v, configState=%v, configuredConfig=%v, alreadyCompleted=%v.\n", ck.gid, ck.me, i, queryConfig, configState, configuredConfig, alreadyCompleted)
				continue
			} else {
				ck.logger.Panicf("%v-%v: during re-configuring, unknown situation #2, i=%v, queryConfig=%v, configState=%v, configuredConfig=%v, alreadyCompleted=%v.\n", ck.gid, ck.me, i, queryConfig, configState, configuredConfig, alreadyCompleted)
			}
		}
	}

	if preConfig.Num > ck.preConfigNum {
		ck.preConfigNum = preConfig.Num
		ck.logger.Printf("%v-%v: update last known preConfig.Num to %v.\n", ck.gid, ck.me, ck.preConfigNum)

	}

	if !alreadyCompleted {

		if configuredConfig.Num+1 != preConfig.Num {
			ck.logger.Panicf("%v-%v: during re-configured, something went wrong, preConfig=%v, configuredConfig=%v, queryConfig=%v, configState=%v.\n", ck.gid, ck.me, preConfig, configuredConfig, queryConfig, configState)
		}

		gid2shards := make(map[int][]int)
		for shard := range preConfig.Shards {
			configuredGid := configuredConfig.Shards[shard]
			preGid := preConfig.Shards[shard]
			if configuredGid != 0 && configuredGid != ck.gid && preGid == ck.gid {
				gid2shards[configuredGid] = append(gid2shards[configuredGid], shard)
				ck.logger.Printf("%v-%v: gain ownership of shard %v, configuredConfig=%v, gid=%v.\n", ck.gid, ck.me, shard, configuredConfig, configuredGid)
			} else if configuredGid == ck.gid && preGid != ck.gid {
				ck.logger.Printf("%v-%v: lost ownership of shard %v, configuredConfig=%v, new_gid=%v.\n", ck.gid, ck.me, configuredConfig, shard, preGid)
			}
		}
		if len(gid2shards) > 0 {
			ch := make(chan int)
			for gid, shards := range gid2shards {
				gid := gid
				shards := shards
				anyShard := shards[0]
				clientId := ck.mutipleClientId[anyShard]
				commandId := &ck.mutipleCommandId[anyShard]
				go func() {
					for {
						state, err := ck.sendGetState(clientId, atomic.AddInt64(commandId, 1), configuredConfig, gid, shards, false)
						switch err {
						case OK:
							ck.logger.Printf("%v-%v: get state success, gid=%v, shards=%v.\n", ck.gid, ck.me, gid, shards)
							missing, err := ck.sendReConfigured(clientId, atomic.AddInt64(commandId, 1), PartialConfiguration{shards, KVState{preConfig.Num, state.KVMap, state.LastAppliedCommandMap}})
							ck.logger.Printf("%v-%v: re-configured, shards=%v, missing=%v, err=%v.\n", ck.gid, ck.me, shards, missing, err)
							if err == OK {
								ch <- len(missing)
								_, err = ck.sendGetState(clientId, atomic.AddInt64(commandId, 1), configuredConfig, gid, shards, true)
								ck.logger.Printf("%v-%v: get state confirm, err=%v.\n", ck.gid, ck.me, err)
							} else {
								ch <- -1
							}
							return
						case ErrShardCreate:
							ck.logger.Printf("%v-%v: get state fail, gid=%v, shards=%v, but get err=%v, state=%v, shardkv's config is fall behind, retry after 100ms.\n", ck.gid, ck.me, gid, shards, err, state)
							time.Sleep(100 * time.Millisecond)
							continue
						case ErrShardDelete:
							ck.logger.Printf("%v-%v: get state fail, gid=%v, shards=%v, but get err=%v, state=%v, maybe other shardkv already updated.\n", ck.gid, ck.me, gid, shards, err, state)
							ch <- -2
							return
						default:
							ck.logger.Panicf("%v-%v: get state fail, gid=%v, shards=%v, but get err=%v, state=%v, maybe other shardkv already updated.\n", ck.gid, ck.me, gid, shards, err, state)
						}
					}
				}()

			}

			for range gid2shards {
				missing := <-ch
				if missing == 0 {
					ck.configuredNum = preConfig.Num
					ck.logger.Printf("%v-%v: missing is empty, update last known configured Num to %v.\n", ck.gid, ck.me, ck.configuredNum)
				}
			}
		} else {
			missing, err := ck.sendReConfigured(ck.mainClientId, atomic.AddInt64(&ck.mainCommandId, 1), PartialConfiguration{nil, KVState{preConfig.Num, nil, nil}})
			if err != OK {
				ck.logger.Printf("%v-%v: expect re-configured success, but err=%v.\n", ck.gid, ck.me, err)
				return
			}
			if len(missing) > 0 {
				ck.logger.Panicf("%v-%v: expect missing is empty, but got: %v.\n", ck.gid, ck.me, missing)
			}
			ck.configuredNum = preConfig.Num
			ck.logger.Printf("%v-%v: group not affected, just update last known configured Num to %v.\n", ck.gid, ck.me, ck.configuredNum)
		}
	} else {
		ck.configuredNum = configuredConfig.Num
		ck.logger.Printf("%v-%v: no need to send re-configured, update last known configured Num to %v.\n", ck.gid, ck.me, ck.configuredNum)
	}

}

func (ck *ConfigureClerk) sendGetState(clientId int64, commandId int64, lastConfig shardctrler.Config, gid int, shards []int, confirm bool) (state KVState, err Err) {
	ck.logger.Printf("%v-%v: GetState. gid=%v, shards=%v.\n", ck.gid, ck.me, gid, shards)
	args := GetStateArgs{Identity{clientId, commandId}, GetState{lastConfig.Num, shards, confirm}}
	for {
		//gid := kv.ctrlerConfig.Shards[shard]
		if servers, ok := lastConfig.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply GetStateReply
				ok := srv.Call("ShardKV.GetState", &args, &reply)
				ck.logger.Printf("%v-%v: call get finish, server='%v', args=%v, reply=%v, ok=%v.\n", ck.gid, ck.me, servers[si], args, reply, ok)
				if ok && (reply.Err == OK || reply.Err == ErrShardDelete || reply.Err == ErrShardCreate) {
					// successfully get state
					ck.logger.Printf("%v-%v: finish get state, args=%v, reply=%v.", ck.gid, ck.me, args, reply)
					return reply.KVState, reply.Err
				}
				if ok && reply.Err == ErrWrongGroup {
					//args.CommandId = atomic.AddInt64(&ck.mutipleCommandId, 1)
					ck.logger.Panicf("%v-%v: send get state receive %v, args=%v, reply=%v.\n", ck.gid, ck.me, err, args, reply)
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *ConfigureClerk) sendReConfiguring(clientId int64, commandId int64, config shardctrler.Config) (configState ConfigState, err Err) {
	args := ReConfiguringArgs{
		Identity: Identity{clientId, commandId},
		Config:   config,
	}
	reply := ReConfiguringReply{}
	ck.kv.ReConfiguring(&args, &reply)
	return reply.ConfigState, reply.Err
}

func (ck *ConfigureClerk) sendReConfigured(clientId int64, commandId int64, partialConfig PartialConfiguration) (missingShards []int, err Err) {
	args := ReConfiguredArgs{
		Identity:             Identity{clientId, commandId},
		PartialConfiguration: partialConfig,
	}
	reply := ReConfiguredReply{}
	ck.kv.ReConfigured(&args, &reply)
	return reply.MissingShards, reply.Err
}
