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
	getStateClientId   int64 // random id, should be unique globally
	configureClientId  int64
	getStateCommandId  int64 // for a client, monotonically increase from 0
	configureCommandId int64
	logger             *log.Logger
	kv                 *ShardKV
	me                 int
	gid                int
	preConfigNum       int
	configuredNum      int
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
	atomic.StoreInt64(&ck.getStateCommandId, 0)
	ck.logger = log.New(os.Stdout, "", log.Lshortfile|log.Lmicroseconds)
	ck.me = kv.me
	ck.gid = kv.gid
	ck.preConfigNum = 0
	ck.configuredNum = 0
	return ck
}

func (ck *ConfigureClerk) onPollConfiguration() {
	// !!Important: carefully check assumption, queryConfig2 may change quickly (concurrent re-configure)

	queryConfig := ck.mck.Query(-1)
	if queryConfig.Num < ck.preConfigNum {
		ck.logger.Panicf("%v-%v: queryConfig.Num < ck.conf.LastConfig.Num, something went wrong, queryConfig=%v, conf=%v\n", ck.gid, ck.me, queryConfig, ck.preConfigNum)
	}

	if queryConfig.Num == ck.preConfigNum && queryConfig.Num == ck.configuredNum {
		return
	}

	configState, err := ck.sendReConfiguring(queryConfig)
	if err != OK {
		ck.logger.Printf("%v-%v: do Re-Completed fail, Err=%v.\n", ck.gid, ck.me, err)
		return
	}

	// guess shardkv preConfig
	var alreadyCompleted bool
	var preConfig shardctrler.Config
	for i := 0; true; i++ {
		if configState.Update {
			preConfig = queryConfig
			alreadyCompleted = false
			ck.logger.Printf("%v-%v: update re-configuring, configNum=%v, configState=%v, queryConfig=%v.\n", ck.gid, ck.me, ck.preConfigNum, configState, queryConfig)
			break
		} else if !configState.Completed {
			preConfig = configState.LastConfig
			alreadyCompleted = false
			break
		} else {
			// preConfig already completed, check whether we should start new one
			preConfig = configState.LastConfig
			if configState.LastConfig.Num == queryConfig.Num-1 {
				ck.logger.Printf("%v-%v: during re-configuring, unknown situation #1, i=%v, queryConfig=%v, configState=%v, preConfig=%v, alreadyCompleted=%v.\n", ck.gid, ck.me, i, queryConfig, configState, preConfig, alreadyCompleted)
			} else if configState.LastConfig.Num < queryConfig.Num-1 {
				queryConfig = ck.mck.Query(preConfig.Num + 1)
				configState, err = ck.sendReConfiguring(queryConfig)
				ck.logger.Printf("%v-%v: query config is too new to update, retry with old one, i=%v, queryConfig=%v, configState=%v, preConfig=%v, alreadyCompleted=%v.\n", ck.gid, ck.me, i, queryConfig, configState, preConfig, alreadyCompleted)
				continue
			} else if configState.LastConfig.Num == queryConfig.Num {
				alreadyCompleted = true
				ck.logger.Printf("%v-%v: already submit and complete, i=%v, queryConfig=%v, configState=%v, preConfig=%v, alreadyCompleted=%v.\n", ck.gid, ck.me, i, queryConfig, configState, preConfig, alreadyCompleted)
				break
			} else if preConfig.Num > queryConfig.Num {
				queryConfig = ck.mck.Query(-1)
				configState, err = ck.sendReConfiguring(queryConfig)
				ck.logger.Printf("%v-%v:concurrent configuration change, i=%v, queryConfig=%v, configState=%v, preConfig=%v, alreadyCompleted=%v.\n", ck.gid, ck.me, i, queryConfig, configState, preConfig, alreadyCompleted)
				continue
			} else {
				ck.logger.Printf("%v-%v: during re-configuring, unknown situation #2, i=%v, queryConfig=%v, configState=%v, preConfig=%v, alreadyCompleted=%v.\n", ck.gid, ck.me, i, queryConfig, configState, preConfig, alreadyCompleted)
			}
		}
	}

	if preConfig.Num > ck.preConfigNum {
		ck.preConfigNum = preConfig.Num
		ck.logger.Printf("%v-%v: update last known preConfig.Num to %v.\n", ck.gid, ck.me, ck.preConfigNum)

	}

	if !alreadyCompleted {

		// may need to do re-configured
		currConfig := ck.mck.Query(preConfig.Num + 1)
		// FIXME: reduce unnecessary RPC
		if currConfig.Num == preConfig.Num {
			ck.logger.Printf("%v-%v: during re-configured, something went wrong, preConfig=%v, currConfig=%v, queryConfig=%v, configState=%v.\n", ck.gid, ck.me, preConfig, currConfig, queryConfig, configState)
			return
		}

		gid2shards := make(map[int][]int)
		for shard := range preConfig.Shards {
			lastGid := preConfig.Shards[shard]
			currGid := currConfig.Shards[shard]
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
			state, err := ck.sendGetState(preConfig, gid, shards, false)
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
		err = ck.sendReConfigured(State{currConfig.Num, getStateKVMap, getStateLastAppliedMap})
		if err != OK {
			ck.logger.Printf("%v-%v: do Re-Configured fail, Err=%v.\n", ck.gid, ck.me, err)
			return
		}
		for gid, shards := range gid2shards {
			state, err := ck.sendGetState(preConfig, gid, shards, true)
			ck.logger.Printf("%v-%v: send state confirm, get err=%v, state=%v.\n", ck.gid, ck.me, err, state)
		}
	}

	ck.configuredNum = preConfig.Num
	ck.logger.Printf("%v-%v: update last known configured queryConfig2 num to %v.\n", ck.gid, ck.me, ck.configuredNum)

}

func (ck *ConfigureClerk) sendGetState(lastConfig shardctrler.Config, gid int, shards []int, confirm bool) (state State, err Err) {
	ck.logger.Printf("%v-%v: GetState. gid=%v, shards=%v.\n", ck.gid, ck.me, gid, shards)
	args := GetStateArgs{Identity{ck.getStateClientId, atomic.AddInt64(&ck.getStateCommandId, 1)}, GetState{lastConfig.Num, shards, confirm}}
	for {
		//gid := kv.ctrlerConfig.Shards[shard]
		if servers, ok := lastConfig.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply GetStateReply
				ok := srv.Call("ShardKV.GetState", &args, &reply)
				ck.logger.Printf("%v-%v call get finishM server=%v, args=%v, reply=%v, ok=%v.\n", ck.gid, ck.me, servers[si], args, reply, ok)
				if ok && (reply.Err == OK || reply.Err == ErrShardDelete) {
					// successfully get state
					ck.logger.Printf("%v-%v: finish get state ok, args=%v, reply=%v.", ck.gid, ck.me, args, reply)
					return reply.State, reply.Err
				}
				if ok && reply.Err == ErrWrongGroup {
					//args.CommandId = atomic.AddInt64(&ck.getStateCommandId, 1)
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
		Identity: Identity{ck.configureClientId, atomic.AddInt64(&ck.configureCommandId, 1)},
		Config:   config,
	}
	reply := ReConfiguringReply{}
	ck.kv.ReConfiguring(&args, &reply)
	return reply.ConfigState, reply.Err
}

func (ck *ConfigureClerk) sendReConfigured(state State) (err Err) {
	args := ReConfiguredArgs{
		Identity: Identity{ck.configureClientId, atomic.AddInt64(&ck.configureCommandId, 1)},
		State:    state,
	}
	reply := ReConfiguredReply{}
	ck.kv.ReConfigured(&args, &reply)
	return reply.Err
}
