package shardctrler

import (
	"6.824/raft"
	"log"
	"sort"
)

type GroupItem struct {
	gid    int
	shards []int
}

type GroupItemList []GroupItem

func (gil GroupItemList) Len() int {
	return len(gil)
}

func (gil GroupItemList) Less(i, j int) bool {
	gili := gil[i]
	gilj := gil[j]
	if len(gili.shards) > len(gilj.shards) {
		return true
	} else if len(gili.shards) == len(gilj.shards) {
		// for deterministic
		return gili.gid > gilj.gid
	} else {
		return false
	}
}

func (gil GroupItemList) Swap(i, j int) {
	gil[j], gil[i] = gil[i], gil[j]
}

func initBalance(gids []int) (shards [NShards]int) {
	shardsSorted := sort.IntSlice(shards[:])
	// for deterministic
	sort.Sort(shardsSorted)
	for k := range shardsSorted {
		shards[k] = gids[k%len(gids)]
	}
	return shards
}
func reBalance(items GroupItemList) (balancedItems GroupItemList) {
	balancedItems = make([]GroupItem, items.Len())
	copy(balancedItems, items)
	sort.Sort(balancedItems)
	average := NShards / len(items)
	remainder := NShards - len(items)*average
	expected := make([]int, len(balancedItems))
	for k := 0; k < remainder; k++ {
		expected[k] = average + 1
	}
	for k := remainder; k < len(balancedItems); k++ {
		expected[k] = average
	}

	i, j := 0, len(balancedItems)-1
	for i < j {
		shardsI := balancedItems[i].shards
		if len(shardsI) <= expected[i] {
			if len(shardsI) < expected[i] {
				log.Panicf("len of %v < %v, balancedItems=%v.", len(shardsI), expected[i], balancedItems)
			}
			i++
			continue
		}
		shardsJ := balancedItems[j].shards
		if len(shardsJ) >= expected[j] {
			if len(shardsJ) > expected[j] {
				log.Panicf("len of %v > %v, balancedItems=%v.", len(shardsJ), expected[j], balancedItems)
			}
			j--
			continue
		}
		// move balancedItems[i] to balancedItems[j]
		popN := len(shardsI) - expected[i]
		pushN := expected[j] - len(shardsJ)
		moveN := raft.MinInt(pushN, popN)
		balancedItems[i].shards = shardsI[:len(shardsI)-moveN]
		balancedItems[j].shards = append(shardsJ, shardsI[len(shardsI)-moveN:]...)
	}
	return
}

func shardToGroupItemList(shards [NShards]int, groupN int) GroupItemList {
	gidToShards := make(map[int][]int)
	for shard, gid := range shards {
		gidToShards[gid] = append(gidToShards[gid], shard)
	}
	var gil GroupItemList
	for gid, gShards := range gidToShards {
		gil = append(gil, GroupItem{gid, gShards})
	}
	return gil
}

func groupItemListToShard(gil GroupItemList) (shards [NShards]int, groupN int) {
	groupN = gil.Len()
	for _, item := range gil {
		for _, shard := range item.shards {
			shards[shard] = item.gid
		}
	}
	return shards, groupN
}
