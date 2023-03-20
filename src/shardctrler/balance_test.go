package shardctrler

import (
	"log"
	"reflect"
	"sort"
	"testing"
	"time"
)
import "math/rand"

func TestReBalance(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	groupN := rand.Intn(NShards) + 1
	seed := time.Now().UnixNano()
	log.Printf("Test groupN=%v.\n", groupN)

	// fill expected
	average := NShards / groupN
	remainder := NShards - groupN*average
	expected := make([]int, groupN)
	for k := 0; k < remainder; k++ {
		expected[k] = average + 1
	}
	for k := remainder; k < groupN; k++ {
		expected[k] = average
	}

	// test 10 times
	var firstGil GroupItemList
	for x := 0; x < 10; x++ {
		// fill gil
		rand.Seed(seed)
		xGil := make(GroupItemList, groupN)
		for k := range xGil {
			xGil[k].gid = rand.Intn(65536) + 1
		}
		for k := 0; k < NShards; k++ {
			shard := rand.Intn(groupN)
			xGil[shard].shards = append(xGil[shard].shards, k)
		}
		rand.Seed(time.Now().UnixNano())
		xGil = reBalance(xGil)
		// check count
		for k := 0; k < groupN; k++ {
			got := len(xGil[k].shards)
			want := expected[k]
			if got != want {
				t.Errorf("index=%v, want=%v, got=%v. context: expected=%v, groupN=%v, xGil=%v.", k, want, got, expected, groupN, xGil)
			}
		}
		// check if all shards occurs
		var shards sort.IntSlice
		for k := 0; k < groupN; k++ {
			shards = append(shards, xGil[k].shards...)
		}
		sort.Sort(shards)
		for k := 0; k < NShards; k++ {
			got := shards[k]
			want := k
			if got != want {
				t.Errorf("index=%v, want=%v, got=%v. context: expected=%v, groupN=%v, gil=%v, exist=%v.", k, want, got, expected, groupN, xGil, shards)
			}
		}
		// check deterministic
		if x > 0 {
			if !reflect.DeepEqual(firstGil, xGil) {
				t.Errorf("not deterministic, gil[0]=%v, gil[%v]=%v.", firstGil, x, xGil)
			}
		} else {
			firstGil = xGil
		}
	}

}

func TestInitBalance(t *testing.T) {
	//initBalance()
}

func TestConvert(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	shards := make([]int, NShards)
	groupN := rand.Intn(NShards) + 1
	log.Printf("Test groupN=%v.\n", groupN)
	for shard := 0; shard < NShards; shard++ {
		var gid int
		if shard < groupN {
			gid = shard + 100
		} else {
			gid = rand.Intn(groupN) + 100
		}
		shards[shard] = gid
	}
	gil := shardToGroupItemList(shards, groupN)
	xShards, xGroupN := groupItemListToShard(gil)
	if !reflect.DeepEqual(shards, xShards) {
		t.Errorf("shards: %v != %v, gil=%v.", shards, xShards, gil)
	}
	if groupN != xGroupN {
		t.Errorf("groupN: %v != %v.", groupN, xGroupN)
	}
}
