package shardctrler

import (
	"log"
	"sort"
	"testing"
	"time"
)
import "math/rand"

func TestReBalance(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	groupN := rand.Intn(NShards) + 1
	gil := make(GroupItemList, groupN)
	log.Printf("Test groupN=%v.\n", groupN)
	for k := 0; k < NShards; k++ {
		randNum := rand.Intn(groupN)
		gil[randNum].shards = append(gil[randNum].shards, k)
	}
	average := NShards / groupN
	remainder := NShards - groupN*average
	expected := make([]int, groupN)
	for k := 0; k < remainder; k++ {
		expected[k] = average + 1
	}
	for k := remainder; k < groupN; k++ {
		expected[k] = average
	}
	gil = reBalance(gil)
	for k := 0; k < groupN; k++ {
		got := len(gil[k].shards)
		want := expected[k]
		if got != want {
			t.Errorf("index=%v, want=%v, got=%v. context: expected=%v, groupN=%v, gil=%v.", k, want, got, expected, groupN, gil)
		}
	}
	var shards sort.IntSlice
	for k := 0; k < groupN; k++ {
		shards = append(shards, gil[k].shards...)
	}
	sort.Sort(shards)
	for k := 0; k < NShards; k++ {
		got := shards[k]
		want := k
		if got != want {
			t.Errorf("index=%v, want=%v, got=%v. context: expected=%v, groupN=%v, gil=%v, exist=%v.", k, want, got, expected, groupN, gil, shards)
		}
	}
}
