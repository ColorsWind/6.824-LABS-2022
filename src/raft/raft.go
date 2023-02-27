package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"sort"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// raft server state

type State string

const (
	State_FOLLOWER  = "FOLLOWER"
	State_CANDIDATE = "CANDIDATE"
	State_LEADER    = "LEADER"
)

func minInt(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func maxInt(x, y int) int {
	if x > y {
		return x
	}
	return y

}

func CommandToString(c interface{}) string {
	s := fmt.Sprintf("%v", c)
	if len(s) > 10 {
		s = s[:10]
	}
	return s
}

type LogsHolder struct {
	EntriesFirstIndex         int   // the number of log Entries that have been trimmed because of Snapshot
	Entries                   []Log // log Entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
	Snapshot                  []byte
	SnapshotLastIncludedTerm  int
	SnapshotLastIncludedIndex int
}

func (holder LogsHolder) String() string {
	return fmt.Sprintf("{EntriesFirstIndex=%v, Entries=%v, Snapshot={Len=%v, LastIncludedTerm=%v, LastIncludedIndex=%v}}", holder.EntriesFirstIndex, holder.Entries, len(holder.Snapshot), holder.SnapshotLastIncludedTerm, holder.SnapshotLastIncludedIndex)
}

func (holder *LogsHolder) length() int {
	return len(holder.Entries) + holder.EntriesFirstIndex
}

func (holder *LogsHolder) append(entries ...Log) {
	holder.Entries = append(holder.Entries, entries...)
}

func (holder *LogsHolder) slice(begin, end int) []Log {
	return holder.Entries[begin-holder.EntriesFirstIndex : end-holder.EntriesFirstIndex]
}

func (holder *LogsHolder) trim(newLength int) {
	holder.Entries = holder.Entries[:newLength-holder.EntriesFirstIndex]

}

func (holder *LogsHolder) get(index int) Log {
	return holder.Entries[index-holder.EntriesFirstIndex]
}

//func (holder *LogsHolder) set(index int, entry Log) {
//	holder.Entries[index] = entry
//}

// find first occurrence log which term is given term in entries
// assume len(entries) = n, then:
// 1. if entries[i].Term == term, return the smallest i;
// 2. if not found and given term is greater than entries[n-1], return n;
// 3. else return the smallest such that entries[i].Term > term
func (holder *LogsHolder) searchTerm(term int, searchLen int) int {
	return sort.Search(searchLen-holder.EntriesFirstIndex, func(i int) bool {
		return holder.Entries[i].Team >= term
	}) + holder.EntriesFirstIndex
}

func (holder *LogsHolder) applySnapshot(data []byte, index int, term int) {
	if index <= holder.SnapshotLastIncludedIndex {
		// ignore
		return
	}
	if index >= holder.length() {
		holder.Entries = []Log{}
	} else {
		holder.Entries = holder.Entries[index-holder.EntriesFirstIndex:]
	}
	holder.Snapshot = data
	holder.SnapshotLastIncludedIndex = index
	holder.SnapshotLastIncludedTerm = term
	holder.EntriesFirstIndex = holder.SnapshotLastIncludedIndex + 1
}

func (holder *LogsHolder) hasHold(index int) bool {
	return index-holder.EntriesFirstIndex >= 0 && len(holder.Entries) > 0
}

//func (holder *LogsHolder) hasNewerSnapshot(index int, term int) bool {
//	if holder.LastIncludedTerm > term {
//		return true
//	} else if holder.LastIncludedTerm == term {
//		return holder.LastIncludedIndex > index
//	} else {
//		return false
//	}
//}

func (holder *LogsHolder) getPrevLogIndex() int {
	return holder.SnapshotLastIncludedIndex + len(holder.Entries)
}

func (holder *LogsHolder) getPrevLogTerm() int {
	if len(holder.Entries) > 0 {
		return holder.Entries[len(holder.Entries)-1].Team
	}
	return holder.SnapshotLastIncludedTerm
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	electionTimer bool
	state         State
	applyCond     *sync.Cond

	applyCh chan ApplyMsg

	/*
		Persistent state on all servers:
		(Updated on stable storage before responding to RPCs)
	*/
	currentTerm int        // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int        // candidateId that received vote in current term (or null if none)
	log         LogsHolder // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	/*
		Volatile state on all servers:
	*/
	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	/*
		Volatile state on leaders:
		(Reinitialized after election)
	*/
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
	// in normal cases, nextIndex = matchIndex + 1, but for followers that are far behind, nextIndex is initialized to
	// last log index, matchIndex is initialized to 0. After message exchanged, nextIndex and matchIndex will be
	// updated to the real value.

}

func (rf *Raft) String() string {
	return fmt.Sprintf("{me=%v, state=%v, electionTimer=%v, currentTerm=%v, voteFor=%v, commitIndex=%v, "+
		"lastApply=%v, nextIndex=%v, matchIndex=%v, log=%v}",
		rf.me, rf.state, rf.electionTimer, rf.currentTerm, rf.votedFor, rf.commitIndex, rf.lastApplied,
		rf.nextIndex, rf.matchIndex, rf.log)
}

// struct for log entry
type Log struct {
	Team    int
	Command interface{}
}

func (l Log) String() string {
	return fmt.Sprintf("Log{cmd=%v, term=%v}", CommandToString(l.Command), l.Team)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.state == State_LEADER
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)

	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	if err := d.Decode(&currentTerm); err != nil {
		log.Panicf("%v: error at readPersist: currentTerm, err=%v.\n", rf.me, err)
	} else {
		rf.currentTerm = currentTerm
	}
	var votedFor int
	if err := d.Decode(&votedFor); err != nil {
		log.Panicf("%v: error at readPersist: votedFor, err=%v.\n", rf.me, err)
	} else {
		rf.votedFor = votedFor
	}
	var holder LogsHolder
	if err := d.Decode(&holder); err != nil {
		log.Panicf("%v: error at readPersist: holder, err=%v.\n", rf.me, err)
	} else {
		rf.log = holder
	}
	log.Printf("%v: boot from persisit.\n", rf.me)
}

//
// A service wants to switch to Snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the Snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	accept := rf.log.SnapshotLastIncludedIndex < lastIncludedIndex
	log.Printf("%v: service wants to switch to Snapshot, term=%v, index=%v, accept=%v", rf.me, lastIncludedTerm, lastIncludedIndex, accept)
	return accept
}

// the service says it has created a Snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("%v: service says it has created a snapshot. index=%v, rf=%v\n", rf.me, index, rf)
	term := rf.log.get(index).Team
	log.Printf("%v: query snapshot last included index. index=%v, queryTerm=%v\n", rf.me, index, term)
	// reserve at least one log entry
	rf.log.applySnapshot(snapshot, index, term)
	rf.persist()
	log.Printf("%v: after discard, rf=%v.\n", rf.me, rf)
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entry
}

func (args RequestVoteArgs) String() string {
	return fmt.Sprintf("{Term=%v, CandidateId=%v, LastLogIndex=%v, LastLogTerm=%v}", args.Term, args.CandidateId, args.LastLogIndex, args.LastLogTerm)
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

func (reply RequestVoteReply) String() string {
	return fmt.Sprintf("{Term=%v, VoteGranted=%v}", reply.Term, reply.VoteGranted)
}

//
// RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("%v <- %v: receive RequestVote. rf=%v, args=%v\n", rf.me, args.CandidateId, rf, args)
	lastLogIndex := rf.log.length() - 1

	if rf.currentTerm > args.Term {
		// case1: Reply false if term < currentTerm (§5.1.1)
		reply.VoteGranted = false
	} else if (rf.votedFor < 0 || rf.currentTerm != args.Term || rf.votedFor == args.CandidateId) &&
		(rf.log.get(lastLogIndex).Team < args.LastLogTerm || (rf.log.get(lastLogIndex).Team == args.LastLogTerm && rf.log.length() <= args.LastLogIndex+1)) {
		// case2: If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
		rf.votedFor = args.CandidateId
		rf.persist()
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}

	// must update rf.currentTerm after handle vote request, because here check rf.currentTerm != args.Term which will be overwritten
	if args.Term > rf.currentTerm {
		log.Printf("%v <- %v: recevier found higher term(%v > %v) when handling RequestVote, changed %v to FOLLOWER.\n", rf.me, args.CandidateId, args.Term, rf.currentTerm, rf.state)
		rf.state = State_FOLLOWER
		rf.currentTerm = args.Term
		rf.persist()
	}

	reply.Term = rf.currentTerm
	log.Printf("%v <- %v: reply RequestVote. rf=%v, reply=%v\n", rf.me, args.CandidateId, rf, reply)
}

type AppendEntriesArgs struct {
	Term         int   // leader’s term
	LeaderId     int   // so follower can redirect clients
	PrevLogIndex int   // index of log entry immediately preceding new ones
	PrevLogTerm  int   // term of prevLogIndex entry
	Entries      []Log // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int   // leader’s commitIndex
}

func (args AppendEntriesArgs) String() string {
	return fmt.Sprintf("{Term=%v, LeaderId=%v, PrevLogTerm=%v, PrevLogIndex=%v, Entries=%v, LeaderCommit=%v}", args.Term, args.LeaderId, args.PrevLogTerm, args.PrevLogIndex, args.Entries, args.LeaderCommit)
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm

	// reject message, -1 for none
	XTerm  int // term in the conflicting entry (if any)
	XIndex int // index of first entry with that term (if any)
	XLen   int // log length
}

func (reply AppendEntriesReply) String() string {
	return fmt.Sprintf("{Term=%v, Success=%v}", reply.Term, reply.Success)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//log.Printf("%v <- %v: receive AppendEntries, args=%v\n", rf.me, args.LeaderId, args)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("%v <- %v: receive AppendEntries, rf=%v, args=%v.\n", rf.me, args.LeaderId, rf, args)

	rf.electionTimer = true

	reply.XTerm = -1
	reply.XIndex = -1
	reply.XLen = -1

	if args.Term > rf.currentTerm {
		log.Printf("%v <- %v: found higher term(%v > %v) when pressing AppendEntriesArgs, changed %v to FOLLOWER.\n", rf.me, args.LeaderId, args.Term, rf.currentTerm, rf.state)
		rf.state = State_FOLLOWER
		rf.currentTerm = args.Term
		rf.persist()

	}

	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		log.Printf("%v <- %v: found outdated AppendEntriesArgs, term=%v, currentTerm=%v.\n", rf.me, args.LeaderId, args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if args.PrevLogIndex < 0 || args.PrevLogIndex >= rf.log.length() || rf.log.get(args.PrevLogIndex).Team != args.PrevLogTerm {

		reply.Success = false
		reply.Term = rf.currentTerm

		if len(args.Entries) > 0 {
			reply.XLen = rf.log.length()
			if args.PrevLogIndex < reply.XLen {
				reply.XTerm = rf.log.get(args.PrevLogIndex).Team
				reply.XIndex = rf.log.searchTerm(reply.XTerm, args.PrevLogIndex)
				if xterm1 := rf.log.get(reply.XIndex).Team; xterm1 != reply.XTerm {
					log.Panicf("%v <- %v: binrary search fail, expect: %v, but get %v. rf=%v\n.", rf.me, args.LeaderId, reply.XTerm, xterm1, rf)
				}
			}
			log.Printf("%v <- %v: receive AppendEntries, but receiver's log doesn't contain matcheing. args=%v, rf=%v, reply=%v.\n", rf.me, args.LeaderId, args, rf, reply)
		}

		return
	}
	// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and
	//    all that follow it (§5.3)
	// Since already check rf.log[args.PrevLogIndex].Team == args.PrevLogTerm, here args.PrevLogIndex < rf.log.length()
	// raft log: -----xxxx++++
	// args log:       xxx****
	// consistencyCount is the number of 'x' in args log (log that consistent with me in Entries)
	//log.Printf("%v <- %v: accept.\n args=%#v\n rf=%v.\n", rf.me, args.LeaderId, args, rf.s())

	if len(args.Entries) > 0 {
		lastConsistentIndex := args.PrevLogIndex
		for index := args.PrevLogIndex + 1; index < rf.log.length(); index++ {
			j := index - (args.PrevLogIndex + 1)
			if j >= len(args.Entries) {
				lastConsistentIndex = rf.log.length() - 1
				break
			} else if rf.log.get(index).Team == args.Entries[j].Team {
				lastConsistentIndex = index
				continue
			} else {
				break
			}
		}

		if lastConsistentIndex < rf.log.length() {
			originLength := rf.log.length()
			rf.log.trim(lastConsistentIndex + 1)
			log.Printf("%v <- %v: delete %v inconsistent log when pressing AppendEntries.", rf.me, args.LeaderId, originLength-rf.log.length())
		}

		j := lastConsistentIndex - (args.PrevLogIndex + 1)
		// 4. Append any new entries not already in the log
		if j < len(args.Entries) {
			entries := args.Entries[j+1:]
			rf.log.append(entries...)
			log.Printf("%v <- %v: append %v log entries.", rf.me, args.LeaderId, len(entries))
		}

		rf.persist()
	}
	//fmt.Printf("\n%#v\n", rf.log)

	reply.Success = true
	reply.Term = rf.currentTerm

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		commitIndex := minInt(args.LeaderCommit, rf.log.length()-1)
		rf.commitIndex = commitIndex
	}
	if rf.log.length() <= rf.commitIndex {
		log.Panicf("%v <- %v: receive AppendEntries, but found rf.log.length()(%v) <= rf.commitIndex(%v), this may be bug, rf=%v.\n", rf.me, args.LeaderId, rf.log.length(), rf.commitIndex, rf)
	}
	rf.applyCond.Signal()
	if args.Entries != nil || len(args.Entries) != 0 {
		log.Printf("%v <- %v: receive AppendEntries, accept AppendEntries. rf=%v, reply=%v.\n", rf.me, args.LeaderId, rf, reply)
	}
}

// call this function WITHOUT lock
func (rf *Raft) onApplyStateMachine() {
	for !rf.killed() {
		rf.mu.Lock()
		rf.applyCond.Wait()
		for rf.commitIndex > rf.lastApplied {
			index := rf.lastApplied + 1
			if rf.log.hasHold(index) {
				command := rf.log.get(index).Command

				rf.mu.Unlock()
				rf.applyCh <- ApplyMsg{
					CommandValid:  true,
					Command:       command,
					CommandIndex:  index,
					SnapshotValid: false,
					Snapshot:      nil,
					SnapshotTerm:  -1,
					SnapshotIndex: -1,
				}
				rf.mu.Lock()
				// only one goroutine modify lastApplied, so we can do this directly.
				log.Printf("%v: apply command to state machine: index=%v, command=%v, rf=%v!\n", rf.me, index, CommandToString(command), rf)
				rf.lastApplied = index
			} else {
				rf.mu.Unlock()
				rf.applyCh <- ApplyMsg{
					CommandValid:  false,
					Command:       nil,
					CommandIndex:  -1,
					SnapshotValid: true,
					Snapshot:      rf.log.Snapshot,
					SnapshotTerm:  rf.log.SnapshotLastIncludedTerm,
					SnapshotIndex: rf.log.SnapshotLastIncludedIndex,
				}
				rf.mu.Lock()
				log.Printf("%v: apply snapshot to state machine: index=%v: term=%v, rf=%v!\n", rf.me, index, rf.log.SnapshotLastIncludedTerm, rf)
				rf.lastApplied = rf.log.SnapshotLastIncludedIndex
			}
		}
		rf.mu.Unlock()
	}
}

type InstallSnapshotArgs struct {
	Term              int    // leader’s term
	LeaderId          int    // so follower can redirect clients
	LastIncludedIndex int    //the Snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    //term of LastIncludedIndex
	Data              []byte // raw bytes of the Snapshot chunk

}

func (args InstallSnapshotArgs) String() string {
	return fmt.Sprintf("{Term=%v, LeaderId=%v, LastIncludedIndex=%v, LastIncludedTerm=%v, Data=[]byte{len=%v}}", args.Term, args.LeaderId, args.LastIncludedIndex, args.LastIncludedTerm, len(args.Data))
}

type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

func (reply InstallSnapshotReply) String() string {
	return fmt.Sprintf("{Term=%v}", reply.Term)
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("%v <- %v: receive InstallSnapshot, rf=%v, args=%v.\n", rf.me, args.LeaderId, rf, args)
	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		return
	}
	if rf.currentTerm < args.Term {
		log.Printf("%v <- %v: receive InstallSnapshot, found higher term (%v > %v) in args, change state to FOLLOWER.\n", rf.me, args.LeaderId, args.Term, rf.currentTerm)
		rf.currentTerm = args.Term
		rf.persist()
		rf.state = State_FOLLOWER
		reply.Term = rf.currentTerm
	}
	if rf.log.SnapshotLastIncludedIndex >= args.LastIncludedIndex {
		log.Printf("%v <- %v: receive InstallSnapshot, found higher LastIncludeIndex (%v >= %v), current is newer.\n", rf.me, args.LeaderId, rf.log.SnapshotLastIncludedIndex, args.LastIncludedTerm)
		return
	}
	rf.log.applySnapshot(args.Data, args.LastIncludedIndex, args.LastIncludedTerm)
	rf.persist()
	log.Printf("%v <- %v: reply InstallSnapshot, rf=%v, reply=%v.\n", rf.me, args.LeaderId, rf, reply)
	rf.applyCond.Signal()

}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	index := -1
	term := -1
	isLeader := false
	// Your code here (2B).
	log.Printf("%v: Start: %v\n", rf.me, CommandToString(command))
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("%v: Start: %v, GetState Success, rf=%v.\n", rf.me, CommandToString(command), rf)
	if rf.state != State_LEADER {
		return index, term, isLeader
	}

	index = rf.log.length()
	term = rf.currentTerm
	isLeader = true

	// append to leader's log
	rf.log.append(Log{term, command})
	rf.persist()

	lastLogIndex := rf.log.length() - 1

	for server := 0; server < len(rf.peers); server++ {
		if server == rf.me {
			continue
		}
		server := server
		go func() {
			rf.mu.Lock()
			for {
				retry, immediate, success := rf.checkSendAppendEntriesWithLock(server, term, lastLogIndex)
				if success {
					rf.checkCommitWithLock()
				}
				log.Printf("%v -> %v: retry=%v, immediate=%v, success=%v, nextIndex=%v, matchIndex=%v.\n", rf.me, server, retry, immediate, success, rf.nextIndex[server], rf.matchIndex[server])
				if !retry {
					break
				}
				if !immediate {
					rf.mu.Unlock()
					time.Sleep(100 * time.Millisecond)
					rf.mu.Lock()
				}
			}
			rf.mu.Unlock()
		}()
	}

	return index, term, isLeader
}

// leader wants to send log to follower
// parameters:
// expectTerm          	since raft instance may lose leadership, we need to re-check assumption
// expectLastLogIndex	since concurrent request may already update follower, we need to re-check assumption
// return:
// retry				need to retry after update nextIndex, but if assumption re-check fail, no need to retry
// immediate			append may fail because nextIndex check fails or network fail, we should decide retry delay
// success				if append success, commitIndex may need to updated
// call this function WITH lock
func (rf *Raft) checkSendAppendEntriesWithLock(server int, expectTerm int, expectLastLogIndex int) (retry bool, immediate bool, success bool) {
	retry = false
	immediate = false
	success = false
	if rf.state != State_LEADER || rf.currentTerm != expectTerm {
		return
	}
	lastLogIndex := rf.log.length() - 1
	if lastLogIndex < rf.nextIndex[server] || lastLogIndex != expectLastLogIndex {
		return
	}
	term := rf.currentTerm
	prevLogIndex := rf.nextIndex[server] - 1
	if rf.log.hasHold(prevLogIndex) {
		prevLogTerm := rf.log.get(prevLogIndex).Team
		// should send AppendEntries with THAT term to prevent incorrectly receiving outdated messages
		args := AppendEntriesArgs{term, rf.me, prevLogIndex, prevLogTerm, rf.log.slice(prevLogIndex+1, rf.log.length()), rf.commitIndex}
		reply := AppendEntriesReply{}
		log.Printf("%v -> %v: send AppendEntries, args=%v.\n", rf.me, server, args)

		// send RPC, temporarily release the lock
		rf.mu.Unlock()
		ok := rf.sendAppendEntries(server, &args, &reply)
		rf.mu.Lock()
		if !ok {
			log.Printf("%v -> %v: send AppendEntries, fail to get reply, retry.\n", rf.me, server)
			retry = true
			return
		}
		if reply.Term > rf.currentTerm {
			log.Printf("%v -> %v: send AppendEntries, found higher term (%v > %v) in reply, change state to FOLLOWER.\n", rf.me, server, reply.Term, rf.currentTerm)
			rf.currentTerm = reply.Term
			rf.persist()
			rf.state = State_FOLLOWER

			return
		}

		if rf.currentTerm != term {
			log.Printf("%v -> %v: send AppendEntries, get outdated reply, term(%v) != currentTerm(%v).\n", rf.me, server, term, rf.currentTerm)
			return
		}

		if !reply.Success {
			var nextIndex int
			if reply.XTerm >= 0 {
				xindex1 := rf.log.searchTerm(rf.log.length(), reply.XTerm)
				if xindex1 >= rf.log.length() || rf.log.get(xindex1).Team != reply.XTerm {
					// Case 1: leader doesn't have XTerm
					nextIndex = reply.XIndex
					log.Printf("%v -> %v: send AppendEntries, leader doesn't have XTerm, nextIndex %v -> %v.\n", rf.me, server, rf.nextIndex[server], nextIndex)
				} else {
					// Case 2: leader has XTerm
					nextIndex = xindex1
					log.Printf("%v -> %v: send AppendEntries, leader has XTerm, nextIndex %v -> %v.\n", rf.me, server, rf.nextIndex[server], nextIndex)
				}
			} else {
				// Case 3: follower's log is too short
				nextIndex = reply.XLen
				log.Printf("%v -> %v: send AppendEntries, follower's log is too short, nextIndex %v -> %v.\n", rf.me, server, rf.nextIndex[server], nextIndex)
			}

			rf.nextIndex[server] = nextIndex
			retry = true
			immediate = true
			return
		}

		if rf.state == State_LEADER && term == rf.currentTerm {
			// If successful: update nextIndex and matchIndex for follower (§5.3)
			// However, concurrent call may have success, so check (index increases monotonically).
			if rf.nextIndex[server] < lastLogIndex+1 {
				rf.nextIndex[server] = lastLogIndex + 1
			}
			if rf.matchIndex[server] < lastLogIndex {
				rf.matchIndex[server] = lastLogIndex
			}
		}
		log.Printf("%v -> %v: send AppendEntries, success, nextIndex=%v.\n", rf.me, server, rf.nextIndex[server])

	} else {
		args := InstallSnapshotArgs{rf.currentTerm, rf.me, rf.log.SnapshotLastIncludedIndex, rf.log.SnapshotLastIncludedTerm, rf.log.Snapshot}
		reply := InstallSnapshotReply{}
		log.Printf("%v -> %v: prevLogIndex=%v, which has beed discarded. Send InstallSnapshot. args=%v.\n", rf.me, server, prevLogIndex, args)
		rf.mu.Unlock()
		ok := rf.sendInstallSnapshot(server, &args, &reply)
		rf.mu.Lock()
		if !ok {
			log.Printf("%v -> %v: send InstallSnapshot, fail to get reply, retry.\n", rf.me, server)
			retry = true
			return
		}
		if reply.Term > rf.currentTerm {
			log.Printf("%v -> %v: send InstallSnapshot, found higher term (%v > %v) in reply, change state to FOLLOWER.\n", rf.me, server, reply.Term, rf.currentTerm)
			rf.currentTerm = reply.Term
			rf.persist()
			rf.state = State_FOLLOWER
			return
		}

		if rf.currentTerm != term {
			log.Printf("%v -> %v: send InstallSnapshot, get outdated reply, term(%v) != currentTerm(%v).\n", rf.me, server, term, rf.currentTerm)
			return
		}

		if rf.state == State_LEADER && term == rf.currentTerm {
			// similar to AppendEntries
			if rf.nextIndex[server] < args.LastIncludedIndex+1 {
				rf.nextIndex[server] = args.LastIncludedIndex + 1
			}
			if rf.matchIndex[server] < args.LastIncludedIndex {
				rf.matchIndex[server] = args.LastIncludedIndex
			}
		}
		if rf.nextIndex[server] < rf.log.length() {
			// need another AppendEntries RPC
			retry = true
			immediate = true
		}
		log.Printf("%v -> %v: send InstallSnapshot, success, nextIndex=%v.\n", rf.me, server, rf.nextIndex[server])
	}

	success = true
	return
}

// call this function WITH lock
func (rf *Raft) checkCommitWithLock() {
	if rf.state != State_LEADER {
		return
	}
	// If there exists an N such that N > commitIndex, a majority
	// of matchIndex[N] ≥ N, and log[N].term == currentTerm:
	// set commitIndex = N
	totalServer := len(rf.peers)
	for N := rf.log.length() - 1; N > rf.commitIndex; N-- {
		if rf.log.get(N).Team != rf.currentTerm {
			break
		}
		var satisfyCount = 1
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			if rf.matchIndex[i] < N {
				continue
			}
			satisfyCount++
			if satisfyCount >= totalServer/2+1 {
				rf.commitIndex = N
				//log.Printf("%v: start command success %v (%v/%v)!\n", rf.me, Log{term, command}, successCount, totalServer)
				rf.applyCond.Signal()
				break
			}
		}
	}
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
// call this function WITHOUT lock.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		timeToSleep := rand.Intn(300) + 300
		time.Sleep(time.Duration(timeToSleep) * time.Millisecond)
		log.Printf("%v: tick.\n", rf.me)

		rf.mu.Lock()

		if rf.electionTimer {
			rf.electionTimer = false
		} else if rf.state != State_LEADER {
			rf.kickOffElectionWithLock()
		}
		rf.mu.Unlock()
	}
}

// call this function WITH lock.
func (rf *Raft) kickOffElectionWithLock() {
	log.Printf("%v: kick off election,rf=%v.\n", rf.me, rf)

	// On conversion to candidate, start election:
	//  Increment currentTerm
	//  Vote for self
	//  Reset election timer
	//  Send RequestVote RPCs to all other servers
	term := rf.currentTerm + 1
	rf.currentTerm = term
	rf.votedFor = rf.me
	rf.persist()
	rf.electionTimer = true
	rf.state = State_CANDIDATE

	totalSever := len(rf.peers)
	lastLogIndex := rf.log.length() - 1
	args := RequestVoteArgs{rf.currentTerm, rf.me, lastLogIndex, rf.log.get(lastLogIndex).Team}
	var receiveVoteCount = 1
	for server := 0; server < totalSever; server++ {
		if server == rf.me {
			continue
		}
		server := server
		args := args
		go func() {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(server, &args, &reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if !ok {
				log.Printf("%v -> %v: receive invalid RequestVodeReply, state=%v, currentTerm=%v.\n", rf.me, server, rf.state, rf.currentTerm)
				return
			}
			if reply.Term > rf.currentTerm {
				// If AppendEntries RPC received from new leader: convert to follower
				log.Printf("%v -> %v: found higher term(%v > %v) when pressing RequestVodeReply. changed %v to FOLLOWER.\n", rf.me, server, reply.Term, rf.currentTerm, rf.state)
				rf.state = State_FOLLOWER
				rf.currentTerm = args.Term
				rf.persist()
				return
			}
			if rf.state != State_CANDIDATE {
				log.Printf("%v -> %v: receive outdated RequestVodeReply, not CANDIDATE now, rf=%v.\n", rf.me, server, rf)
				return
			}
			if term != rf.currentTerm {
				// even it still candidate, a new term is found (implicit a new election begin).
				log.Printf("%v -> %v: receive outdated RequestVodeReply, currentTerm=%v, but rf's term when sending is %v.\n", rf.me, server, rf.currentTerm, term)
				return
			}
			if !reply.VoteGranted {
				log.Printf("%v -> %v: not voted.\n", rf.me, server)
				return
			}
			receiveVoteCount += 1
			log.Printf("%v -> %v: get voted(%v/%v).\n", rf.me, server, receiveVoteCount, totalSever)
			if receiveVoteCount == totalSever/2+1 {
				//  If votes received from majority of servers: become leader
				rf.becomeLeaderWithLock(receiveVoteCount, totalSever)
			}

		}()
	}
}

// call this function WITH lock
func (rf *Raft) becomeLeaderWithLock(receiveVoteCount, totalSever int) {
	log.Printf("%v: win election: %v/%v.\n", rf.me, receiveVoteCount, totalSever)
	rf.state = State_LEADER
	rf.sendLeaderHeartbeatWithLock()
	for k := 0; k < totalSever; k++ {
		rf.nextIndex[k] = rf.log.length()
		rf.matchIndex[k] = 0
	}

}

// call this function WITH lock.
func (rf *Raft) sendLeaderHeartbeatWithLock() {
	log.Printf("%v: send HEARTBEAT, state=%v, currentTerm=%v.\n", rf.me, rf.state, rf.currentTerm)
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.log.getPrevLogIndex(),
		PrevLogTerm:  rf.log.getPrevLogTerm(),
		Entries:      nil,
		LeaderCommit: rf.commitIndex,
	}
	// not possible to happen args.Term > rf.currentTerm

	for server := 0; server < len(rf.peers); server++ {
		if server == rf.me {
			continue
		}
		args := args
		server := server
		go func() {
			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(server, &args, &reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if ok {
				if reply.Term > rf.currentTerm {
					log.Printf("%v -> %v: send HEARTBEAT, found higher term(%v > %v), changed %v to FOLLOWER.\n", rf.me, server, reply.Term, rf.currentTerm, rf.state)
					rf.state = State_FOLLOWER
					rf.currentTerm = args.Term
					rf.persist()
				}
			} else {
				log.Printf("%v -> %v: send HEARTBEAT, receive invalid reply.", rf.me, server)
			}
		}()
	}

}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	rf.state = State_FOLLOWER
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// Your initialization code here (2A, 2B, 2C).
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.log = LogsHolder{
		EntriesFirstIndex:         0,
		Entries:                   []Log{{0, nil}},
		Snapshot:                  nil,
		SnapshotLastIncludedIndex: -1,
		SnapshotLastIncludedTerm:  -1,
	}
	rf.applyCond = sync.NewCond(&rf.mu)

	log.SetFlags(log.Lshortfile | log.Lmicroseconds)
	//log.SetOutput(ioutil.Discard)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go func() {
		for rf.killed() == false {
			rf.mu.Lock()
			log.Printf("%v: tick heartbeat, rf=%v.\n", rf.me, rf)
			if rf.state == State_LEADER {
				rf.sendLeaderHeartbeatWithLock()
			}
			rf.mu.Unlock()
			time.Sleep(140 * time.Millisecond)
		}
	}()
	go rf.onApplyStateMachine()
	//for server := range rf.peers {
	//	if server == rf.me {
	//		continue
	//	}
	//	server := server
	//	go func() {
	//		for rf.killed() == false {
	//			rf.mu.Lock()
	//			immediate := rf.checkSendAppendEntriesWithLock(server)
	//			rf.mu.Unlock()
	//			if !immediate {
	//				time.Sleep(10 * time.Millisecond)
	//			}
	//		}
	//	}()
	//}
	//go func() {
	//	for rf.killed() == false {
	//		rf.mu.Lock()
	//		//rf.checkCommitWithLock()
	//		rf.mu.Unlock()
	//		time.Sleep(10 * time.Millisecond)
	//	}
	//
	//}()

	return rf
}
