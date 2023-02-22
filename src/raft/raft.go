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
	"fmt"
	"log"
	"math/rand"
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

func maxnt(x, y int) int {
	if x > y {
		return x
	}
	return y

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

	applyCh chan ApplyMsg

	/*
		Persistent state on all servers:
		(Updated on stable storage before responding to RPCs)
	*/
	currentTerm int   // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int   // candidateId that received vote in current term (or null if none)
	log         []Log // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

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

}

func (rf *Raft) String() string {
	return fmt.Sprintf("{me=%v, state=%v, electionTimer=%v, currentTerm=%v, voteFor=%v, commitIndex=%v, lastApply=%v, nextIndex=%v, log=%v}", rf.me, rf.state, rf.electionTimer, rf.currentTerm, rf.votedFor, rf.commitIndex, rf.lastApplied, rf.nextIndex, rf.log)
}

// struct for log entry
type Log struct {
	Team    int
	Command interface{}
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
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

func CommandToString(c interface{}) string {
	s := fmt.Sprintf("%v", c)
	if len(s) > 5 {
		s = s[:5]
	}
	return s
}

func (l Log) String() string {
	return fmt.Sprintf("Log{cmd=%v, term=%v}", CommandToString(l.Command), l.Team)
}

//func (args AppendEntriesArgs) String() string {
//	return fmt.Sprintf("{Term=%v, LeaderId=%v, Entries=%v, PrevLogTerm=%v, PrevLogIndex=%v, LeaderCommit=%v}", args.Term, args.LeaderId, args.Entries, args.PrevLogTerm, args.PrevLogIndex, args.LeaderCommit)
//}

//
// RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.log[len(rf.log)-1].Team > args.Term {
		// case1: Reply false if term < currentTerm (§5.1.1)
		reply.VoteGranted = false
	} else if (rf.votedFor < 0 || rf.currentTerm != args.Term || rf.votedFor == args.CandidateId) &&
		(rf.log[len(rf.log)-1].Team < args.LastLogTerm || rf.log[len(rf.log)-1].Team == args.LastLogTerm && len(rf.log)-1 <= args.LastLogIndex) {
		// case2: If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
	log.Printf("%v <- %v: receive vote request. rf=%v, args=%#v\n", rf.me, args.CandidateId, rf, args)

	// must update rf.currentTerm after handle vote request, because here check rf.currentTerm != args.Term
	if args.Term > rf.currentTerm {
		log.Printf("%v <- %v: recevier found higher term(%v > %v) when handling RequestVote, changed %v to FOLLOWER.\n", rf.me, args.CandidateId, args.Term, rf.currentTerm, rf.state)
		rf.state = State_FOLLOWER
		rf.currentTerm = args.Term
	}

	reply.Term = rf.currentTerm

}

type AppendEntriesArgs struct {
	Term         int   // leader’s term
	LeaderId     int   // so follower can redirect clients
	PrevLogIndex int   // index of log entry immediately preceding new ones
	PrevLogTerm  int   // term of prevLogIndex entry
	Entries      []Log // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int   // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//log.Printf("%v <- %v: receive AppendEntries, args=%v\n", rf.me, args.LeaderId, args)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("%v <- %v: receive AppendEntries, args=%v, rf=%v.\n", rf.me, args.LeaderId, args, rf)

	rf.electionTimer = true

	if args.Term > rf.currentTerm {
		log.Printf("%v <- %v: receiver found higher term(%v > %v) when handling AppendEntries, changed %v to FOLLOWER.\n", rf.me, args.LeaderId, args.Term, rf.currentTerm, rf.state)
		rf.state = State_FOLLOWER
		rf.currentTerm = args.Term

	}

	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		log.Printf("%v <- %v: receive outdated AppendEntries, term=%v, currentTerm=%v.\n", rf.me, args.LeaderId, args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// decoder will convert nil to []
	if args.Entries != nil && len(args.Entries) > 0 {
		// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
		if args.PrevLogIndex < 0 || args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Team != args.PrevLogTerm {
			log.Printf("%v <- %v: receiver's log doesn’t contain matcheing log when handling AppendEntries. args=%#v, rf=%v.\n", rf.me, args.LeaderId, args, rf)
			reply.Success = false
			reply.Term = rf.currentTerm
			return
		}
		// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and
		//    all that follow it (§5.3)
		// Since already check rf.log[args.PrevLogIndex].Team == args.PrevLogTerm, here args.PrevLogIndex < len(rf.log)
		// raft log: -----xxxx++++
		// args log:       xxx****
		// consistencyCount is the number of 'x' in args log
		//log.Printf("%v <- %v: accept.\n args=%#v\n rf=%v.\n", rf.me, args.LeaderId, args, rf.s())
		var consistencyCount = len(args.Entries)
		for i, logEntry := range args.Entries {
			j := args.PrevLogIndex + 1 + i
			if j >= len(rf.log) {
				consistencyCount = i
				//log.Panicf("a")
				break
			}
			if rf.log[j].Team != logEntry.Team {
				consistencyCount = i
				//log.Panicf("b")
				break
			}
		}
		if deleteCount := len(rf.log) - (args.PrevLogIndex + consistencyCount + 1); deleteCount > 0 {
			log.Printf("%v <- %v: delete %v inconsistent log.", rf.me, args.LeaderId, deleteCount)
		}
		rf.log = rf.log[:args.PrevLogIndex+1+consistencyCount]

		// 4. Append any new entries not already in the log
		rf.log = append(rf.log, args.Entries[consistencyCount:]...)
		//fmt.Printf("\n%#v\n", rf.log)

		reply.Success = true
		reply.Term = rf.currentTerm
	} else {
		// just heartbeat
		reply.Success = false
		reply.Term = rf.currentTerm
	}

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		commitIndex := minInt(args.LeaderCommit, len(rf.log)-1)
		rf.commitIndex = commitIndex
	}
	rf.updateLastApply()
	if args.Entries != nil || len(args.Entries) != 0 {
		log.Printf("%v <- %v: accept AppendEntries. args=%v, rf=%v.\n", rf.me, args.LeaderId, args, rf)
	}

}

// call this function with lock
func (rf *Raft) updateLastApply() {
	if rf.commitIndex > rf.lastApplied {
		for index := rf.lastApplied + 1; index <= rf.commitIndex; index++ {
			command := rf.log[index].Command
			rf.applyCh <- ApplyMsg{
				CommandValid:  true,
				Command:       command,
				CommandIndex:  index,
				SnapshotValid: false,
				Snapshot:      nil,
				SnapshotTerm:  -1,
				SnapshotIndex: -1,
			}

			log.Printf("%v: apply command to state machine: %v: %v!\n", rf.me, index, CommandToString(command))
		}
		rf.lastApplied = rf.commitIndex
	}

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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("%v: Receive Start: %v, rf=%v.\n", rf.me, CommandToString(command), rf)
	if rf.state != State_LEADER {
		return index, term, isLeader
	}

	index = len(rf.log)
	term = rf.currentTerm
	isLeader = true

	// append to leader's log
	rf.log = append(rf.log, Log{term, command})

	totalServer := len(rf.peers)

	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		server := server
		// If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
		if len(rf.log)-1 >= rf.nextIndex[server] {
			go func() {
				var reply AppendEntriesReply
				for {
					rf.mu.Lock()
					prevLogIndex := rf.nextIndex[server] - 1
					prevLogTerm := rf.log[prevLogIndex].Team

					args := AppendEntriesArgs{rf.currentTerm, rf.me, prevLogIndex, prevLogTerm, rf.log[rf.nextIndex[server]:], rf.commitIndex}
					reply = AppendEntriesReply{}
					rf.mu.Unlock()
					for {
						ok := rf.sendAppendEntries(server, &args, &reply)
						log.Printf("%v -> %v: send AppendEntries, args=%v.\n", rf.me, server, args)
						if ok {
							break
						} else {
							time.Sleep(10 * time.Millisecond)
						}
					}
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.state = State_FOLLOWER
						rf.mu.Unlock()
						log.Printf("%v -> %v: found higher term (%v > %v), change state to FOLLOWER.\n", rf.me, server, reply.Term, rf.currentTerm)
						return
					}
					if !reply.Success {
						rf.nextIndex[server]--
						log.Printf("%v -> %v: AppendEntries fail, decrease nextIndex to %v.\n", rf.me, server, rf.nextIndex[server])
						rf.mu.Unlock()
					} else {
						rf.mu.Unlock()
						break
					}

				}
				rf.mu.Lock()
				defer rf.mu.Unlock()

				// If successful: update nextIndex and matchIndex for follower (§5.3)
				if rf.nextIndex[server] < index+1 {
					rf.nextIndex[server] = index + 1
				}
				if rf.matchIndex[server] < index {
					rf.matchIndex[server] = index
				}

				// If there exists an N such that N > commitIndex, a majority
				// of matchIndex[N] ≥ N, and log[N].term == currentTerm:
				// set commitIndex = N
				log.Printf("%v -> %v: AppendEntries success: %v.\n", rf.me, server, rf.nextIndex[server])
				if rf.matchIndex[server] < index {
					rf.matchIndex[server] = index
				}
				for N := len(rf.log) - 1; N > rf.commitIndex; N-- {
					if rf.log[N].Team != rf.currentTerm {
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
							rf.updateLastApply()
							break
						}
					}
				}
			}()
		} else {
			log.Panicln()
		}
	}

	return index, term, isLeader
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
			rf.kickOffElection()
		}
		rf.mu.Unlock()
	}
}

// call this function WITH lock.
func (rf *Raft) kickOffElection() {
	log.Printf("%v: kick off election, electionTimer=%v, currentTerm=%v.\n", rf.me, rf.electionTimer, rf.currentTerm)

	// On conversion to candidate, start election:
	//  Increment currentTerm
	//  Vote for self
	//  Reset election timer
	//  Send RequestVote RPCs to all other servers
	term := rf.currentTerm + 1
	rf.currentTerm = term
	rf.votedFor = rf.me
	rf.electionTimer = true
	rf.state = State_CANDIDATE

	totalSever := len(rf.peers)
	//lastLogIndex := len(rf.log) - 1
	args := RequestVoteArgs{rf.currentTerm, rf.me, rf.commitIndex, rf.log[rf.commitIndex].Team}
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
				return
			}
			if rf.state != State_CANDIDATE {
				return
			}
			if term != rf.currentTerm {
				// even it still candidate, a new term begins.
				log.Printf("%v -> %v: receive outdated RequestVodeReply, currentTerm=%v, but rf's term when sending is %v.\n", rf.me, server, rf.currentTerm, term)
				return
			}
			receiveVoteCount += 1
			if receiveVoteCount == totalSever/2+1 {
				//  If votes received from majority of servers: become leader
				rf.becomeLeader(receiveVoteCount, totalSever)
			}
			log.Printf("%v -> %v: get voted(%v/%v).\n", rf.me, server, receiveVoteCount, totalSever)
		}()
	}
}

// call this function WITH lock
func (rf *Raft) becomeLeader(receiveVoteCount, totalSever int) {
	log.Printf("%v: win election: %v/%v.\n", rf.me, receiveVoteCount, totalSever)
	rf.state = State_LEADER
	rf.sendLeaderHeartbeat()
	for k := 0; k < totalSever; k++ {
		rf.nextIndex[k] = len(rf.log)
		rf.matchIndex[k] = 0
	}

}

// call this function WITH lock.
func (rf *Raft) sendLeaderHeartbeat() {
	log.Printf("%v: send leader heartbeat, state=%v, currentTerm=%v.\n", rf.me, rf.state, rf.currentTerm)
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: len(rf.log) - 1,
		PrevLogTerm:  rf.log[len(rf.log)-1].Team,
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
					log.Printf("%v -> %v: found higher term(%v > %v) when pressing heartbeat reply, changed %v to FOLLOWER.\n", rf.me, server, reply.Term, rf.currentTerm, rf.state)
					rf.state = State_FOLLOWER
					rf.currentTerm = args.Term
				}
			} else {
				log.Printf("%v -> %v: receive invalid heartbeat reply.", rf.me, server)
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
	rf.log = append(rf.log, Log{0, nil})
	log.SetFlags(log.Lshortfile | log.Lmicroseconds)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go func() {
		for rf.killed() == false {
			rf.mu.Lock()
			log.Printf("%v: tick plus, rf=%v.\n", rf.me, rf)
			if rf.state == State_LEADER {
				rf.sendLeaderHeartbeat()
			}
			rf.mu.Unlock()
			time.Sleep(140 * time.Millisecond)
		}
	}()

	return rf
}
