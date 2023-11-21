package raft

/**
 * Only the server who has the latest commited log can be leader,
 * only the log which was replicate on the majority servers can be commited
 *
 */
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
	//	"bytes"

	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

type RoleT int

const (
	LEADER    RoleT = 0
	FOLLOWER  RoleT = 1
	CANDIDATE RoleT = 2
)

const ELECTION_TIMEOUT time.Duration = 300 * time.Millisecond

var HEARTBEAT_TIME_INTERVAL time.Duration = 100

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

func (args *ApplyMsg) String() string {
	var str string
	if args.CommandValid {
		str = fmt.Sprintf("{CommandValid: %v, Command: %v, CommandIndex: %v}",
			args.CommandValid, args.Command, args.CommandIndex)
	} else if args.SnapshotValid {
		str = fmt.Sprintf("{ SnapshotValid: %v, SnapshotTerm: %v, SnapshotIndex: %v, Snapshot:%v}",
			args.SnapshotValid, args.SnapshotTerm, args.SnapshotIndex, len(args.Snapshot))
	}
	return str
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote

}

type AppendEntriesArgs struct {
	Term              int     // leader’s term
	LeaderId          int     // so follower can redirect clients
	PrevLogIndex      int     // index of log entry immediately preceding new ones
	PrevLogTerm       int     // term of prevLogIndex entry
	Entries           []Entry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommitIndex int     // leader’s commitIndex
}

func (args *AppendEntriesArgs) String() string {
	return fmt.Sprintf("{Term: %v, LeaderId: %v, PrevLogIndex: %v, PrevLogTerm: %v, Entries: %v, LeaderCommitIndex:%v }",
		args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), args.LeaderCommitIndex)
}

type AppendEntriesReply struct {
	Term          int // currentTerm, for leader to update itself
	ConflictTerm  int // term of the conflicting entry
	ConflictIndex int
	Success       bool // true if follower contained entry matching prevLogIndex and prevLogTerm

	LeaderId int // for debug
}

type InstallSnapshotArgs struct {
	Term              int    // leader’s term
	LeaderId          int    // so follower can redirect clients
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of lastIncludedIndex
	Data              []byte // raw bytes of the snapshot chunk, starting at offset
	// offset int // byte offset where chunk is positioned in the snapshot file
	// done bool  // raw bytes of the snapshot chunk, starting at offset
}

func (args *InstallSnapshotArgs) String() string {
	return fmt.Sprintf("{Term: %v, LeaderId: %v, LastIncludedIndex: %v, LastIncludedTerm: %v, Data: %v }",
		args.Term, args.LeaderId, args.LastIncludedIndex, args.LastIncludedTerm, len(args.Data))
}

type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers:
	currentTerm   int // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor      int // candidateId that received vote in current term (or null if none)
	log           Log
	snapshotTerm  int
	snapshotIndex int
	snapshot      []byte

	// Volatile state on all servers:
	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	applyCond   *sync.Cond

	// Volatile state on leaders:
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	role RoleT
	// leaderId int
	lastHeartbeat time.Time
	// commitCh      chan int
	applyCh chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == LEADER
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	rf.persister.Save(rf.stateBytes(), rf.snapshot)
}

func (rf *Raft) persistSate() {
	rf.persister.SaveRaftState(rf.stateBytes())
}

func (rf *Raft) stateBytes() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log.entries)
	e.Encode(rf.snapshotIndex)
	e.Encode(rf.snapshotTerm)
	raftstate := w.Bytes()
	return raftstate
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	Debug(TraceEvent, rf.me, "Restore From Persist")
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor, snapshotIndex, snapshotTerm int
	var entries []Entry
	if err := d.Decode(&currentTerm); err != nil {
		panic(err)
	}
	if err := d.Decode(&votedFor); err != nil {
		panic(err)
	}
	if err := d.Decode(&entries); err != nil {
		panic(err)
	}
	if err := d.Decode(&snapshotIndex); err != nil {
		panic(err)
	}
	if err := d.Decode(&snapshotTerm); err != nil {
		panic(err)
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = Log{entries, snapshotIndex}
	rf.snapshotIndex = snapshotIndex
	rf.snapshotTerm = snapshotTerm

}

/**
 * If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
 */
func (rf *Raft) stepDown(term int) {
	if rf.role == LEADER {
		Debug(TraceEvent, rf.me, "stepdown")
	}
	rf.currentTerm = term
	rf.role = FOLLOWER
	rf.votedFor = -1
	rf.persistSate()
}

/**
 * you should only restart your election timer if
 * a) you get an AppendEntries RPC from the current leader (i.e., if the term in the AppendEntries arguments is outdated, you should not reset your timer);
 * b) you are starting an election; or
 * c) you grant a vote to another peer.
 */
func (rf *Raft) resetElectionTimer() {
	rf.lastHeartbeat = time.Now()
}

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
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader = rf.role == LEADER
	if !isLeader {
		return
	}

	term = rf.currentTerm
	entry := Entry{
		Command: command,
		Term:    rf.currentTerm,
	}
	rf.log.append(entry)
	index = rf.log.lastIndex()
	rf.persistSate()
	rf.leaderLogReplication()
	return
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	Debug(TraceEvent, rf.me, "Kill")
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.applyCond.Broadcast()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// pause for a random amount of time between 50 and 350 milliseconds.
		time.Sleep(time.Duration(50+rand.Int63n(300)) * time.Millisecond)
		// leaderElection run in a seperate goroutine so that another election preocess can start when this election process was timeout with out a result
		go rf.leaderElection()
	}
	// log.Printf("S%d ticker finished\n", rf.me)
}

func (rf *Raft) leaderHeartbeats() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.role == LEADER {
			rf.leaderLogReplication()
			rf.mu.Unlock()
		} else {
			rf.mu.Unlock()
			break
		}

		time.Sleep(HEARTBEAT_TIME_INTERVAL * time.Millisecond)
		rf.mu.Lock()
		rf.leaderCommit()
		rf.mu.Unlock()
	}
	// log.Printf("S%d leaderHeartbeats finished\n", rf.me)
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.log = makeLog(1, 0)
	rf.role = FOLLOWER
	// rf.leaderId = -1
	rf.votedFor = -1
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()

	rf.resetElectionTimer()
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()

	return rf
}
