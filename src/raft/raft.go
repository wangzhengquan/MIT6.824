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
	//	"bytes"

	"bytes"
	"math/rand"
	"sort"
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

// var ELECTION_TIMEOUT time.Duration = time.Duration(300+rand.Int31n(700)) * time.Millisecond
const ELECTION_TIMEOUT time.Duration = time.Duration(300) * time.Millisecond

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
	currentTerm int        // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int        // candidateId that received vote in current term (or null if none)
	log         []LogEntry // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	// Volatile state on all servers:
	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// Volatile state on leaders:
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	role RoleT
	// leaderId int
	lastHeartbeat time.Time
	applyCh       chan ApplyMsg
}

type LogEntry struct {
	Command interface{}
	Term    int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)

}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		Debug(ErrorEvent, rf.me, "Decode error of func readPersist")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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
	Term              int        // leader’s term
	LeaderId          int        // so follower can redirect clients
	PrevLogIndex      int        // index of log entry immediately preceding new ones
	PrevLogTerm       int        // term of prevLogIndex entry
	Entries           []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommitIndex int        // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) lastLogIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) lastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		Debug(VoteEvent, rf.me, "Reject vote to %d for candidate term %d < current term %d.\n",
			args.CandidateId, args.Term, rf.currentTerm)
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = FOLLOWER
		rf.votedFor = -1
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(args.LastLogTerm > rf.lastLogTerm() ||
			(args.LastLogTerm == rf.lastLogTerm() && args.LastLogIndex >= rf.lastLogIndex())) {
		Debug(VoteEvent, rf.me, "Grant vote to %d.\n", args.CandidateId)
		rf.votedFor = args.CandidateId
		rf.lastHeartbeat = time.Now()
		reply.VoteGranted = true
	} else {
		Debug(VoteEvent, rf.me, "Reject vote to %d for votedFor is not null or not candidateId, or candidate’s log is not at least as up-to-date as receiver’s log.\n",
			args.CandidateId)
	}

	// rf.persist()

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.Success = false
	if args.Term < rf.currentTerm {
		Debug(HeartbeatEvent, rf.me, "reject heartbeat from %d, for candidate term %d < current term %d\n",
			args.LeaderId, args.Term, rf.currentTerm)
		return
	}

	rf.lastHeartbeat = time.Now()

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = FOLLOWER
		rf.votedFor = -1
	} else if rf.role == CANDIDATE {
		rf.role = FOLLOWER
		rf.votedFor = -1
	}

	Debug(HeartbeatEvent, rf.me, "recieve heartbeats from %d", args.LeaderId)
	if len(args.Entries) == 0 {
		reply.Success = true
		return
	}

	if args.PrevLogIndex == 0 {
		rf.log = rf.log[:1]
	} else {
		if len(rf.log) <= args.PrevLogIndex {
			return
		} else if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			rf.log = rf.log[0:args.PrevLogIndex]
			return
		}
	}

	rf.log = append(rf.log, args.Entries...)
	// follower commit
	if args.LeaderCommitIndex > rf.commitIndex {
		if args.LeaderCommitIndex < rf.lastLogIndex() {
			rf.commitIndex = args.LeaderCommitIndex
		} else {
			rf.commitIndex = rf.lastLogIndex()
		}
	}
	reply.Success = true
	go rf.applyLogToStateMachine()

}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, ch chan *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		ch <- reply
	} else {
		ch <- nil
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	// return ok
	if ok := rf.peers[server].Call("Raft.AppendEntries", args, reply); ok {
		rf.mu.Lock()
		if reply.Success {
			rf.nextIndex[server] += len(args.Entries)
			rf.matchIndex[server] += len(args.Entries)
		} else {
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.role = FOLLOWER
			} else {
				rf.nextIndex[server]--
			}
		}
		rf.mu.Unlock()
	}
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

	if rf.role != LEADER {
		return -1, -1, false
	}

	index = len(rf.log)
	term = rf.currentTerm
	logEntry := LogEntry{
		Command: command,
		Term:    rf.currentTerm,
	}
	rf.log = append(rf.log, logEntry)
	rf.persist()
	return index, term, true
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
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		time.Sleep(time.Duration(300+rand.Int63n(300)) * time.Millisecond)
		// Your code here (2A)
		// Check if a leader election should be started.
		go rf.leaderElection()
	}
}

func (rf *Raft) leaderElection() {
	rf.mu.Lock()

	if rf.role == FOLLOWER && time.Now().Sub(rf.lastHeartbeat) >= ELECTION_TIMEOUT {
		rf.role = CANDIDATE
	}

	if rf.role == CANDIDATE && time.Now().Sub(rf.lastHeartbeat) >= ELECTION_TIMEOUT {
		Debug(VoteEvent, rf.me, "Leader election start\n")
		rf.currentTerm++
		rf.votedFor = rf.me
		ch := make(chan *RequestVoteReply)

		for peerId := 0; peerId < len(rf.peers); peerId++ {
			if peerId == rf.me {
				continue
			}
			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: rf.lastLogIndex(),
				LastLogTerm:  rf.lastLogTerm(),
			}

			reply := RequestVoteReply{}
			Debug(VoteEvent, rf.me, "Send request vote to %d with term: %d\n", peerId, rf.currentTerm)
			go rf.sendRequestVote(peerId, &args, &reply, ch)
		}
		rf.mu.Unlock()

		voteGrantedCount := 1
		majority := len(rf.peers)/2 + 1
		for i := 0; voteGrantedCount < majority && i < len(rf.peers)-1; i++ {
			reply := <-ch
			if reply != nil {
				if reply.VoteGranted {
					voteGrantedCount++
					Debug(VoteEvent, rf.me, "vote reiceved granted reply, count = %d\n", voteGrantedCount)

				} else if reply.Term > rf.currentTerm {
					rf.mu.Lock()
					rf.currentTerm = reply.Term
					rf.role = FOLLOWER
					rf.votedFor = -1
					rf.mu.Unlock()
					Debug(VoteEvent, rf.me, "vote reiceved rejected reply\n")
					break
				}
			} else {
				Debug(VoteEvent, rf.me, "vote reiceved nil reply\n")
			}
		}

		rf.mu.Lock()
		if rf.role == CANDIDATE && voteGrantedCount >= majority {
			Debug(VoteEvent, rf.me, "Elected success with term %d\n", rf.currentTerm)

			rf.role = LEADER
			rf.nextIndex = make([]int, len(rf.peers))
			for i := 0; i < len(rf.peers); i++ {
				rf.nextIndex[i] = len(rf.log)
			}
			rf.matchIndex = make([]int, len(rf.peers))

			go rf.leaderHeartbeats()
		} else {
			Debug(VoteEvent, rf.me, "Elected failed with term %d\n", rf.currentTerm)
		}
		rf.mu.Unlock()
	} else {
		rf.mu.Unlock()
	}

}

func (rf *Raft) leaderHeartbeats() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.role != LEADER {
			rf.mu.Unlock()
			break
		}
		Debug(HeartbeatEvent, rf.me, "Leader send heart beats")
		rf.mu.Unlock()
		rf.leaderLogReplication()
		time.Sleep(100 * time.Millisecond)
		go rf.leaderCommit()

	}
}

func (rf *Raft) leaderLogReplication() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for peerId := 0; peerId < len(rf.peers); peerId++ {
		if peerId == rf.me {
			continue
		}

		args := AppendEntriesArgs{
			Term:              rf.currentTerm,
			LeaderId:          rf.me,
			PrevLogIndex:      rf.nextIndex[peerId] - 1,
			PrevLogTerm:       rf.log[rf.nextIndex[peerId]-1].Term,
			Entries:           rf.log[rf.nextIndex[peerId]:],
			LeaderCommitIndex: rf.commitIndex,
		}
		// args.Entries = append(args.Entries, rf.log[rf.nextIndex[peerId]:]...)
		reply := AppendEntriesReply{}
		go rf.sendAppendEntries(peerId, &args, &reply)

	}

}

func (rf *Raft) leaderCountReplicas() int {
	var matchIndex []int
	matchIndex = append(matchIndex, rf.matchIndex...)
	sort.Ints(matchIndex)
	return matchIndex[len(rf.matchIndex)/2]
}

func (rf *Raft) leaderCommit() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	n := rf.leaderCountReplicas()
	if rf.log[n].Term == rf.currentTerm && n > rf.commitIndex {
		rf.commitIndex = n
	}
	go rf.applyLogToStateMachine()
}

func (rf *Raft) applyLogToStateMachine() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.lastApplied < rf.commitIndex {
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i].Command,
				CommandIndex: i,
			}
			rf.applyCh <- msg
		}
		rf.lastApplied = rf.commitIndex
	}

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
	rf.log = make([]LogEntry, 1)
	rf.role = FOLLOWER
	// rf.leaderId = -1
	rf.votedFor = -1
	rf.lastHeartbeat = time.Now()
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.lastHeartbeat = time.Now()
	// start ticker goroutine to start elections
	go rf.ticker()
	// go rf.leaderLogReplication()

	return rf
}
