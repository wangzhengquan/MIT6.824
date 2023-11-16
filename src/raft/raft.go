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
	"log"
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

const ELECTION_TIMEOUT time.Duration = 300 * time.Millisecond

var HEARTBEAT_TIME_INTERVAL time.Duration = 10

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

func (args *RequestVoteArgs) String() string {
	return fmt.Sprintf("{Term: %v, CandidateId: %v, LastLogIndex: %v, LastLogTerm: %v }",
		args.Term, args.CandidateId, args.LastLogIndex, args.LastLogTerm)
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

func (args *AppendEntriesArgs) String() string {
	return fmt.Sprintf("{Term: %v, LeaderId: %v, PrevLogIndex: %v, PrevLogTerm: %v, Entries: %v, LeaderCommitIndex:%v }",
		args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), args.LeaderCommitIndex)
}

type AppendEntriesReply struct {
	Term          int // currentTerm, for leader to update itself
	ConflictTerm  int // term of the conflicting entry
	ConflictIndex int
	Success       bool // true if follower contained entry matching prevLogIndex and prevLogTerm
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
	currentTerm   int        // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor      int        // candidateId that received vote in current term (or null if none)
	log           []LogEntry // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
	snapshotTerm  int
	snapshotIndex int
	snapshot      []byte

	// Volatile state on all servers:
	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	commitCond  *sync.Cond

	// Volatile state on leaders:
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	role RoleT
	// leaderId int
	lastHeartbeat time.Time
	// commitCh      chan int
	applyCh chan ApplyMsg
}

type LogEntry struct {
	Command interface{}
	Term    int
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
	e.Encode(rf.log)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor, snapshotIndex, snapshotTerm int
	var log []LogEntry
	if err := d.Decode(&currentTerm); err != nil {
		panic(err)
	}
	if err := d.Decode(&votedFor); err != nil {
		panic(err)
	}
	if err := d.Decode(&log); err != nil {
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
	rf.log = log
	rf.snapshotIndex = snapshotIndex
	rf.snapshotTerm = snapshotTerm

}

func (rf *Raft) logIdxToArrayIdx(index int) int {
	arrayIndex := index - rf.snapshotIndex
	assert(arrayIndex >= 0, "index=%d,rf.snapshotIndex=%d, arrayIndex=%d\n", index, rf.snapshotIndex, arrayIndex)
	return arrayIndex
}

func (rf *Raft) ArrayIdxToLogIdx(index int) int {
	return index + rf.snapshotIndex
}

func (rf *Raft) getLogEntry(index int) *LogEntry {
	arrayIndex := rf.logIdxToArrayIdx(index)
	return &rf.log[arrayIndex]
}

func (rf *Raft) lastLogIndex() int {
	// log.Println("len(rf.log)", len(rf.log))
	return rf.ArrayIdxToLogIdx(len(rf.log)) - 1
}

func (rf *Raft) lastLogTerm() int {
	// return rf.getLogEntry(rf.lastLogIndex()).Term
	return rf.log[len(rf.log)-1].Term
}

/**
 * If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
 */
func (rf *Raft) stepDown(term int) {
	rf.currentTerm = term
	rf.role = FOLLOWER
	rf.votedFor = -1
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Debug(SnapEvent, rf.me, "Snapshot start index=%d\n", index)

	if index <= rf.snapshotIndex {
		return
	}
	rf.snapshot = snapshot
	arrayIndex := rf.logIdxToArrayIdx(index)
	rf.snapshotTerm = rf.log[arrayIndex].Term
	rf.snapshotIndex = index
	// keep the log entry at snapshotIndex as the first log entry of the sliced log,
	// and the log's length always >= 1
	log_tmp := rf.log[arrayIndex:]
	rf.log = make([]LogEntry, len(log_tmp))
	copy(rf.log, log_tmp)
	// log.Printf("S%d Snapshot rf.log = %+v\n", rf.me, rf.log)
	rf.persist()
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

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		Debug(VoteEvent, rf.me, "Voter denies vote to %d for candidate term %d < current term %d.\n",
			args.CandidateId, args.Term, rf.currentTerm)
		return
	}

	if args.Term > rf.currentTerm {
		rf.stepDown(args.Term)
	}

	// “up-to-date log” check
	// Raft determines which of two logs is more up-to-date by comparing the index and term of the last entries in the logs.
	// If the logs have last entries with different terms, then the log with the later term is more up-to-date.
	// If the logs end with the same term, then whichever log is longer is more up-to-date.
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if args.LastLogTerm > rf.lastLogTerm() ||
			(args.LastLogTerm == rf.lastLogTerm() && args.LastLogIndex >= rf.lastLogIndex()) {
			Debug(VoteEvent, rf.me, "Grant vote to %d.\n", args.CandidateId)
			rf.votedFor = args.CandidateId
			rf.resetElectionTimer()
			reply.VoteGranted = true
		} else {
			Debug(VoteEvent, rf.me, "Voter denies its vote for its own log is more up-to-date than that of the candidate S%d.\n",
				args.CandidateId)
		}
		rf.persist()
	} else {
		Debug(VoteEvent, rf.me, "Reject vote to %d for voter had voted for %d\n",
			args.CandidateId, rf.votedFor)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		Debug(HeartbeatEvent, rf.me, "reject AppendEntries from %d, for args.term %d < current term %d\n",
			args.LeaderId, args.Term, rf.currentTerm)
		reply.Success = false
		return
	}

	Debug(HeartbeatEvent, rf.me, "recieved AppendEntries from %d, args=%v\n",
		args.LeaderId, args)

	rf.resetElectionTimer()

	if args.Term > rf.currentTerm || rf.role == CANDIDATE {
		rf.stepDown(args.Term)
	}

	// “PrevLogIndex log matched” check

	if args.PrevLogIndex == 0 {
		rf.log = rf.log[:1]
		rf.log = append(rf.log, args.Entries...)
		Debug(HeartbeatEvent, rf.me, "append entries =%v\n", len(args.Entries))
		reply.Success = true
		// follower commit
		rf.followerCommit(args.LeaderCommitIndex)
	} else {
		if rf.lastLogIndex() < args.PrevLogIndex {
			Debug(HeartbeatEvent, rf.me, "could not append entries for rf.lastLogIndex <= args.PrevLogIndex\n")
			reply.ConflictTerm = rf.lastLogTerm()
			reply.ConflictIndex = rf.lastLogIndex()
			reply.Success = false
		} else if args.PrevLogIndex < rf.snapshotIndex {
			j := 1
			i := rf.snapshotIndex - args.PrevLogIndex
			log.Println("---args.PrevLogIndex < rf.snapshotIndex")
			// it should be "args.Entries[i-1] == rf.log[0]"
			assert(args.Entries[i-1].Term == rf.snapshotTerm, "args.Entries[i-1].Term=%d ,rf.snapshotTerm=%d", args.Entries[i-1].Term, rf.snapshotTerm)
			for ; i < len(args.Entries) && j < len(rf.log); i, j = i+1, j+1 {
				rf.log[j] = args.Entries[i]
			}
			rf.log = append(rf.log, args.Entries[i:]...)
			reply.Success = true
			Debug(HeartbeatEvent, rf.me, "append entries args.PrevLogIndex <= rf.snapshotIndex j=%d, append=%v\n", j, len(args.Entries[j:]))
			rf.followerCommit(args.LeaderCommitIndex)

		} else if rf.getLogEntry(args.PrevLogIndex).Term != args.PrevLogTerm {
			Debug(HeartbeatEvent, rf.me, "could not append entries for rf.log[args.PrevLogIndex].Term != args.PrevLogTerm\n")
			reply.ConflictTerm = rf.getLogEntry(args.PrevLogIndex).Term
			reply.ConflictIndex = args.PrevLogIndex

			assert(rf.logIdxToArrayIdx(args.PrevLogIndex) > 0,
				"assert rf.logIdxToArrayIdx(args.PrevLogIndex)>0, args.PrevLogIndex=%d, rf.snapshotIndex=%d, rf.log=%+v",
				args.PrevLogIndex, rf.snapshotIndex, rf.log)
			rf.log = rf.log[:rf.logIdxToArrayIdx(args.PrevLogIndex)]

			reply.Success = false
		} else {
			// follower's log matches the leader’s log up to and including the args.prevLogIndex
			j := 0
			// overlap part
			for i := rf.logIdxToArrayIdx(args.PrevLogIndex + 1); i < len(rf.log) && j < len(args.Entries); i, j = i+1, j+1 {
				rf.log[i] = args.Entries[j]
			}
			rf.log = append(rf.log, args.Entries[j:]...)
			Debug(HeartbeatEvent, rf.me, "append entries j=%d, append=%v\n", j, len(args.Entries[j:]))
			reply.Success = true
			// follower commit
			rf.followerCommit(args.LeaderCommitIndex)

		}
	}
}

func (rf *Raft) followerCommit(leaderCommitIndex int) {
	if leaderCommitIndex > rf.commitIndex {
		if leaderCommitIndex < rf.lastLogIndex() {
			rf.commitIndex = leaderCommitIndex
		} else {
			rf.commitIndex = rf.lastLogIndex()
		}
		rf.persistSate()
		rf.commitCond.Broadcast()
	}
	Debug(CommitEvent, rf.me, "follower commit args.LeaderCommitIndex=%d rf.commitIndex=%d\n",
		leaderCommitIndex, rf.commitIndex)
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		Debug(SnapEvent, rf.me, "reject InstallSnapshot from %d, for args.term %d < current term %d\n",
			args.LeaderId, args.Term, rf.currentTerm)
		return
	}

	Debug(SnapEvent, rf.me, "Recieved InstallSnapshot from %d, args=%v\n",
		args.LeaderId, args)

	rf.resetElectionTimer()

	if args.Term > rf.currentTerm || rf.role == CANDIDATE {
		rf.stepDown(args.Term)
	}

	if rf.snapshotIndex >= args.LastIncludedIndex || rf.lastApplied >= args.LastIncludedIndex {
		return
	}
	// log.Printf("S%d InstallSnapshot before , rf.snapshotIndex=%d, rf.lastLogIndex=%d,  args.LastIncludedIndex=%d, rf.log = %+v\n",
	// rf.me, rf.snapshotIndex, rf.lastLogIndex(), args.LastIncludedIndex, rf.log)
	if rf.lastLogIndex() > args.LastIncludedIndex {
		// keep the log entry at snapshotIndex as the first log entry of the sliced log,
		// and the log's length always >= 1
		log_tmp := rf.log[rf.logIdxToArrayIdx(args.LastIncludedIndex):]
		rf.log = make([]LogEntry, len(log_tmp))
		copy(rf.log, log_tmp)
	} else {
		// keep the log entry at snapshotIndex as the first log entry of the sliced log,
		// and the log's length always >= 1
		rf.log = []LogEntry{{Term: args.LastIncludedTerm}}
	}
	// log.Printf("S%d InstallSnapshot after rf.log = %+v\n", rf.me, rf.log)
	rf.snapshotIndex = args.LastIncludedIndex
	rf.snapshotTerm = args.LastIncludedTerm
	rf.snapshot = args.Data
	rf.persist()
	go rf.applySnapshot()

}

func (rf *Raft) applySnapshot() {
	rf.mu.Lock()
	lastApplied := rf.lastApplied
	snapshotIndex := rf.snapshotIndex
	Debug(SnapEvent, rf.me, "applySnapshot rf.lastApplied=%d, rf.snapshotIndex=%d\n", rf.lastApplied, rf.snapshotIndex)
	if lastApplied < snapshotIndex {
		Debug(SnapEvent, rf.me, "applySnapshot before\n")
		msg := ApplyMsg{
			CommandValid:  false,
			SnapshotValid: true,
			Snapshot:      rf.snapshot,
			SnapshotTerm:  rf.snapshotTerm,
			SnapshotIndex: rf.snapshotIndex,
		}
		rf.mu.Unlock()

		rf.applyCh <- msg
		rf.mu.Lock()
		if snapshotIndex >= rf.snapshotIndex {
			rf.commitIndex = snapshotIndex
			rf.lastApplied = rf.commitIndex
		}
		rf.mu.Unlock()

		Debug(SnapEvent, rf.me, "applySnapshot after\n")
	} else {
		rf.mu.Unlock()
	}

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
	if ok := rf.peers[server].Call("Raft.AppendEntries", args, reply); ok {
		rf.mu.Lock()
		Debug(HeartbeatEvent, rf.me, "hearbeat, reply  from %d : reply=%+v.\n",
			server, reply)
		if reply.Success {
			if args.PrevLogIndex == rf.nextIndex[server]-1 { // check rf.nextIndex[server] has not been modify by other replys
				rf.nextIndex[server] += len(args.Entries)
				rf.matchIndex[server] = rf.nextIndex[server] - 1
			}
		} else {
			if reply.Term > rf.currentTerm {
				rf.stepDown(reply.Term)
			} else {
				if args.PrevLogIndex == rf.nextIndex[server]-1 {
					if reply.ConflictIndex < rf.snapshotIndex {
						rf.nextIndex[server] = reply.ConflictIndex + 1
					} else if rf.getLogEntry(reply.ConflictIndex).Term == reply.ConflictTerm {
						// this situation could occur when "length of follower's log" <= args.prevLogIndex
						rf.nextIndex[server] = reply.ConflictIndex + 1
					} else {
						// step over all the log with term of rf.log[reply.ConflictIndex]
						i := rf.logIdxToArrayIdx(reply.ConflictIndex)
						term := rf.log[i].Term
						for ; rf.log[i].Term == term && i > 0; i-- {
						}
						rf.nextIndex[server] = rf.ArrayIdxToLogIdx(i + 1)
					}
				}
				Debug(HeartbeatEvent, rf.me, "hearbeat, reset rf.nextIndex[%d]=%d.\n",
					server, rf.nextIndex[server])
			}
		}
		rf.mu.Unlock()
	} else {
		Debug(HeartbeatEvent, rf.me, "hearbeat, no reply from %d. \n", server)
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply); ok {
		Debug(SnapEvent, rf.me, "sendInstallSnapshot : reply from %d, reply.Term=%d, rf.currentTerm=%d", server, reply.Term, rf.currentTerm)
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.stepDown(reply.Term)
		} else {
			rf.nextIndex[server] = args.LastIncludedIndex + 1
			rf.matchIndex[server] = args.LastIncludedIndex
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
	isLeader = rf.role == LEADER
	if !isLeader {
		return
	}

	index = rf.ArrayIdxToLogIdx(len(rf.log))
	term = rf.currentTerm
	logEntry := LogEntry{
		Command: command,
		Term:    rf.currentTerm,
	}
	rf.log = append(rf.log, logEntry)
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
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.commitCond.Broadcast()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// pause for a random amount of time between 50 and 350 milliseconds.
		time.Sleep(time.Duration(50+rand.Int63n(300)) * time.Millisecond)
		// leaderElection run in a seperate goroutine so that another election preocess can start when this election process timeout with out a result
		go rf.leaderElection()
	}
	// log.Printf("S%d ticker finished\n", rf.me)
}

func (rf *Raft) leaderElection() {
	rf.mu.Lock()

	if rf.role == FOLLOWER && time.Since(rf.lastHeartbeat) >= ELECTION_TIMEOUT {
		rf.role = CANDIDATE
	}

	if rf.role == CANDIDATE && time.Since(rf.lastHeartbeat) >= ELECTION_TIMEOUT {
		Debug(VoteEvent, rf.me, "Leader election start\n")
		rf.currentTerm++
		rf.votedFor = rf.me
		rf.persist()
		rf.resetElectionTimer()

		ch := make(chan *RequestVoteReply, len(rf.peers)/2)

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

		// count votes
		voteGrantedCount := 1
		majority := len(rf.peers)/2 + 1
		for i := 0; voteGrantedCount < majority && i < len(rf.peers)-1; i++ {
			reply := <-ch
			if reply != nil {
				if reply.VoteGranted {
					voteGrantedCount++
					Debug(VoteEvent, rf.me, "vote reiceved granted reply, count = %d\n", voteGrantedCount)
				} else {
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						rf.stepDown(reply.Term)
						Debug(VoteEvent, rf.me, "vote reiceved rejected reply\n")
						rf.mu.Unlock()
						break
					}
					rf.mu.Unlock()
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
				rf.nextIndex[i] = rf.lastLogIndex() + 1
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
		// rf.mu.Lock()
		if rf.role != LEADER {
			// rf.mu.Unlock()
			break
		}
		// rf.mu.Unlock()
		rf.leaderLogReplication()
		time.Sleep(HEARTBEAT_TIME_INTERVAL * time.Millisecond)
		go rf.leaderCommit()

	}
	// log.Printf("S%d leaderHeartbeats finished\n", rf.me)
}

func (rf *Raft) leaderLogReplication() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != LEADER {
		return
	}
	Debug(HeartbeatEvent, rf.me, "Leader send heart beats")
	idle := (rf.commitIndex == rf.lastLogIndex())

	for peerId := 0; peerId < len(rf.peers); peerId++ {
		if peerId == rf.me {
			continue
		}
		prevLogIndex := rf.nextIndex[peerId] - 1
		if prevLogIndex < rf.snapshotIndex {
			args := InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.snapshotIndex,
				LastIncludedTerm:  rf.snapshotTerm,
				Data:              rf.snapshot,
			}
			reply := InstallSnapshotReply{}
			go rf.sendInstallSnapshot(peerId, &args, &reply)

			Debug(SnapEvent, rf.me, "Leader send InstallSnapshot to S%d with prevLogIndex=%d args=%v\n", peerId, prevLogIndex, &args)
		} else {
			args := AppendEntriesArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				PrevLogIndex:      prevLogIndex,
				PrevLogTerm:       rf.getLogEntry(prevLogIndex).Term,
				Entries:           rf.log[rf.logIdxToArrayIdx(prevLogIndex+1):],
				LeaderCommitIndex: rf.commitIndex,
			}

			reply := AppendEntriesReply{}
			go rf.sendAppendEntries(peerId, &args, &reply)
			Debug(HeartbeatEvent, rf.me, "Leader send AppendEntries to S%d with rf.snapshotIndex=%d, args=%v\n", peerId, rf.snapshotIndex, &args)
		}

		idle = idle && (prevLogIndex == rf.lastLogIndex())
	}

	// tune heartbeat interval base on if having more logs to send
	if idle {
		HEARTBEAT_TIME_INTERVAL = 100
	} else {
		HEARTBEAT_TIME_INTERVAL = 10
	}

}

func (rf *Raft) leaderCountReplicas() int {
	var matchIndex []int
	matchIndex = append(matchIndex, rf.matchIndex...)
	matchIndex[rf.me] = rf.lastLogIndex()
	sort.Ints(matchIndex)
	return matchIndex[len(rf.matchIndex)/2]
}

func (rf *Raft) leaderCommit() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != LEADER {
		return
	}
	commitIndex := rf.leaderCountReplicas()
	if commitIndex <= rf.snapshotIndex {
		return
	}
	Debug(CommitEvent, rf.me, "leader commit check commitIndex = %d\n", commitIndex)
	// Raft never commits log entries from previous terms by count- ing replicas. Only log entries from the leader’s current term are committed by counting replicas;
	if rf.getLogEntry(commitIndex).Term == rf.currentTerm && commitIndex > rf.commitIndex {
		rf.commitIndex = commitIndex
		rf.persistSate()
		rf.commitCond.Broadcast()
	}
}

// func (rf *Raft) applyCommitToStateMachine() {
// 	for rf.killed() == false {
// 		commitIndex := <-rf.commitCh
// 		for rf.lastApplied < commitIndex {
// 			Debug(CommitEvent, rf.me, "IsLeader=%v Apply commit %d-%d\n", rf.role == LEADER, rf.lastApplied, rf.commitIndex)
// 			commandIndex := rf.lastApplied + 1
// 			msg := ApplyMsg{
// 				CommandValid: true,
// 				Command:      rf.log[commandIndex].Command,
// 				CommandIndex: commandIndex,
// 			}
// 			rf.applyCh <- msg
// 			rf.lastApplied = commandIndex
// 		}

// 	}
// }

func (rf *Raft) applyCommitToStateMachine() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.lastApplied >= rf.commitIndex {
			rf.commitCond.Wait()
			rf.mu.Unlock()
		} else {
			Debug(CommitEvent, rf.me, "IsLeader=%v Apply commit %d-%d\n", rf.role == LEADER, rf.lastApplied, rf.commitIndex)
			commandIndex := rf.lastApplied + 1
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.getLogEntry(commandIndex).Command,
				CommandIndex: commandIndex,
			}
			rf.mu.Unlock()
			rf.applyCh <- msg

			rf.mu.Lock()
			if commandIndex >= rf.lastApplied+1 {
				rf.lastApplied = commandIndex
			}
			rf.mu.Unlock()
		}

	}
	// log.Printf("S%d applyCommitToStateMachine finished\n", rf.me)

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
	rf.applyCh = applyCh
	rf.commitCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()

	rf.resetElectionTimer()
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyCommitToStateMachine()
	go rf.applySnapshot()
	return rf
}
