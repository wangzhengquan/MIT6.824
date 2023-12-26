package raft

import (
	"math/rand"
	"time"
)

const ELECTION_TIMEOUT time.Duration = 300 * time.Millisecond

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

	Debug(VoteEvent, rf.me, "received vote request, args=%+v.\n", args)

	if args.Term > rf.currentTerm {
		rf.stepDown(args.Term)
	}

	// “up-to-date log” check
	// Raft determines which of two logs is more up-to-date by comparing the index and term of the last entries in the logs.
	// If the logs have last entries with different terms, then the log with the later term is more up-to-date.
	// If the logs end with the same term, then whichever log is longer is more up-to-date.
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		upToDate := args.LastLogTerm > rf.log.lastEntry().Term ||
			(args.LastLogTerm == rf.log.lastEntry().Term && args.LastLogIndex >= rf.log.lastIndex())
		if upToDate {
			Debug(VoteEvent, rf.me, "Grant vote to %d.\n", args.CandidateId)
			rf.votedFor = args.CandidateId
			rf.resetElectionTimer()
			rf.persistSate()
			reply.VoteGranted = true
		} else {
			Debug(VoteEvent, rf.me, "Voter denies its vote for its own log is more up-to-date than that of the candidate S%d.\n",
				args.CandidateId)
		}

	} else {
		Debug(VoteEvent, rf.me, "Reject vote to %d for voter had voted for %d\n",
			args.CandidateId, rf.votedFor)
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, ch chan *RequestVoteReply) {
	reply := RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", args, &reply)
	if ok {
		ch <- &reply
	} else {
		ch <- nil
	}
}

func (rf *Raft) leaderElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role == FOLLOWER && time.Since(rf.lastHeartbeat) >= ELECTION_TIMEOUT {
		rf.role = CANDIDATE
	}

	if rf.role == CANDIDATE && time.Since(rf.lastHeartbeat) >= ELECTION_TIMEOUT {
		Debug(VoteEvent, rf.me, "Leader election start\n")
		rf.currentTerm++
		rf.votedFor = rf.me
		rf.persistSate()
		rf.resetElectionTimer()

		// set chan size len(rf.peers) so that when func that read from chan finished the function that write to chan can return and release the resource without been blocked
		replyCh := make(chan *RequestVoteReply, len(rf.peers))
		args := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: rf.log.lastIndex(),
			LastLogTerm:  rf.log.lastEntry().Term,
		}
		for peerId := 0; peerId < len(rf.peers); peerId++ {
			if peerId == rf.me {
				continue
			}
			Debug(VoteEvent, rf.me, "Send request vote to %d with term: %d\n", peerId, rf.currentTerm)
			go rf.sendRequestVote(peerId, &args, replyCh)
		}
		rf.mu.Unlock()

		// count votes
		majority := len(rf.peers)/2 + 1
		votesCount := rf.countVotes(replyCh)

		rf.mu.Lock()
		// a trick bug in TestFigure8Unreliable2C since I didn't check rf.currentTerm before then
		// whiout this check it may be elected success with and old term, but now run as a leader with new term
		if rf.currentTerm == args.Term && rf.role == CANDIDATE && votesCount >= majority {
			rf.becomeLeader()
		} else {
			Debug(VoteEvent, rf.me, "Elected failed with term %d\n", args.Term)
		}
	}
}

func (rf *Raft) countVotes(replyCh <-chan *RequestVoteReply) int {
	count := 1
	majority := len(rf.peers)/2 + 1
	for i := 0; count < majority && i < len(rf.peers)-1; i++ {
		reply := <-replyCh
		if reply != nil {
			if reply.VoteGranted {
				count++
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
	return count
}

func (rf *Raft) becomeLeader() {
	Debug(VoteEvent, rf.me, "Elected success with term %d\n", rf.currentTerm)
	rf.leaderId = rf.me
	rf.role = LEADER
	rf.nextIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.log.lastIndex() + 1
	}
	rf.matchIndex = make([]int, len(rf.peers))

	go rf.leaderHeartbeats()
}

func (rf *Raft) ticker() {
	for rf.Killed() == false {
		// pause for a random amount of time between 50 and 350 milliseconds.
		time.Sleep(ELECTION_TIMEOUT + time.Duration(rand.Int63n(300))*time.Millisecond)
		// leaderElection run in a seperate goroutine so that another election preocess can start when this election process was timeout with out a result
		go rf.leaderElection()
	}
}
