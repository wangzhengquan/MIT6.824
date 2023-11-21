package raft

import "sort"

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.LeaderId = args.LeaderId // for debug

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
		oldLogLen := rf.log.length()
		rf.log.place(args.PrevLogIndex+1, args.Entries...)
		Debug(HeartbeatEvent, rf.me, "approve AppendEntries request from %d, old log=%d, new log=%d\n",
			args.LeaderId, oldLogLen, rf.log.length())
		rf.persistSate()
		rf.followerCommit(args.LeaderCommitIndex)
		reply.Success = true
	} else {
		if args.PrevLogIndex > rf.log.lastIndex() {
			Debug(HeartbeatEvent, rf.me, "refuse AppendEntries request from %d for args.PrevLogIndex %d > rf.lastLogIndex %d \n",
				args.LeaderId, args.PrevLogIndex, rf.log.lastIndex())
			reply.ConflictTerm = rf.log.lastEntry().Term
			reply.ConflictIndex = rf.log.lastIndex()
			reply.Success = false
		} else if args.PrevLogIndex < rf.snapshotIndex {
			i := rf.snapshotIndex - args.PrevLogIndex - 1

			// log.Printf("---args.PrevLogIndex %d < rf.snapshotIndex %d, i=%d, len(args.Entries)=%d\n", args.PrevLogIndex, rf.snapshotIndex, i, len(args.Entries))
			if len(args.Entries) > i {
				Assert(args.Entries[i].Term == rf.snapshotTerm, "args.Entries[i].Term=%d ,rf.snapshotTerm=%d", args.Entries[i].Term, rf.snapshotTerm)
				rf.log.place(rf.snapshotIndex, args.Entries[i:]...)
				rf.persistSate()
				Debug(HeartbeatEvent, rf.me, "approve append entries, i=%d, len(args.Entries)=%d\n", i, len(args.Entries))
				rf.followerCommit(args.LeaderCommitIndex)
			}
			// j := 1
			// i := rf.snapshotIndex - args.PrevLogIndex
			// if len(args.Entries) > i {
			// 	// it should be "args.Entries[i-1] == rf.log[0]"
			// 	Assert(args.Entries[i-1].Term == rf.snapshotTerm, "args.Entries[i-1].Term=%d ,rf.snapshotTerm=%d", args.Entries[i-1].Term, rf.snapshotTerm)
			// 	for ; i < len(args.Entries) && j < len(rf.log.entries); i, j = i+1, j+1 {
			// 		rf.log.entries[j] = args.Entries[i]
			// 	}
			// 	rf.log.entries = append(rf.log.entries, args.Entries[i:]...)
			// 	rf.persistSate()
			// }

			reply.Success = true

		} else if rf.log.entry(args.PrevLogIndex).Term != args.PrevLogTerm {
			reply.ConflictTerm = rf.log.entry(args.PrevLogIndex).Term
			reply.ConflictIndex = args.PrevLogIndex
			Debug(HeartbeatEvent, rf.me, "refuse AppendEntries request from %d for rf.log[PrevLogIndex %d].Term %d != args.PrevLogTerm %d\n",
				args.LeaderId, args.PrevLogIndex, reply.ConflictTerm, args.PrevLogTerm)
			rf.log.cutOffTail(args.PrevLogIndex)
			rf.persistSate()
			reply.Success = false
		} else {
			// follower's log matches the leader’s log up to and including the args.prevLogIndex
			oldLogLen := rf.log.length()
			rf.log.place(args.PrevLogIndex+1, args.Entries...)

			Debug(HeartbeatEvent, rf.me, "approve AppendEntries request from %d, old log=%d, new log=%d\n",
				args.LeaderId, oldLogLen, rf.log.length())
			rf.persistSate()
			// follower commit
			rf.followerCommit(args.LeaderCommitIndex)
			reply.Success = true
		}
	}
}

func (rf *Raft) followerCommit(leaderCommitIndex int) {
	if leaderCommitIndex > rf.commitIndex {
		var commitIndex int
		if leaderCommitIndex < rf.log.lastIndex() {
			commitIndex = leaderCommitIndex
		} else {
			commitIndex = rf.log.lastIndex()
		}
		Debug(CommitEvent, rf.me, "follower commit, old commitIndex=%d, new commitIndex=%d\n",
			rf.commitIndex, commitIndex)
		rf.commitIndex = commitIndex
		rf.applyCond.Broadcast()
	}
}

// =============leader================

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if ok := rf.peers[server].Call("Raft.AppendEntries", args, reply); ok {
		rf.mu.Lock()
		if rf.role != LEADER || args.Term != rf.currentTerm {
			rf.mu.Unlock()
			return
		}
		Debug(HeartbeatEvent, rf.me, "hearbeat reply from %d : reply=%+v, request args=%v, rf.currentTerm=%d \n",
			server, reply, args, rf.currentTerm)

		if reply.Success {
			oldNextIndex := rf.nextIndex[server]
			matchIndex := args.PrevLogIndex + len(args.Entries)
			if rf.matchIndex[server] < matchIndex {
				rf.matchIndex[server] = matchIndex
			}
			rf.nextIndex[server] = rf.matchIndex[server] + 1
			Debug(HeartbeatEvent, rf.me, "rf.nextIndex[%d] advance, old=%d, new=%d\n",
				server, oldNextIndex, rf.nextIndex[server])

		} else {
			if reply.Term > rf.currentTerm {
				rf.stepDown(reply.Term)
			} else {
				// Assert(reply.LeaderId == rf.me, "reply.LeaderId=%d, rf.me=%d\n", reply.LeaderId, rf.me)
				oldNextIndex := rf.nextIndex[server]
				var newNextIndex int
				if reply.ConflictIndex < rf.snapshotIndex {
					newNextIndex = reply.ConflictIndex + 1
				} else if rf.log.entry(reply.ConflictIndex).Term == reply.ConflictTerm {
					// this situation could occur when "length of follower's log" <= args.prevLogIndex
					newNextIndex = reply.ConflictIndex + 1
				} else {
					// step over all the log with term of rf.log[reply.ConflictIndex]
					term := rf.log.entry(reply.ConflictIndex).Term
					for newNextIndex = reply.ConflictIndex; rf.log.entry(newNextIndex).Term == term && newNextIndex > rf.log.start(); newNextIndex-- {
					}
					newNextIndex += 1
				}

				if rf.matchIndex[server] < newNextIndex {
					rf.nextIndex[server] = newNextIndex
				} else {
					rf.nextIndex[server] = rf.matchIndex[server] + 1
				}

				Debug(HeartbeatEvent, rf.me, "hearbeat reply, rf.nextIndex[%d] go back , old=%d, new=%d.\n",
					server, oldNextIndex, rf.nextIndex[server])
				Assert(rf.matchIndex[server] < rf.nextIndex[server],
					"sendAppendEntries reply,rf.matchIndex[%d]=%d, rf.nextIndex=%d\n",
					server, rf.matchIndex[server], rf.nextIndex[server])
			}
		}

		rf.mu.Unlock()
	} else {
		Debug(HeartbeatEvent, rf.me, "hearbeat, no reply from %d. \n", server)
	}
}

func (rf *Raft) leaderLogReplication() {
	// if rf.role != LEADER {
	// 	return
	// }
	Debug(HeartbeatEvent, rf.me, "Leader send heart beats")
	idle := (rf.commitIndex == rf.log.lastIndex())

	for peerId := 0; peerId < len(rf.peers); peerId++ {
		if peerId == rf.me {
			continue
		}
		prevLogIndex := rf.nextIndex[peerId] - 1
		Assert(rf.matchIndex[peerId] <= prevLogIndex,
			"leaderLogReplication, rf.matchIndex[%d]=%d, prevLogIndex=%d",
			peerId, rf.matchIndex[peerId], prevLogIndex)
		if prevLogIndex < rf.snapshotIndex {
			//sendInstallSnapshot
			args := InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.snapshotIndex,
				LastIncludedTerm:  rf.snapshotTerm,
				Data:              rf.snapshot,
			}
			reply := InstallSnapshotReply{}
			go rf.sendInstallSnapshot(peerId, &args, &reply)

			Debug(SnapEvent, rf.me, "Leader send InstallSnapshot to S%d with prevLogIndex=%d, matchIndex=%d, args=%v\n\n",
				peerId, prevLogIndex, rf.matchIndex[peerId], &args)
		} else {
			//sendAppendEntries
			args := AppendEntriesArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				PrevLogIndex:      prevLogIndex,
				PrevLogTerm:       rf.log.entry(prevLogIndex).Term,
				Entries:           rf.log.slice(prevLogIndex + 1),
				LeaderCommitIndex: rf.commitIndex,
			}

			reply := AppendEntriesReply{}
			go rf.sendAppendEntries(peerId, &args, &reply)
			Debug(HeartbeatEvent, rf.me, "Leader send AppendEntries to S%d with rf.snapshotIndex=%d, matchIndex=%d, args=%v\n\n",
				peerId, rf.snapshotIndex, rf.matchIndex[peerId], &args)
		}

		idle = idle && (prevLogIndex == rf.log.lastIndex())
	}

	// tune heartbeat interval base on if having more logs to send
	if idle {
		HEARTBEAT_TIME_INTERVAL = 100
	} else {
		HEARTBEAT_TIME_INTERVAL = 100
	}

}

func (rf *Raft) leaderCountReplicas() int {
	var matchIndex []int
	matchIndex = append(matchIndex, rf.matchIndex...)
	matchIndex[rf.me] = rf.log.lastIndex()
	sort.Ints(matchIndex)
	return matchIndex[len(rf.matchIndex)/2]
}

func (rf *Raft) leaderCommit() {
	if rf.role != LEADER {
		return
	}
	commitIndex := rf.leaderCountReplicas()
	if commitIndex <= rf.snapshotIndex {
		return
	}
	// Raft never commits log entries from previous terms by count- ing replicas. Only log entries from the leader’s current term are committed by counting replicas;
	if rf.log.entry(commitIndex).Term == rf.currentTerm && commitIndex > rf.commitIndex {
		Debug(CommitEvent, rf.me, "leader commit, old commitIndex = %d, new commitIndex = %d\n", rf.commitIndex, commitIndex)
		rf.commitIndex = commitIndex
		rf.applyCond.Broadcast()
	}
}
