package raft

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.snapshotIndex {
		return
	}
	Debug(SnapEvent, rf.me, "Snapshot start index=%d\n", index)
	rf.snapshot = snapshot
	rf.snapshotTerm = rf.log.entry(index).Term
	rf.snapshotIndex = index
	// keep the log entry at snapshotIndex as the first log entry of the sliced log,
	// and the log's length always >= 1
	rf.log.cutOffHead(index)
	// log.Printf("S%d Snapshot rf.log = %+v\n", rf.me, rf.log)
	rf.persist()
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
	// rf.me, rf.snapshotIndex, rf.log.lastIndex(), args.LastIncludedIndex, rf.log)
	if rf.log.lastIndex() > args.LastIncludedIndex {
		// keep the log entry at snapshotIndex as the first log entry of the sliced log,
		// and the log's length always >= 1
		rf.log.cutOffHead(args.LastIncludedIndex)
	} else {
		// keep the log entry at snapshotIndex as the first log entry of the sliced log,
		// and the log's length always >= 1
		rf.log = Log{[]Entry{{Term: args.LastIncludedTerm}}, args.LastIncludedIndex}
	}
	// log.Printf("S%d InstallSnapshot after rf.log = %+v\n", rf.me, rf.log)
	rf.snapshotIndex = args.LastIncludedIndex
	rf.snapshotTerm = args.LastIncludedTerm
	rf.snapshot = args.Data
	rf.persist()
	rf.applyCond.Broadcast()
	// go rf.applySnapshot()

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
