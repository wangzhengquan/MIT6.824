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
