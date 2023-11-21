package raft

func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()
		snapshotIndex := rf.snapshotIndex
		if snapshotIndex > rf.lastApplied {
			// applySnapshot
			Debug(SnapEvent, rf.me, "applySnapshot %d before\n", snapshotIndex)
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
			Debug(SnapEvent, rf.me, "applySnapshot after. snapshotIndex=%d, lastApplied old=%d\n", snapshotIndex, rf.lastApplied)
			if snapshotIndex > rf.lastApplied {
				rf.lastApplied = snapshotIndex
				if snapshotIndex > rf.commitIndex {
					rf.commitIndex = snapshotIndex
				}

			}
			rf.mu.Unlock()
		} else if rf.lastApplied < rf.commitIndex {
			// apply commit
			Debug(CommitEvent, rf.me, "IsLeader=%v Apply commit %d-%d\n", rf.role == LEADER, rf.lastApplied, rf.commitIndex)
			commandIndex := rf.lastApplied + 1
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log.entry(commandIndex).Command,
				CommandIndex: commandIndex,
			}
			rf.mu.Unlock()
			rf.applyCh <- msg

			rf.mu.Lock()
			if commandIndex >= rf.lastApplied+1 {
				rf.lastApplied = commandIndex
			}
			rf.mu.Unlock()
		} else {
			rf.applyCond.Wait()
			rf.mu.Unlock()
		}
	}
}
