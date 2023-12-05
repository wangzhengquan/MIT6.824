package raft

import "fmt"

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

	LeaderId int
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
				LeaderId:      rf.leaderId,
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
				LeaderId:     rf.leaderId,
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
