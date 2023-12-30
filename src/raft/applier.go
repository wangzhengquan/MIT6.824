package raft

import (
	"fmt"
)

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

func (rf *Raft) applier() {
	for rf.Killed() == false {
		rf.mu.Lock()
		snapshotIndex := rf.snapshotIndex
		if snapshotIndex > rf.lastApplied {
			// applySnapshot
			Debug(SnapEvent, rf.me, "apply snapshot %d before\n", snapshotIndex)
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
			Debug(SnapEvent, rf.me, "apply snapshot after. snapshotIndex=%d, lastApplied old=%d\n", snapshotIndex, rf.lastApplied)
			if snapshotIndex > rf.lastApplied {
				rf.lastApplied = snapshotIndex
				if snapshotIndex > rf.commitIndex {
					rf.commitIndex = snapshotIndex
				}

			}
			rf.mu.Unlock()
		} else if rf.lastApplied < rf.commitIndex {
			// apply commit

			Debug(CommitEvent, rf.me, "apply %d-%d,  IsLeader=%v\n",
				rf.lastApplied, rf.commitIndex, rf.role == LEADER)
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
