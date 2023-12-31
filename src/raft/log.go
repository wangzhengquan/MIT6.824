package raft

type Entry struct {
	Term    int
	Command interface{}
}

type Log struct {
	entries []Entry // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
	index0  int
}

func makeLog(len int, start int) Log {
	return Log{make([]Entry, len), start}
}

func (l *Log) append(a ...Entry) {
	l.entries = append(l.entries, a...)
}

func (l *Log) place(index int, a ...Entry) {
	j := 0
	i := index - l.index0
	// overlap part
	Assert(index-l.index0 >= 0, "place index=%d, l.index0=%d\n", index, l.index0)
	for ; i < len(l.entries) && j < len(a); i, j = i+1, j+1 {
		l.entries[i] = a[j]
	}

	l.entries = append(l.entries, a[j:]...)
	i += len(a[j:])
	// The old leader have append alot of logs at this server's log at index_1 then step down,
	// new leader's append log whose PrevLogIndex match with this server's log at (index_1-1),
	// Therefore the new logs are also placed at index_1, but with a shorter length than the old leader has appended.
	// As a result, some of the old logs remains, which have a lower term than the log ahead of it, which is unacceptable.
	// Therefore this remains part must be cut off.
	if i < len(l.entries) && l.entries[i-1].Term > l.entries[i].Term {
		l.entries = l.entries[:i]
	}

}

func (l *Log) start() int {
	return l.index0
}

func (l *Log) entry(index int) *Entry {
	Assert(index-l.index0 >= 0, "entry index=%d, l.index0=%d\n", index, l.index0)
	return &l.entries[index-l.index0]
}

func (l *Log) lastIndex() int {
	return l.index0 + len(l.entries) - 1
}

func (l *Log) lastEntry() *Entry {
	return l.entry(l.lastIndex())
}

func (l *Log) length() int {
	return l.index0 + len(l.entries)
}

func (l *Log) slice(index int) []Entry {
	return l.entries[index-l.index0:]
}

func (l *Log) cutOffHead(index int) {
	l.entries = l.entries[index-l.index0:]
	l.index0 = index
}

func (l *Log) cutOffTail(index int) {
	if index == l.index0 {
		l.entries = l.entries[:1]
	} else {
		l.entries = l.entries[:index-l.index0]
	}

}
