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
	// overlap part
	for i := index - l.index0; i < len(l.entries) && j < len(a); i, j = i+1, j+1 {
		l.entries[i] = a[j]
	}
	l.entries = append(l.entries, a[j:]...)

}

func (l *Log) start() int {
	return l.index0
}

func (l *Log) entry(index int) *Entry {
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
	l.entries = l.entries[:index-l.index0]
}
