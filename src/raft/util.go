package raft

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

type LogEventT string

const (
	// dClient  LogEventT = "CLNT"
	// dLeader  LogEventT = "LEAD"
	// LogEventT     LogEventT = "LOG1"
	// Log2Event    LogEventT = "LOG2"

	ErrorEvent LogEventT = "ERRO"
	InfoEvent  LogEventT = "INFO"
	WarnEvent  LogEventT = "WARN"

	TestEvent  LogEventT = "TEST"
	TraceEvent LogEventT = "TRCE"

	VoteEvent      LogEventT = "VOTE"
	HeartbeatEvent LogEventT = "HEARTBEAT"
	CommitEvent    LogEventT = "CMIT"
	SnapEvent      LogEventT = "SNAP"

	DropEvent    LogEventT = "DROP"
	PersistEvent LogEventT = "PERS"
	TermEvent    LogEventT = "TERM"
	TimerEvent   LogEventT = "TIMR"
)

// Retrieve the verbosity level from an environment variable
func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

var debugStart time.Time
var verbosity int

func DebugInit() {
	// debugVerbosity = 1
	verbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func Debug(what LogEventT, who int, format string, a ...interface{}) {
	if verbosity >= 1 {
		time := time.Since(debugStart).Microseconds()
		// time /= 100
		var prefix string
		if who >= 0 {
			prefix = fmt.Sprintf("%06d %v S%d ", time, string(what), who)
		} else {
			prefix = fmt.Sprintf("%06d %v ", time, string(what))
		}

		format = prefix + format
		log.Printf(format, a...)
	}
}

func Assert(expect bool, format string, a ...interface{}) {
	if !expect {
		panic(fmt.Sprintf(format, a...))
	}
}
