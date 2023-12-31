package shardkv

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

const (
	OFF   = iota
	FATAL = iota
	ERROR = iota
	WARN  = iota
	TEST  = iota
	DEBUG = iota
	INFO  = iota
	ALL   = iota
)

var LEVEL_NAME = []string{"OFF", "FATAL", "ERROR", "WARN", "TEST", "DEBUG", "INFO", "ALL"}

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

func debugInit() {
	// debugVerbosity = 1
	verbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

// func DPrintf(level int, format string, a ...interface{}) {
// 	if verbosity >= level {
// 		time := time.Since(debugStart).Microseconds()
// 		var prefix string
// 		prefix = fmt.Sprintf("%06d %v ", time, LEVEL_NAME[level])

// 		format = prefix + format
// 		log.Printf(format, a...)
// 	}
// }

func Assert(expect bool, format string, a ...interface{}) {
	if !expect {
		panic(fmt.Sprintf(format, a...))
	}
}

type LogEventT string

const (
	ErrorEvent LogEventT = "ERRO"
	WarnEvent  LogEventT = "WARN"
	InfoEvent  LogEventT = "INFO"
	TestEvent  LogEventT = "TEST"
	TraceEvent LogEventT = "TRCE"

	VoteEvent      LogEventT = "VOTE"
	HeartbeatEvent LogEventT = "HEARTBEAT"
	CommitEvent    LogEventT = "CMIT"
	SnapEvent      LogEventT = "SNAP"

	ConfigEvent    LogEventT = "CONFIG"
	PutEvent       LogEventT = "PUT"
	GetEvent       LogEventT = "GET"
	MigrationEvent LogEventT = "MIGRATION"
)

func Debug(what LogEventT, format string, a ...interface{}) {
	if verbosity >= 1 {
		time := time.Since(debugStart).Milliseconds()
		// time /= 100
		var prefix string
		prefix = fmt.Sprintf("%06d %v ", time, string(what))

		format = prefix + format
		log.Printf(format, a...)
	}
}
