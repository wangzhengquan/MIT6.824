package kvraft

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

type LogEventT string

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

func Assert(expect bool, format string, a ...interface{}) {
	if !expect {
		panic(fmt.Sprintf(format, a...))
	}
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if verbosity >= 1 {
		log.Printf(format, a...)
	}
	return
}
