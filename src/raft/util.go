package raft

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// https://blog.josejg.com/debugging-pretty/

type logTopic string

const (
	TopicHB              logTopic = "HeatsBeat  "
	TopicVR              logTopic = "VoteRequest"
	TopicTickerCandidate logTopic = "TKCandidate"
	TopicTickerFollower  logTopic = "TKFollower "
	TopicTickerLeader    logTopic = "TKLeader   "
	TopicStart           logTopic = "Start/Req  "
)

type logLevel int

const (
	trace logLevel = iota
	debug
	none
)

// Retrieve the verbosity level from an environment variable
func getVerbosity() logLevel {
	v := os.Getenv("RAFT_LOG")
	switch strings.ToLower(v) {
	case "trace":
		return trace
	case "debug":
		return debug
	case "none":
		return none
	default:
		return trace // change to none to stop log
	}
}

var debugStart time.Time
var debugVerbosity logLevel

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func TPrintf(topic logTopic, traceStr string, format string, a ...interface{}) {
	//if topic == TopicVR { // change this to filter other logs
	//	return
	//}
	if debugVerbosity == none {
		return
	}

	time := time.Since(debugStart).Milliseconds()
	timeMS := time % 1000
	time1000MS := time / 1000
	prefix := ""
	if debugVerbosity == trace {
		prefix = fmt.Sprintf("%03d|%03dMS [%v] |%v| - ", time1000MS, timeMS, topic, traceStr)
	} else if debugVerbosity == debug {
		prefix = fmt.Sprintf("%06d [%v] - ", time, topic)
	}

	format = prefix + format
	log.Printf(format, a...)
}
