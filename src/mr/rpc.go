package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type EmptyAges struct {
}

// Add your RPC definitions here.

type NumsTaskReplay struct {
	NMap     int
	NReduce  int
	FileName []string
}

type AskTaskReply struct {
	Valid    bool
	TaskType int
	TaskNum  int
}
type BoolReply struct {
	Boolre bool
}
type TaskNumAges struct {
	TaskT   int
	TaskNum int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
