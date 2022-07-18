package raft

import (
	"log"
	"os"
	"time"
)

// Debugging
const Debug = true

func init() {
	file := "lab2.log"
	logFile, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE, 0766)
	if err != nil {
		panic(err)
	}
	log.SetOutput(logFile)
	log.SetFlags(log.Lmicroseconds)
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// All have accquired the rf.mu.Lock()
func (rf *Raft) IsRpcExpired(state, term int) bool {
	return rf.state != state || rf.currnetTerm != term
}

func (rf *Raft) becomeFollower(term int) {
	rf.state = Follower
	rf.currnetTerm = term
}

func (rf *Raft) becomeCandidate() {
	rf.state = Candidate
	rf.currnetTerm++
	rf.votedFor = rf.me
	rf.heartbeatTime = time.Now()
}

func (rf *Raft) becomeLeader() {
	rf.state = Leader
	go rf.leaderSendAppendEntries()
}
