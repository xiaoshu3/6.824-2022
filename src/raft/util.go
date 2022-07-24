package raft

import (
	"log"
	"time"
)

// Debugging
const Debug = false

func max(a, b int) int {
	if a < b {
		return b
	}
	return a
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func init() {
	// file := "lab2.log"
	// logFile, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE, 0766)
	// if err != nil {
	// 	panic(err)
	// }
	// log.SetOutput(logFile)
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
	rf.heartbeatTime = time.Now()
}

func (rf *Raft) becomeCandidate() {
	rf.state = Candidate
	rf.currnetTerm++
	rf.votedFor = rf.me
	rf.heartbeatTime = time.Now()
}

func (rf *Raft) becomeLeader() {
	rf.state = Leader
	go rf.leaderSendAppendEntries(rf.currnetTerm)

	lenLog := len(rf.log)
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = lenLog
		rf.matchIndex[i] = 0
	}
}

func (rf *Raft) apply(applyCh chan ApplyMsg) {
	commitIndex, lastApplied := 0, 0
	rf.applyCond.L.Lock()
	for {
		rf.applyCond.Wait()

		DPrintf("[%d] apply wake up\n", rf.me)
		rf.mu.Lock()
		commitIndex, lastApplied = rf.commitIndex, rf.lastApplied

		DPrintf("[%d] commitIndex = %d lastApplied = %d\n", rf.me, commitIndex, lastApplied)
		toApply := rf.log[lastApplied+1 : commitIndex+1]
		toApplyCopy := make([]logEntry, len(toApply))
		copy(toApplyCopy, toApply)
		DPrintf("[%d] toApply: %v\n", rf.me, toApplyCopy)
		rf.lastApplied = rf.commitIndex
		rf.mu.Unlock()

		for _, cmd := range toApplyCopy {
			oneApplyMsg := ApplyMsg{true, cmd.Command, cmd.Index, false, nil, 0, 0}
			applyCh <- oneApplyMsg

			DPrintf("[%d] ApplyMsg %v send\n", rf.me, oneApplyMsg)
		}
	}

}

func (rf *Raft) getAppendArgs(nextIndex int, args *AppendEntriesArgs) {
	args.Term = rf.currnetTerm
	args.LeaderId = rf.me
	args.PrevLogIndex = rf.log[nextIndex-1].Index
	args.PrevLogTerm = rf.log[nextIndex-1].Term
	entry := rf.log[nextIndex:]
	entryCopy := make([]logEntry, len(entry))
	copy(entryCopy, entry)
	// args.Entries = rf.log[nextIndex:]
	args.Entries = entryCopy
	args.LeaderCommit = rf.commitIndex
}

// func (rf *Raft) getMaxCommit() int {
// 	nowIndex := rf.commitIndex + 1
// 	for nowIndex < len(rf.log) {
// 		numsAgree := 1
// 		for i := 0; i < len(rf.peers); i++ {
// 			if i == rf.me {
// 				continue
// 			}
// 			if rf.matchIndex[i] >= nowIndex && rf.log[nowIndex].Term == rf.currnetTerm {
// 				numsAgree++
// 			}
// 		}
// 		if numsAgree > len(rf.peers)/2 {
// 			nowIndex++
// 		} else {
// 			break
// 		}
// 	}
// 	return nowIndex - 1
// }

// rf have accquired rf.lock
func (rf *Raft) getMaxCommit() int {
	prevCommit := rf.commitIndex
	nowIndex := len(rf.log) - 1
	for ; nowIndex > prevCommit; nowIndex-- {
		numsAgree := 1
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			if rf.matchIndex[i] >= nowIndex && rf.log[nowIndex].Term == rf.currnetTerm {
				numsAgree++
			}
		}
		if numsAgree > len(rf.peers)/2 {
			break
		}
	}
	return nowIndex
}
