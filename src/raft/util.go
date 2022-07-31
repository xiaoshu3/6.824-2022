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
	return rf.state != state || rf.CurrnetTerm != term
}

func (rf *Raft) becomeFollower(term int) {
	rf.state = Follower
	rf.CurrnetTerm = term
	rf.heartbeatTime = time.Now()
}

func (rf *Raft) becomeCandidate() {
	rf.state = Candidate
	rf.CurrnetTerm++
	rf.VotedFor = rf.me
	rf.heartbeatTime = time.Now()
}

func (rf *Raft) becomeLeader() {
	rf.state = Leader
	// rf.appandANoOpLog()
	go rf.leaderSendAppendEntries(rf.CurrnetTerm)

	lenLog := len(rf.Log)
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = lenLog
		rf.matchIndex[i] = 0
	}
}

// become Leader first appaand a no-op log
// can't pass in 2B some tests
func (rf *Raft) appandANoOpLog() {
	lastIndex := len(rf.Log)
	// nil can't pass 2C internalChurn 1094 "not an int"
	// cmd := logEntry{lastIndex, rf.CurrnetTerm, nil}
	cmd := logEntry{lastIndex, rf.CurrnetTerm, 0}

	rf.Log = append(rf.Log, cmd)
}

func (rf *Raft) apply(applyCh chan ApplyMsg) {
	commitIndex, lastApplied := 0, 0
	rf.applyCond.L.Lock()
	for !rf.killed() {
		rf.applyCond.Wait()

		DPrintf("[%d] apply wake up\n", rf.me)
		rf.mu.Lock()
		commitIndex, lastApplied = rf.commitIndex, rf.lastApplied

		DPrintf("[%d] commitIndex = %d lastApplied = %d\n", rf.me, commitIndex, lastApplied)
		toApply := rf.Log[lastApplied+1 : commitIndex+1]
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
	args.Term = rf.CurrnetTerm
	args.LeaderId = rf.me
	args.PrevLogIndex = rf.Log[nextIndex-1].Index
	args.PrevLogTerm = rf.Log[nextIndex-1].Term

	// entry := rf.Log[nextIndex:]
	// entryCopy := make([]logEntry, len(entry))
	// copy(entryCopy, entry)
	// args.Entries = entryCopy

	args.Entries = rf.Log[nextIndex:]
	// args.Entries = entry
	args.LeaderCommit = rf.commitIndex
}

// func (rf *Raft) getMaxCommit() int {
// 	nowIndex := rf.commitIndex + 1
// 	for nowIndex < len(rf.Log) {
// 		numsAgree := 1
// 		for i := 0; i < len(rf.peers); i++ {
// 			if i == rf.me {
// 				continue
// 			}
// 			if rf.matchIndex[i] >= nowIndex && rf.Log[nowIndex].Term == rf.CurrnetTerm {
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
	nowIndex := len(rf.Log) - 1
	for ; nowIndex > prevCommit; nowIndex-- {
		numsAgree := 1
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			if rf.matchIndex[i] >= nowIndex && rf.Log[nowIndex].Term == rf.CurrnetTerm {
				numsAgree++
			}
		}
		if numsAgree > len(rf.peers)/2 {
			break
		}
	}
	return nowIndex
}
