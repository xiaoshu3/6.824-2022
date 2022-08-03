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

	lastindex := rf.getLastIndex()
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = lastindex + 1
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

func (rf *Raft) apply() {
	commitIndex, lastApplied := 0, 0
	rf.applyCond.L.Lock()
	defer rf.applyCond.L.Unlock()
	for {
		rf.applyCond.Wait()
		if rf.killed() {
			return
		}
		DPrintf("[%d] apply wake up\n", rf.me)
		rf.mu.Lock()
		commitIndex, lastApplied = rf.commitIndex, rf.lastApplied

		DPrintf("[%d] commitIndex = %d lastApplied = %d lastIncludeIndex = %d\n", rf.me, commitIndex, lastApplied, rf.LastIncludedIndex)

		toApply := rf.Log[rf.getRelativelyIndex(lastApplied)+1 : rf.getRelativelyIndex(commitIndex)+1]
		toApplyCopy := make([]logEntry, len(toApply))
		copy(toApplyCopy, toApply)
		DPrintf("[%d] toApply: %v\n", rf.me, toApplyCopy)
		rf.lastApplied = rf.commitIndex
		rf.mu.Unlock()

		for _, cmd := range toApplyCopy {
			oneApplyMsg := ApplyMsg{true, cmd.Command, cmd.Index, false, nil, cmd.Term, 0}
			rf.applyCh <- oneApplyMsg

			DPrintf("[%d] ApplyMsg %v send\n", rf.me, oneApplyMsg)
		}
	}

}

func (rf *Raft) getAppendArgs(nextIndex int, args *AppendEntriesArgs) bool {
	relativeIndex := rf.getRelativelyIndex(nextIndex)
	if relativeIndex < 0 {
		DPrintf("relativeIndex < 0 nextIndex = %d  lastIncludeIndex = %d\n", nextIndex, rf.LastIncludedIndex)
		return false
	} else if relativeIndex == 0 {
		args.PrevLogIndex = rf.LastIncludedIndex
		args.PrevLogTerm = rf.LastIncludedTerm
	} else {
		args.PrevLogIndex = rf.Log[relativeIndex-1].Index
		args.PrevLogTerm = rf.Log[relativeIndex-1].Term
	}

	args.Term = rf.CurrnetTerm
	args.LeaderId = rf.me
	args.LeaderCommit = rf.commitIndex

	args.Entries = rf.Log[relativeIndex:]
	return true
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
	nowIndex := rf.getLastIndex()
	for ; nowIndex > prevCommit; nowIndex-- {
		numsAgree := 1
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}

			if nowIndex < rf.LastIncludedIndex {
				DPrintf("[%d] nowIndex = %d commitIndex = %d lastIncludeIndex = %d lastIndex = %d\n", rf.me, nowIndex, rf.commitIndex, rf.LastIncludedIndex, rf.getLastIndex())
			}
			if rf.matchIndex[i] >= nowIndex && rf.Log[rf.getRelativelyIndex(nowIndex)].Term == rf.CurrnetTerm {
				numsAgree++
			}

			// if rf.matchIndex[i] >= nowIndex {

			// 	if nowIndex < rf.LastIncludedIndex {
			// 		DPrintf("[%d] nowIndex = %d commitIndex = %d lastIncludeIndex = %d\n", rf.me, nowIndex, rf.commitIndex, rf.LastIncludedIndex)
			// 	}
			// 	term := 0
			// 	if nowIndex == rf.LastIncludedIndex {
			// 		term = rf.LastIncludedTerm
			// 	} else {
			// 		term = rf.Log[rf.getRelativelyIndex(nowIndex)].Term
			// 	}

			// 	if term == rf.CurrnetTerm {
			// 		numsAgree++
			// 	}
			// }
		}
		if numsAgree > len(rf.peers)/2 {
			break
		}
	}
	return nowIndex
}

func (rf *Raft) getRelativelyIndex(index int) int {
	return index - rf.LastIncludedIndex - 1
}

func (rf *Raft) getLastIndex() int {
	return rf.LastIncludedIndex + len(rf.Log)
}
