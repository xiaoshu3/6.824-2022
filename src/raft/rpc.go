package raft

import (
	"time"
)

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []logEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	Xterm  int
	Xindex int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// DPrintf("accquire lock")
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// DPrintf("after accquire lock")
	reply.Term = rf.CurrnetTerm
	reply.VoteGranted = false

	DPrintf("[%d]:%d receive a vote request from [%d]:%d\n", rf.me, rf.CurrnetTerm, args.CandidateId, args.Term)
	if args.Term < rf.CurrnetTerm {
		return
	}

	if args.Term > rf.CurrnetTerm {
		rf.CurrnetTerm = args.Term
		rf.state = Follower
		rf.VotedFor = -1
	}

	nowLastIndex := len(rf.Log) - 1
	var lastIndexTerm, lastIndex int
	if nowLastIndex == -1 {
		lastIndexTerm = rf.LastIncludedTerm
		lastIndex = rf.LastIncludedIndex
	} else {
		lastIndexTerm = rf.Log[nowLastIndex].Term
		lastIndex = rf.Log[nowLastIndex].Index
	}

	if rf.VotedFor == -1 || rf.VotedFor == args.CandidateId {
		if lastIndexTerm > args.LastLogTerm || (lastIndexTerm == args.LastLogTerm &&
			lastIndex > args.LastLogIndex) {
			return
		}

		rf.VotedFor = args.CandidateId
		// reply.Term = rf.CurrnetTerm
		reply.VoteGranted = true
		DPrintf("rf.Log[lastIndex].Term %d args.LastLogTerm %d\n", lastIndexTerm, args.LastLogTerm)
		DPrintf("rf.Log[lastIndex].Index %d args.LastLogIndex %d\n", lastIndex, args.LastLogIndex)
		DPrintf("[%d] vote for [%d] and now term %d\n", rf.me, rf.VotedFor, rf.CurrnetTerm)

		rf.heartbeatTime = time.Now()
	}
	rf.persist()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// DPrintf("[%d] receive Append from [%d] \n", rf.me, args.LeaderId)
	rf.mu.Lock()
	// DPrintf("acquire the lock\n")
	defer rf.mu.Unlock()

	reply.Term = rf.CurrnetTerm
	reply.Success = false

	if args.Term < rf.CurrnetTerm {
		DPrintf("< return\n")
		return
	}
	if args.Term > rf.CurrnetTerm {
		// if args.Term > rf.CurrnetTerm {
		DPrintf("[%d] receive Append from [%d] get a higher term %d\n", rf.me, args.LeaderId, args.Term)
		// }
		// rf.becomeFollower(args.Term)
		// reply.Success = true
		// rf.heartbeatTime = time.Now()
		// DPrintf("[%d] receive Append from [%d] at term %d\n", rf.me, args.LeaderId, rf.CurrnetTerm)
		rf.becomeFollower(args.Term)
		reply.Term = args.Term
	}

	rf.heartbeatTime = time.Now()
	// lastlogIndex := len(rf.Log) - 1
	// if args.PrevLogIndex > rf.Log[lastlogIndex].Index || args.PrevLogTerm != rf.Log[args.PrevLogIndex].Term {

	// 	if args.PrevLogIndex > rf.Log[lastlogIndex].Index {
	// 		reply.Term = -1
	// 		reply.Xindex = rf.Log[lastlogIndex].Index
	// 	} else {
	// 		reply.Xterm = rf.Log[args.PrevLogIndex].Term
	// 		i := args.PrevLogIndex - 1
	// 		for i >= 0 {
	// 			if rf.Log[i].Term == rf.Log[args.PrevLogIndex].Term {
	// 				i--
	// 			} else {
	// 				break
	// 			}
	// 		}
	// 		reply.Xindex = i + 1
	// 	}

	// 	DPrintf("[%d] receive Append from [%d] don't match\n", rf.me, args.LeaderId)
	// 	DPrintf("[%d] prevIndex: %d lastlogIndex: %d  prevTerm %d log %v\n",
	// 		rf.me, args.PrevLogIndex, rf.Log[lastlogIndex].Index, args.PrevLogTerm, rf.Log)
	// 	return
	// }

	nowLastIndex := rf.getLastIndex()
	if args.PrevLogIndex > nowLastIndex {
		DPrintf("args.PrevLogIndex > nowLastIndex %d > %d\n", args.PrevLogIndex, nowLastIndex)
		reply.Term = -1
		reply.Xindex = nowLastIndex
		return
	}

	if args.PrevLogIndex < rf.LastIncludedIndex {
		DPrintf("Something error \n")
		reply.Term = -1
		reply.Xindex = args.PrevLogIndex - 1
		return
	} else if args.PrevLogIndex == rf.LastIncludedIndex {
		if args.PrevLogTerm != rf.LastIncludedTerm {
			DPrintf("Something error here\n")

			reply.Term = -1
			reply.Xindex = args.PrevLogIndex - 1
			return
		}
	} else if rf.Log[rf.getRelativelyIndex(args.PrevLogIndex)].Term != args.PrevLogTerm {
		reply.Xterm = rf.Log[rf.getRelativelyIndex(args.PrevLogIndex)].Term
		i := args.PrevLogIndex - 1
		for i > rf.LastIncludedIndex {
			if rf.Log[rf.getRelativelyIndex(i)].Term == reply.Xterm {
				i--
			} else {
				break
			}
		}
		reply.Xindex = i + 1
		DPrintf("[%d] receive Append from [%d] don't match\n", rf.me, args.LeaderId)
		DPrintf("[%d] prevIndex: %d LastIncludedIndex: %d  prevTerm %d log %v\n",
			rf.me, args.PrevLogIndex, rf.LastIncludedIndex, args.PrevLogTerm, rf.Log)
		return
	}

	DPrintf("[%d] receive Append from [%d]  match\n", rf.me, args.LeaderId)
	reply.Success = true
	// EntriesCopy := make([]logEntry, len(args.Entries))
	// copy(EntriesCopy, args.Entries)

	index := args.PrevLogIndex + 1
	relativeIndex := rf.getRelativelyIndex(index)
	if len(rf.Log[relativeIndex:]) <= len(args.Entries) {
		rf.Log = rf.Log[:relativeIndex]
		rf.Log = append(rf.Log, args.Entries...)
	} else {
		for _, entery := range args.Entries {
			rf.Log[rf.getRelativelyIndex(index)] = entery
			index++
		}
		if index > rf.commitIndex {
			DPrintf("commitIndex %d index %d len %d\n", rf.commitIndex, index, len(rf.Log))
			rf.Log = rf.Log[:rf.getRelativelyIndex(index)]
		}
	}

	DPrintf("[%d] receive Append from [%d]  match\n", rf.me, args.LeaderId)
	DPrintf("now [%d] log %v\n", rf.me, rf.Log)

	if args.LeaderCommit > rf.commitIndex {
		DPrintf("[%d] update commit to %d\n", rf.me, args.LeaderCommit)
		// rf.commitIndex = min(args.LeaderCommit, rf.Log[len(rf.Log)-1].Index)
		rf.commitIndex = min(args.LeaderCommit, rf.getLastIndex())
		rf.applyCond.Signal()
	}

	rf.persist()
	rf.heartbeatTime = time.Now()

}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	// DPrintf("[%d] receive a snapshop rpc from [%d]\n", rf.me, args.LeaderId)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.CurrnetTerm

	// DPrintf("start a snapshot rpc\n")
	if args.Term < rf.CurrnetTerm || args.LastIncludedIndex <= rf.LastIncludedIndex {
		DPrintf("return\n")
		return
	}

	index := args.LastIncludedIndex + 1
	realativeIndex := rf.getRelativelyIndex(index)
	if len(rf.Log) <= realativeIndex {
		rf.Log = rf.Log[:0]
	} else {
		rf.Log = rf.Log[realativeIndex:]
	}

	rf.LastIncludedIndex = args.LastIncludedIndex
	rf.LastIncludedTerm = args.LastIncludedTerm
	rf.lastApplied = rf.LastIncludedIndex
	rf.commitIndex = rf.lastApplied

	DataCopy := make([]byte, len(args.Data))
	copy(DataCopy, args.Data)

	rf.logRecoverFromSnapshot(args.Data)
	rf.persister.SaveSnapshotState(DataCopy)
	rf.persist()

	DPrintf("[%d] install a snapshot\n", rf.me)
	DPrintf("[%d] lastIncludeIndex = %d  lastApplied = %d log = %v\n", rf.me, rf.LastIncludedIndex, rf.lastApplied, rf.Log)
}
