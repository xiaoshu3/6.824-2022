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
	reply.Term = rf.currnetTerm
	reply.VoteGranted = false

	DPrintf("[%d]:%d receive a vote request from [%d]:%d\n", rf.me, rf.currnetTerm, args.CandidateId, args.Term)
	if args.Term < rf.currnetTerm {
		return
	}

	if args.Term > rf.currnetTerm {
		rf.currnetTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
	}

	lastIndex := len(rf.log) - 1

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if rf.log[lastIndex].Term > args.LastLogTerm || (rf.log[lastIndex].Term == args.LastLogTerm &&
			rf.log[lastIndex].Index > args.LastLogIndex) {
			return
		}
		rf.votedFor = args.CandidateId
		// reply.Term = rf.currnetTerm
		reply.VoteGranted = true
		rf.heartbeatTime = time.Now()
		DPrintf("[%d] vote for [%d] and now term %d\n", rf.me, rf.votedFor, rf.currnetTerm)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currnetTerm
	reply.Success = false

	if args.Term < rf.currnetTerm {
		return
	}
	if args.Term > rf.currnetTerm {
		// if args.Term > rf.currnetTerm {
		DPrintf("[%d] receive Append from [%d] get a higher term %d\n", rf.me, args.LeaderId, args.Term)
		// }
		// rf.becomeFollower(args.Term)
		// reply.Success = true
		// rf.heartbeatTime = time.Now()
		// DPrintf("[%d] receive Append from [%d] at term %d\n", rf.me, args.LeaderId, rf.currnetTerm)
		rf.becomeFollower(args.Term)
		reply.Term = args.Term
	}

	rf.heartbeatTime = time.Now()
	lastlogIndex := len(rf.log) - 1
	if args.PrevLogIndex > rf.log[lastlogIndex].Index || args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {

		DPrintf("[%d] receive Append from [%d] don't match\n", rf.me, args.LeaderId)
		DPrintf("[%d] prevIndex: %d lastlogIndex: %d  prevTerm %d log %v\n",
			rf.me, args.PrevLogIndex, rf.log[lastlogIndex].Index, args.PrevLogTerm, rf.log)
		return
	}

	DPrintf("[%d] receive Append from [%d]  match\n", rf.me, args.LeaderId)
	reply.Success = true
	EntriesCopy := make([]logEntry, len(args.Entries))
	copy(EntriesCopy, args.Entries)

	index := args.PrevLogIndex + 1
	if rf.log[lastlogIndex].Index == args.PrevLogIndex || len(rf.log[index:]) <= len(EntriesCopy) {
		rf.log = rf.log[:index]
		rf.log = append(rf.log, EntriesCopy...)
	} else {
		for _, entery := range EntriesCopy {
			rf.log[index] = entery
			index++
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		DPrintf("[%d] update commit to %d\n", rf.me, args.LeaderCommit)
		rf.commitIndex = min(args.LeaderCommit, rf.log[len(rf.log)-1].Index)
		rf.applyCond.Signal()
	}
	rf.heartbeatTime = time.Now()

}
