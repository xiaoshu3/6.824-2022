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

	lastIndex := len(rf.Log) - 1

	if rf.VotedFor == -1 || rf.VotedFor == args.CandidateId {
		if rf.Log[lastIndex].Term > args.LastLogTerm || (rf.Log[lastIndex].Term == args.LastLogTerm &&
			rf.Log[lastIndex].Index > args.LastLogIndex) {
			return
		}

		rf.VotedFor = args.CandidateId
		// reply.Term = rf.CurrnetTerm
		reply.VoteGranted = true
		DPrintf("rf.Log[lastIndex].Term %d args.LastLogTerm %d\n", rf.Log[lastIndex].Term, args.LastLogTerm)
		DPrintf("rf.Log[lastIndex].Index %d args.LastLogIndex %d\n", rf.Log[lastIndex].Index, args.LastLogIndex)
		DPrintf("[%d] vote for [%d] and now term %d\n", rf.me, rf.VotedFor, rf.CurrnetTerm)

		rf.heartbeatTime = time.Now()
	}
	rf.persist()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.CurrnetTerm
	reply.Success = false

	if args.Term < rf.CurrnetTerm {
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
	lastlogIndex := len(rf.Log) - 1
	if args.PrevLogIndex > rf.Log[lastlogIndex].Index || args.PrevLogTerm != rf.Log[args.PrevLogIndex].Term {

		if args.PrevLogIndex > rf.Log[lastlogIndex].Index {
			reply.Term = -1
			reply.Xindex = rf.Log[lastlogIndex].Index
		} else {
			reply.Xterm = rf.Log[args.PrevLogIndex].Term
			i := args.PrevLogIndex - 1
			for i >= 0 {
				if rf.Log[i].Term == rf.Log[args.PrevLogIndex].Term {
					i--
				} else {
					break
				}
			}
			reply.Xindex = i + 1
		}

		DPrintf("[%d] receive Append from [%d] don't match\n", rf.me, args.LeaderId)
		DPrintf("[%d] prevIndex: %d lastlogIndex: %d  prevTerm %d log %v\n",
			rf.me, args.PrevLogIndex, rf.Log[lastlogIndex].Index, args.PrevLogTerm, rf.Log)
		return
	}

	DPrintf("[%d] receive Append from [%d]  match\n", rf.me, args.LeaderId)
	reply.Success = true
	EntriesCopy := make([]logEntry, len(args.Entries))
	copy(EntriesCopy, args.Entries)

	index := args.PrevLogIndex + 1
	if rf.Log[lastlogIndex].Index == args.PrevLogIndex || len(rf.Log[index:]) <= len(EntriesCopy) {
		rf.Log = rf.Log[:index]
		rf.Log = append(rf.Log, EntriesCopy...)
	} else {
		for _, entery := range EntriesCopy {
			rf.Log[index] = entery
			index++
		}

		// no this may change the log commited
		if index > rf.commitIndex {
			DPrintf("commitIndex %d index %d len %d\n", rf.commitIndex, index, len(rf.Log))
			rf.Log = rf.Log[:index]
		}
	}

	DPrintf("[%d] receive Append from [%d]  match\n", rf.me, args.LeaderId)
	DPrintf("now [%d] log %v\n", rf.me, rf.Log)

	if args.LeaderCommit > rf.commitIndex {
		DPrintf("[%d] update commit to %d\n", rf.me, args.LeaderCommit)
		rf.commitIndex = min(args.LeaderCommit, rf.Log[len(rf.Log)-1].Index)
		rf.applyCond.Signal()
	}

	rf.persist()
	rf.heartbeatTime = time.Now()

}
