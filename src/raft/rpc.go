package raft

import (
	"time"
)

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term        int
	CandidateId int
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

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId
		// reply.Term = rf.currnetTerm
		reply.VoteGranted = true
		rf.heartbeatTime = time.Now()
		DPrintf("[%d] vote for [%d] and now term %d\n", rf.me, rf.votedFor, rf.currnetTerm)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// c := sync.NewCond(&rf.mu)
	// c.L.Lock()
	// defer c.Broadcast()
	// defer c.L.Unlock()

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currnetTerm
	reply.Success = false

	if args.Term < rf.currnetTerm {
		return
	}
	if args.Term >= rf.currnetTerm {
		if args.Term > rf.currnetTerm {
			DPrintf("[%d] receive Append from [%d] get a higher term %d\n", rf.me, args.LeaderId, args.Term)
		}
		rf.becomeFollower(args.Term)
		reply.Success = true
		rf.heartbeatTime = time.Now()
		DPrintf("[%d] receive Append from [%d] at term %d\n", rf.me, args.LeaderId, rf.currnetTerm)
	}

}
