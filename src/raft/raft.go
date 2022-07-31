package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const (
	Follower = iota
	Candidate
	Leader
)

//
// A Go object implementing a single Raft peer.
//

type logEntry struct {
	Index   int
	Term    int
	Command interface{}
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state       int
	CurrnetTerm int
	VotedFor    int
	Log         []logEntry

	applyCond     sync.Cond
	heartbeatTime time.Time

	commitIndex int
	lastApplied int

	//leaders
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.CurrnetTerm
	isleader = (rf.state == Leader)
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrnetTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedfor int
	var log []logEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedfor) != nil || d.Decode(&log) != nil {
		fmt.Println("readPersist decode error")
	} else {
		rf.CurrnetTerm = currentTerm
		rf.VotedFor = votedfor
		rf.Log = log
	}

	DPrintf("[%d] restart term %d  votedFor %d log %v\n", rf.me, currentTerm, votedfor, log)
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	// DPrintf("before call request vote\n")
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	// DPrintf("after request vote\n")
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	// Your code here (2B).
	term, isLeader = rf.GetState()
	if isLeader {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.state == Leader {
			lastIndex := len(rf.Log)
			cmd := logEntry{lastIndex, rf.CurrnetTerm, command}
			rf.Log = append(rf.Log, cmd)
			DPrintf("[%d] append a log %v and now log %v\n", rf.me, cmd, rf.Log)
			index = lastIndex

			rf.persist()
		}
	} else {
		isLeader = false
	}

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) leaderElection() {
	rf.mu.Lock()
	if rf.state != Candidate {
		rf.mu.Unlock()
		return
	}

	candidateId := rf.me
	candidateTerm := rf.CurrnetTerm
	lastIndex := len(rf.Log) - 1
	lastLogIndex := rf.Log[lastIndex].Index
	lastLogTerm := rf.Log[lastIndex].Term
	var numsVote int32 = 1
	var finished int32 = 1
	numsPeer := len(rf.peers)

	c := sync.NewCond(&rf.mu)
	for i := 0; i < numsPeer; i++ {
		if i == rf.me {
			continue
		}
		go func(server int) {
			defer atomic.AddInt32(&finished, 1)

			args := RequestVoteArgs{candidateTerm, candidateId, lastLogIndex, lastLogTerm}
			reply := RequestVoteReply{}
			DPrintf("[%d] send requrst vote to [%d] at term %d\n", rf.me, server, candidateTerm)
			ok := rf.sendRequestVote(server, &args, &reply)
			if ok {
				c.L.Lock()
				defer c.L.Unlock()
				defer c.Signal()

				if rf.IsRpcExpired(Candidate, candidateTerm) {
					return
				}
				if reply.Term > rf.CurrnetTerm {
					rf.becomeFollower(reply.Term)
					rf.persist()
					DPrintf("[%d] receive [%d] a vote and beacome follower at term %d\n", rf.me, server, rf.CurrnetTerm)
					return
				}
				if reply.VoteGranted {
					atomic.AddInt32(&numsVote, 1)
					// DPrintf("[%d] got a vote from [%d] and nums vote: %d\n", rf.me, server, numsVote)
				}
			}
		}(i)
	}

	for !rf.IsRpcExpired(Candidate, candidateTerm) && atomic.LoadInt32(&numsVote) <= (int32(numsPeer/2)) && atomic.LoadInt32(&finished) < int32(numsPeer) && !rf.killed() {
		DPrintf("[%d] have a election term %d finished = %d\n", rf.me, rf.CurrnetTerm, atomic.LoadInt32(&finished))
		c.Wait()
	}
	if !rf.IsRpcExpired(Candidate, candidateTerm) && !rf.killed() && atomic.LoadInt32(&numsVote) > (int32(numsPeer/2)) {
		rf.becomeLeader()
		rf.persist()
		DPrintf("[%d] become leader at term %d\n", rf.me, rf.CurrnetTerm)
	}

	c.L.Unlock()
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.

func (rf *Raft) ticker() {
	for !rf.killed() {
		electionTimeOut := rand.Intn(200) + 500
		time.Sleep(time.Duration(electionTimeOut) * time.Millisecond)

		rf.mu.Lock()
		if rf.state != Leader {
			if time.Since(rf.heartbeatTime).Milliseconds() >= int64(electionTimeOut) {
				rf.becomeCandidate()
				rf.persist()
				DPrintf("[%d] attemping election at term %d\n ", rf.me, rf.CurrnetTerm)
				go rf.leaderElection()
			}
		}
		rf.mu.Unlock()
	}

}

func (rf *Raft) leaderSendAppendEntries(leaderTerm int) {
	heartBeatTimeOut := 200
	for !rf.killed() {

		leaderId := rf.me
		numsPeer := len(rf.peers)

		for i := 0; i < numsPeer; i++ {
			if i == leaderId {
				continue
			}
			go func(server int) {
				args := AppendEntriesArgs{}
				reply := AppendEntriesReply{}

				rf.mu.Lock()

				// maybe now rf is'not leader and it's nextIndex changed
				if rf.IsRpcExpired(Leader, leaderTerm) {
					DPrintf("[%d] find it is not leader\n", rf.me)
					rf.mu.Unlock()
					return
				}

				rf.getAppendArgs(rf.nextIndex[server], &args)
				if rf.IsRpcExpired(Leader, leaderTerm) {
					rf.mu.Unlock()
					return
				}

				nowNextIndexServer := rf.nextIndex[server]
				rf.mu.Unlock()
				DPrintf("[%d] send append to [%d]\n", rf.me, server)
				ok := rf.sendAppendEntries(server, &args, &reply)

				for !ok || !reply.Success {
					rf.mu.Lock()
					if rf.IsRpcExpired(Leader, leaderTerm) || nowNextIndexServer != rf.nextIndex[server] {
						rf.mu.Unlock()
						return
					}
					rf.mu.Unlock()

					if !ok {
						DPrintf("[%d] send append to [%d] failed\n", rf.me, server)
						time.Sleep(10 * time.Millisecond)
						reply = AppendEntriesReply{}
						ok = rf.sendAppendEntries(server, &args, &reply)
						continue
					}
					rf.mu.Lock()
					// If no next f.IsRpcExpired it will data race
					// data race write at rpc.go 126
					// I think it's not a leader and other leader write it log and rpc read it  since in args log is shared
					// I decrease heartbeatTime and data race arise
					// so I pass it by a log copy
					if rf.IsRpcExpired(Leader, leaderTerm) {
						DPrintf("[%d] find it is not leader\n", rf.me)
						rf.mu.Unlock()
						return
					}
					if reply.Term > rf.CurrnetTerm {
						DPrintf("[%d] receive a higher term from [%d] at term %d\n", rf.me, server, rf.CurrnetTerm)
						rf.becomeFollower(reply.Term)
						rf.persist()
						rf.mu.Unlock()
						return
					}

					DPrintf("[%d] nextIndex become to %d\n", server, rf.nextIndex[server]-1)
					// rf.nextIndex[server]--
					if nowNextIndexServer != rf.nextIndex[server] {
						rf.mu.Unlock()
						return
					}

					if reply.Term == -1 || rf.Log[reply.Xindex].Term == reply.Term {
						rf.nextIndex[server] = reply.Xindex + 1
					} else {
						rf.nextIndex[server] = reply.Xindex
					}
					// rf.nextIndex[server] = rf.nextIndex[server] - 1
					nowNextIndexServer = rf.nextIndex[server]

					rf.getAppendArgs(rf.nextIndex[server], &args)
					rf.mu.Unlock()
					reply = AppendEntriesReply{}
					ok = rf.sendAppendEntries(server, &args, &reply)
				}

				rf.mu.Lock()
				DPrintf("[%d] send append to [%d] successful\n", rf.me, server)
				if rf.IsRpcExpired(Leader, leaderTerm) {
					DPrintf("[%d] find it is not leader\n", rf.me)
					rf.mu.Unlock()
					return
				}
				DPrintf("after match server:[%d]:  args:%v\n", server, args)
				rf.matchIndex[server] = max(rf.matchIndex[server], args.PrevLogIndex+len(args.Entries))
				rf.nextIndex[server] = max(rf.nextIndex[server], rf.matchIndex[server]+1)

				DPrintf("[%d] matchIndex %d,nextIndex %d\n", server, rf.matchIndex[server], rf.nextIndex[server])
				rf.mu.Unlock()

			}(i)
		}
		rf.mu.Lock()
		if rf.IsRpcExpired(Leader, leaderTerm) {
			rf.mu.Unlock()
			return
		}
		nowCommit := rf.getMaxCommit()
		if nowCommit > rf.commitIndex {
			DPrintf("[%d] update commit %d\n", rf.me, nowCommit)
			rf.commitIndex = nowCommit
			rf.applyCond.Signal()
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(heartBeatTimeOut) * time.Millisecond)
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.applyCond = *sync.NewCond(&sync.Mutex{})
	rf.becomeFollower(0)
	rf.VotedFor = -1

	rf.Log = []logEntry{}
	rf.Log = append(rf.Log, logEntry{0, -1, nil})
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.persist()
	// start ticker goroutine to start elections

	go rf.ticker()
	go rf.apply(applyCh)
	return rf
}
