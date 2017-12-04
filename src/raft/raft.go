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

import "sync"
import "labrpc"
import (
	"fmt"
	"math/rand"
	// "os"
	"time"
)

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	CurrentTerm int
	// VoteFor should be -1 when it is not a candidate or not vote for anyone
	VoteFor     int
	CommitIndex int
	LastApplied int
	// listen from leader index, initial to 0
	LeaderApplied  int
	SelfApplyIndex int
	NextIndex      []int
	MatchIndex     []int
	state          int //0: follower 1: candidate 2: leader
	// when it is a candidate, its reply is here
	ReqForVoteReply []*RequestVoteReply
	ReqVoteBool     []bool
	ReqForAppEntry  []*RequestAppendEntriesReply
	AppendEntryBool []bool

	AppeFromLeader bool
	timer          *time.Timer
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	term = rf.CurrentTerm
	isleader = rf.state == 2
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

type RequestAppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      int
	LeaderCommit int
}

type RequestAppendEntriesReply struct {
	Term    int
	Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	if args.Term < rf.CurrentTerm {
		reply.VoteGranted = false
		return
	}
	if (rf.VoteFor == -1 || rf.VoteFor == args.CandidateId || rf.CurrentTerm < args.Term) && args.LastLogIndex >= 0 {
		reply.VoteGranted = true
		rf.CurrentTerm = args.Term
		rf.AppeFromLeader = true
		rf.state = 0
		rf.VoteFor = args.CandidateId
		rf.timer.Stop()
		rf.loop()
		// fmt.Printf("raft %d voted for raft %d\n", rf.me, args.CandidateId)
		return
	}
	reply.VoteGranted = false
	// fmt.Printf("***********\n"+
	// 	"raft %d call request for vote should not come into here!!!\n"+
	// 	"VoteFor: %d, CandidateId: %d, CurrentTerm: %d, candidateTerm: %d\n"+
	// 	"*************\n",
	// 	rf.me, rf.VoteFor, args.CandidateId, rf.CurrentTerm, args.Term)
}

func (rf *Raft) RequestAppendEntries(args RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	// fmt.Printf("raft %d be called append entry from raft %d\n", rf.me, args.LeaderId)
	if args.Term < rf.CurrentTerm {
		reply.Success = false
		reply.Term = rf.CurrentTerm
		return
	}
	if 1 == 2 /*log doesn't contain an entry at prevlofindex whose term matches prevlogterm*/ {
		reply.Success = false
		return
	}
	// if an existing entry conflicts with a new one (same index but different terms).
	// delete the existing entry and all that follow it
	if true {

	}
	// append any new entries not already in the log

	// fmt.Printf("append entry into here\n")
	// if leaderCommit > commitIndex, set commitIndex - min(leaderCommit, index of last new entry)
	reply.Success = true
	rf.AppeFromLeader = true
	rf.CurrentTerm = args.Term
	rf.VoteFor = -1
	rf.state = 0
	rf.timer.Stop()
	go rf.loop()
	// fmt.Printf("^^^^^^^^^^^^^^^^^^^^^^^^^^^\n"+
	// 	"raft %d appended entry from raft %d\n"+
	// 	"^^^^^^^^^^^^^^^^^^^^^^\n", rf.me, args.LeaderId)
	return
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) {
	rf.ReqVoteBool[server] = rf.peers[server].Call("Raft.RequestVote", args, reply)
	// return ok
}

func (rf *Raft) sendRequestAppendEntry(server int, args RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	rf.AppendEntryBool[server] = rf.peers[server].Call("Raft.RequestAppendEntries", args, reply)
	// return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	isLeader = rf.state == 2
	fmt.Printf("***********************************\n"+
		"raft %d call start()\n"+
		"*************************************\n",
		rf.me)
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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

	rf.VoteFor = -1
	rf.state = 0
	rf.CurrentTerm = -1
	rf.AppeFromLeader = false

	// initial request for vote
	rf.ReqForVoteReply = []*RequestVoteReply{}
	rf.ReqForAppEntry = []*RequestAppendEntriesReply{}
	rf.ReqVoteBool = []bool{}
	rf.AppendEntryBool = []bool{}
	// rf.ReqVoteBool = [len(peers)]bool{false}
	for i := 0; i < len(peers); i++ {
		Replyy := new(RequestVoteReply)
		Replyy.VoteGranted = false
		Replyy.Term = -1
		rf.ReqForVoteReply = append(rf.ReqForVoteReply, Replyy)

		AppEntry := new(RequestAppendEntriesReply)
		AppEntry.Success = false
		AppEntry.Term = -1
		rf.ReqForAppEntry = append(rf.ReqForAppEntry, AppEntry)

		false1 := false
		rf.ReqVoteBool = append(rf.ReqVoteBool, false1)

		false2 := false
		rf.AppendEntryBool = append(rf.AppendEntryBool, false2)
	}

	// Your initialization code here.
	// end.Call("Raft.AppendEntries", &args, &reply)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rand.Seed(time.Now().UnixNano())

	go rf.loop()

	return rf
}
func (rf *Raft) loop() {
	// fmt.Printf("***********************************\n" +
	// 	"raft %d call loop()\n" +
	// 	"*************************************\n",
	// 	rf.me )
	x := 150
	if rf.state != 2 {
		x += rand.Intn(300)
	}
	// fmt.Printf("raft %d loop, sleep %d ms\n", rf.me, x)
	// rf.timer = time.NewTimer(time.Millisecond * time.Duration(x))
	// time.Sleep(time.Millisecond * time.Duration(x))
	rf.timer = time.AfterFunc(time.Millisecond*time.Duration(x), func() {
		rf.keepAlive()
		go rf.loop()
	})

}

func (rf *Raft) keepAlive() {
	// for all servers:1. increment lastApplied, apply log[lastApplied] to state machine
	// 2. if rpc request or response contains term T > currentTermm convert to follower.

	// for follower
	if rf.state == 0 {
		if rf.AppeFromLeader == false && rf.VoteFor == -1 {
			rf.state = 1
			rf.VoteFor = rf.me
			rf.CurrentTerm = 1 + rf.CurrentTerm //+ rf.me
			rf.RequestForVote()
		} else {
			rf.VoteFor = -1
			rf.AppeFromLeader = false
		}
	} else if rf.state == 1 {
		count := 1
		count2 := 1
		m := len(rf.peers)
		for i := 0; i < m; i++ {
			if i != rf.me {
				if rf.ReqVoteBool[i] == true && rf.ReqForVoteReply[i].VoteGranted == true {
					count++
				}
				if rf.ReqVoteBool[i] == true {
					count2++
				}
			}
		}
		// fmt.Printf("########################\n"+
		// 	"raft %d candidation summary: count1, %d, count2, %d\n"+
		// 	"#######################\n",
		// 	rf.me, count, count2)
		if count*2 > count2 && count2 != 1 {
			rf.state = 2
			rf.CurrentTerm++ //+ rf.me
			rf.AppendEntry()
			// fmt.Printf("***** raft %d claim to be leader!\n", rf.me)
		} else {
			rf.VoteFor = rf.me
			// rf.CurrentTerm = 1 + rf.CurrentTerm
			rf.RequestForVote()
		}
	} else if rf.state == 2 {
		count3 := 1
		m := len(rf.peers)
		for i := 0; i < m; i++ {
			if i != rf.me {
				if rf.AppendEntryBool[i] == true {
					count3++
				}
			}
		}
		if count3 == 1 {
			rf.state = 1
			rf.CurrentTerm += 1 + rf.me
			rf.RequestForVote()
			// fmt.Printf("******* raft %d turn from leader to candidate\n", rf.me)
		}
		rf.AppendEntry()
	}
}

// type RequestVoteArgs struct {
// 	// Your data here.
// 	Term         int
// 	CandidateId  int
// 	LastLogIndex int
// 	LastLogTerm  int
// }
func (rf *Raft) RequestForVote() {
	// fmt.Printf("raft %d request for vote\n", rf.me)
	rf.clearVote()
	args := RequestVoteArgs{rf.CurrentTerm, rf.me, 1, 1}
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go rf.sendRequestVote(i, args, rf.ReqForVoteReply[i])
		}
	}
}
func (rf *Raft) clearVote() {
	m := len(rf.peers)
	for i := 0; i < m; i++ {
		rf.ReqVoteBool[i] = false
	}
}

// type RequestAppendEntriesArgs struct {
// 	Term         int
// 	LeaderId	 int
// 	PrevLogIndex int
// 	PrevLogTerm  int
// 	Entries      int
// 	LeaderCommit int
// }
func (rf *Raft) AppendEntry() {
	rf.clearAppendEntry()
	args := RequestAppendEntriesArgs{
		rf.CurrentTerm,
		rf.me,
		0,
		0,
		0,
		0,
	}
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go rf.sendRequestAppendEntry(i, args, rf.ReqForAppEntry[i])
		}
	}
}
func (rf *Raft) clearAppendEntry() {
	m := len(rf.peers)
	for i := 0; i < m; i++ {
		rf.AppendEntryBool[i] = false
	}
}
