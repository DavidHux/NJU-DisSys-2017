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
	"bytes"
	"encoding/gob"
	"fmt"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// "os"

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

type Entry struct {
	Term    int
	Index   int
	Command interface{}
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
	// SleepOne    int
	// VoteFor should be -1 when it is not a candidate or not vote for anyone
	CurrentTerm int
	VoteFor     int
	CommitIndex int
	LastApplied int
	state       int //0: follower 1: candidate 2: leader
	// listen from leader index, initial to 0
	// LeaderApplied  int
	// SelfApplyIndex int
	NextIndex     []int
	LastNextIndex []int
	MatchIndex    []int
	LastAppendEnd []int
	// when it is a candidate, its reply is here
	ReqForVoteReply []*RequestVoteReply
	ReqVoteBool     []bool
	ReqForAppEntry  []*RequestAppendEntriesReply
	AppendEntryBool []bool
	Entries         []*Entry

	ApplyCh chan ApplyMsg

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
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VoteFor)
	e.Encode(rf.CommitIndex)
	e.Encode(rf.LastApplied)
	// e.Encode(rf.state)
	// fmt.Printf("encode rf entry length %d\n", len(rf.Entries))
	e.Encode(len(rf.Entries))
	for i := 0; i < len(rf.Entries); i++ {
		e.Encode(rf.Entries[i].Index)
		e.Encode(rf.Entries[i].Term)
		e.Encode(rf.Entries[i].Command.(int))
	}
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.CurrentTerm)
	d.Decode(&rf.VoteFor)
	d.Decode(&rf.CommitIndex)
	d.Decode(&rf.LastApplied)
	// d.Decode(&rf.state)
	rf.Entries = []*Entry{}
	lenEntry := 0
	command := 0
	d.Decode(&lenEntry)
	// fmt.Printf("persit d length %d\n", lenEntry)
	for i := 0; i < lenEntry; i++ {
		newEntry := new(Entry)
		d.Decode(&newEntry.Index)
		d.Decode(&newEntry.Term)
		d.Decode(&command)
		newEntry.Command = command
		rf.Entries = append(rf.Entries, newEntry)
	}
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
	Entries      []*Entry
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
	// fmt.Printf("***********\n"+
	// 	"raft %d call request for vote. "+
	// 	"VoteFor: %d, CandidateId: %d, CurrentTerm: %d, candidateTerm: %d\n"+
	// 	"*************\n",
	// 	rf.me, rf.VoteFor, args.CandidateId, rf.CurrentTerm, args.Term)
	// begin refuse vote
	// candidate's term <= local
	if args.Term <= rf.CurrentTerm {
		reply.VoteGranted = false
		reply.Term = rf.CurrentTerm
		return
	}
	// last entry's term of candidate's log < local lastTerm
	if len(rf.Entries) > 0 && (args.LastLogTerm < rf.Entries[len(rf.Entries)-1].Term ||
		args.LastLogTerm == rf.Entries[len(rf.Entries)-1].Term && args.LastLogIndex < len(rf.Entries)) {
		reply.VoteGranted = false
		reply.Term = -2
		rf.CurrentTerm = max(rf.CurrentTerm, args.Term)
		// fmt.Printf("raft %d not voted for raft %d, entry len %d, last %d\n", rf.me, args.CandidateId, len(rf.Entries), args.LastLogIndex)
		return
	}
	if !(rf.VoteFor == -1 || rf.VoteFor == args.CandidateId) {
		reply.VoteGranted = false
		reply.Term = -3
		if rf.CurrentTerm < args.Term {
			rf.AppeFromLeader = true
			rf.state = 0
		}
		rf.CurrentTerm = max(rf.CurrentTerm, args.Term)
		return
	}

	// grant vote
	if rf.VoteFor == -1 || rf.VoteFor == args.CandidateId {
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
	// if args.Term > rf.CurrentTerm {
	// 	rf.CurrentTerm = args.Term
	// 	// rf.AppeFromLeader = true
	// 	rf.state = 0
	// 	rf.VoteFor = -1
	// 	reply.VoteGranted = false
	// 	reply.Term = rf.CurrentTerm
	// 	// rf.timer.Stop()
	// 	// rf.loop()
	// 	fmt.Printf("request for vote cannot come into here rf %d\n", rf.me)
	// 	return
	// }
	reply.VoteGranted = false
	reply.Term = rf.CurrentTerm
	fmt.Printf("***********\n"+
		"raft %d call request for vote should not come into here!!!\n"+
		"VoteFor: %d, CandidateId: %d, CurrentTerm: %d, candidateTerm: %d\n"+
		"*************\n",
		rf.me, rf.VoteFor, args.CandidateId, rf.CurrentTerm, args.Term)
}

func (rf *Raft) RequestAppendEntries(args RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	// fmt.Printf("raft %d be called append entry from raft %d,term %d, leader Term %d, \n", rf.me, args.LeaderId, rf.CurrentTerm, args.Term)
	if args.Term < rf.CurrentTerm {
		// fmt.Printf("raft %d be called append entry from raft %d,term %d, leader Term %d, \n", rf.me, args.LeaderId, rf.CurrentTerm, args.Term)

		reply.Success = false
		reply.Term = rf.CurrentTerm
		return
	}
	// if args.Term > rf.CurrentTerm {
	// 	reply.Success = true
	// 	rf.CurrentTerm = args.Term
	// 	return
	// }
	// if len(rf.Entries) > 0 && (args.PrevLogTerm < rf.Entries[len(rf.Entries)-1].Term ||
	// 	args.PrevLogTerm == rf.Entries[len(rf.Entries)-1].Term && args.PrevLogIndex < len(rf.Entries)-1) {
	// 	fmt.Printf("-3args prevlogterm %d,localterm %d \n", args.PrevLogTerm, rf.Entries[len(rf.Entries)-1].Term)
	// 	reply.Success = false
	// 	reply.Term = -3
	// 	rf.CurrentTerm = max(rf.CurrentTerm, args.Term)
	// 	rf.VoteFor = -1
	// 	rf.state = 0
	// 	return
	// }

	if args.PrevLogIndex >= 0 && len(rf.Entries) > 0 && (len(rf.Entries) < args.PrevLogIndex+1 || rf.Entries[args.PrevLogIndex].Term != args.PrevLogTerm) {
		// fmt.Printf("-2raft %d be called append entry from raft %d,term %d, leader Term %d, \n", rf.me, args.LeaderId, rf.CurrentTerm, args.Term)
		reply.Success = false
		reply.Term = -2
		rf.AppeFromLeader = true
		rf.CurrentTerm = args.Term
		rf.VoteFor = -1
		rf.state = 0
		return
	}
	if len(args.Entries) == 0 && len(rf.Entries) > 0 && (args.PrevLogTerm < rf.Entries[len(rf.Entries)-1].Term ||
		args.PrevLogTerm == rf.Entries[len(rf.Entries)-1].Term && args.PrevLogIndex < len(rf.Entries)-1) {
		// fmt.Printf("-3args prevlogterm %d,localterm %d \n", args.PrevLogTerm, rf.Entries[len(rf.Entries)-1].Term)
		reply.Success = false
		reply.Term = -3
		rf.CurrentTerm = max(rf.CurrentTerm, args.Term)
		rf.VoteFor = -1
		rf.state = 0
		return
	}
	// if an existing entry conflicts with a new one (same index but different terms).
	// delete the existing entry and all that follow it
	if len(rf.Entries) > args.PrevLogIndex+1 {
		rf.Entries = rf.Entries[:args.PrevLogIndex+1]
	}
	// append any new entries not already in the log
	if len(args.Entries) > 0 {
		rf.Entries = append(rf.Entries, args.Entries...)
	}

	// fmt.Printf("append entry into here\n")
	// if leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if rf.me == 5 {
		fmt.Printf("rf %d: leader commit index %d, rf.commitindex %d, rf.entries.length -1 %d\n",
			rf.me, args.LeaderCommit, rf.CommitIndex, len(rf.Entries)-1)
	}
	if args.LeaderCommit > rf.CommitIndex {
		rf.CommitIndex = min(args.LeaderCommit, len(rf.Entries)-1)
	}
	// fmt.Printf("rf %d return ture\n", rf.me)
	reply.Success = true
	rf.AppeFromLeader = true
	rf.CurrentTerm = args.Term
	rf.VoteFor = -1
	rf.state = 0
	// rf.timer.Stop()
	// go rf.loop()
	// rf.keepAlive()
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
	isLeader := rf.state == 2

	if isLeader == false {
		return index, term, isLeader
	}
	count := 1
	// ccc := 0
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me && rf.ReqForAppEntry[i].Success == true {
			count++
		}
		// if i != rf.me && rf.ReqForAppEntry[i].Term == -3 {
		// ccc++
		// fmt.Printf("rf %d start failed\n", rf.me)
		// }
	}
	// if count == 0 {
	if 2*count < len(rf.peers) {
		isLeader = false
		return index, term, isLeader
	}

	// fmt.Printf("***********************************\n"+
	// 	"raft %d call start()\n"+
	// 	"*************************************\n",
	// 	rf.me)
	rf.mu.Lock()
	index = len(rf.Entries) + 1
	term = rf.CurrentTerm
	newEntry := new(Entry)
	newEntry.Term = term
	newEntry.Index = index
	newEntry.Command = command
	rf.Entries = append(rf.Entries, newEntry)
	rf.mu.Unlock()
	// fmt.Printf("raft %d entries length %d, term %d, command %d\n", rf.me, len(rf.Entries), rf.CurrentTerm, command.(int))
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

	rf.ApplyCh = applyCh
	rf.VoteFor = -1
	// rf.SleepOne = 0
	rf.state = 0
	rf.CurrentTerm = -1
	rf.AppeFromLeader = false
	// 3
	rf.CommitIndex = -1
	rf.LastApplied = -1
	// listen from leader index, initial to 0
	// rf.LeaderApplied = -1
	// rf.NextIndex
	// rf.MatchIndex     []int

	rf.Entries = []*Entry{}
	// rf.NextIndex = []int{0}
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

		rf.NextIndex = append(rf.NextIndex, 0)
		rf.LastNextIndex = append(rf.LastNextIndex, 0)
		rf.MatchIndex = append(rf.MatchIndex, 0)
		rf.LastAppendEnd = append(rf.LastAppendEnd, 0)
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

// type ApplyMsg struct {
// 	Index       int
// 	Command     interface{}
// 	UseSnapshot bool   // ignore for lab2; only used in lab3
// 	Snapshot    []byte // ignore for lab2; only used in lab3
// }
func (rf *Raft) keepAlive() {
	// for all servers:1. increment lastApplied, apply log[lastApplied] to state machine
	// 2. if rpc request or response contains term T > currentTermm convert to follower.
	rf.persist()
	if rf.me > 4 {
		// fmt.Printf("raft %d call keepalive\n", rf.me)
	}
	// for follower
	for rf.CommitIndex > rf.LastApplied && len(rf.Entries) > rf.LastApplied+1 {
		rf.LastApplied++
		// appmsg := ApplyMsg{rf.LastApplied, rf.Entries[rf.LastApplied].Command, false, nil}
		// appmsg.Index = rf.LastApplied
		// appmsg.Command = rf.Entries[rf.LastApplied].Command
		// fmt.Printf("***********************************\n"+
		// 	"raft %d apply command: index %d, term %d\n"+
		// 	"*************************************\n",
		// 	rf.me, rf.LastApplied, rf.Entries[rf.LastApplied].Command)
		// rf.ApplyCh <- appmsg
		// msg := "hi"
		appmsg := ApplyMsg{rf.LastApplied + 1, rf.Entries[rf.LastApplied].Command, false, nil}
		select {
		case rf.ApplyCh <- appmsg:
			// fmt.Printf("rf %d sent message index %d command %d\n", rf.me, rf.LastApplied+1, appmsg.Command.(int))
			// default:
			// fmt.Printf("rf %d no message sent\n", rf.me)
		}
	}
	if rf.state == 0 {
		if rf.AppeFromLeader == false && rf.VoteFor == -1 {
			// if rf.SleepOne >= 1 {
			rf.state = 1
			rf.VoteFor = rf.me
			rf.CurrentTerm = 1 + rf.CurrentTerm //+ rf.me
			rf.RequestForVote()
			// } else {
			// rf.SleepOne++
			// }
		} else {
			// rf.SleepOne = 0
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
					if rf.ReqForVoteReply[i].VoteGranted == false {
						if rf.ReqForVoteReply[i].Term > rf.CurrentTerm {
							rf.CurrentTerm = rf.ReqForVoteReply[i].Term
							rf.state = 0
							rf.VoteFor = -1
							rf.AppeFromLeader = false
							return
						}
						if rf.ReqForVoteReply[i].Term == -2 {
							rf.state = 0
							rf.VoteFor = -1
							rf.AppeFromLeader = true
							return
						}
					}
				}
			}
		}
		if count2 == 1 {
			// rf.CurrentTerm = 0
		}

		// fmt.Printf("########################\n"+
		// 	"raft %d candidation summary: count1, %d, count2, %d\n"+
		// 	"#######################\n",
		// 	rf.me, count, count2)
		if count*2 > count2 && count2 != 1 {
			rf.state = 2
			// fmt.Printf("***** raft %d claim to be leader!count %d, count2 %d, term %d\n", rf.me, count, count2, rf.CurrentTerm)
			for i := 0; i < len(rf.peers); i++ {
				rf.NextIndex[i] = len(rf.Entries)
				rf.LastNextIndex[i] = len(rf.Entries)
				rf.MatchIndex[i] = 0
				// fmt.Printf("***** raft %d for %d, next index %d!\n", rf.me, i, rf.NextIndex[i])
			}
			rf.CurrentTerm++ //+ rf.me

			rf.AppendEntry()
		} else {
			rf.VoteFor = rf.me
			rf.CurrentTerm = 1 + rf.CurrentTerm
			rf.RequestForVote()
		}
	} else if rf.state == 2 {
		count3 := 1
		count4 := 1 // commit replicated on major peers
		m := len(rf.peers)
		for i := 0; i < m; i++ {
			if i != rf.me {
				if rf.AppendEntryBool[i] == true {
					// fmt.Printf("rf %d app reply %v\n", i, rf.ReqForAppEntry[i].Success)
					count3++
					if rf.ReqForAppEntry[i].Success == false {
						// fmt.Printf("rf %d append rf %d failed current term %d, return term %d\n", rf.me, i, rf.CurrentTerm, rf.ReqForAppEntry[i].Term)
						if rf.ReqForAppEntry[i].Term == -2 {
							rf.NextIndex[i] = rf.LastNextIndex[i]
							rf.NextIndex[i]--
						}
						if rf.ReqForAppEntry[i].Term > rf.CurrentTerm {
							rf.CurrentTerm = rf.ReqForAppEntry[i].Term
							rf.state = 0
							rf.VoteFor = -1
							rf.AppeFromLeader = false
							return
						}
						if rf.ReqForAppEntry[i].Term == -3 {
							rf.state = 0
							rf.VoteFor = -1
							rf.AppeFromLeader = false
							return
						}
					} else {
						rf.MatchIndex[i] = rf.NextIndex[i] - 1
					}
				} else {
					rf.NextIndex[i] = rf.LastNextIndex[i]
				}
				// fmt.Printf("leader nextindex for %d is %d\n", i, rf.NextIndex[i])

				if rf.MatchIndex[i] > rf.CommitIndex {
					count4++
				}
			}
		}

		if 2*count4 >= len(rf.peers) {
			rf.CommitIndex++
		}
		// if count3 == 1 {
		// 	rf.state = 1
		// 	rf.CurrentTerm += 1 + rf.me
		// 	rf.RequestForVote()
		// 	fmt.Printf("******* raft %d turn from leader to candidate\n", rf.me)
		// }
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
	lastIndex := -1
	lastTerm := -1

	if len(rf.Entries) > 0 {
		// if rf.CommitIndex > 0 {
		lastIndex = rf.Entries[len(rf.Entries)-1].Index
		lastTerm = rf.Entries[len(rf.Entries)-1].Term
		// lastIndex = rf.Entries[rf.CommitIndex].Index
		// lastTerm = rf.Entries[rf.CommitIndex].Term
	}
	args := RequestVoteArgs{rf.CurrentTerm, rf.me, lastIndex, lastTerm}
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
	// fmt.Printf("raft %d call for append entry\n", rf.me)

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			prevterm := -1
			previndex := -1
			entries := []*Entry{}
			if rf.NextIndex[i] > 0 {
				previndex = rf.NextIndex[i] - 1
				prevterm = rf.Entries[rf.NextIndex[i]-1].Term
			}
			if rf.NextIndex[i] < len(rf.Entries) {
				entries = rf.Entries[rf.NextIndex[i]:]
			}
			args := RequestAppendEntriesArgs{
				rf.CurrentTerm,
				rf.me,
				previndex,
				prevterm,
				entries,
				rf.CommitIndex,
			}
			rf.LastNextIndex[i] = rf.NextIndex[i]
			rf.NextIndex[i] = len(rf.Entries)
			rf.LastAppendEnd[i] = len(rf.Entries)
			go rf.sendRequestAppendEntry(i, args, rf.ReqForAppEntry[i])
		}
	}
}
func (rf *Raft) clearAppendEntry() {
	m := len(rf.peers)
	for i := 0; i < m; i++ {
		rf.AppendEntryBool[i] = false
		rf.ReqForAppEntry[i].Success = false
	}
}

func (rf *Raft) appendEntry2Followers() {

}
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
func max(a, b int) int {
	if a < b {
		return b
	}
	return a
}
