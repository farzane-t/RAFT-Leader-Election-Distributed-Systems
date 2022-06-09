package raft

// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new Log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the Log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.

import (
	"log"
	"sync"
	"math/rand"
	"labrpc"
	"time"
)



type LogEntry struct {
	LogTerm int
	LogComd interface{}
	LogIndex int
}

//
// as each Raft peer becomes aware that successive Log entries are
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
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	state  string
	// Your daa here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentLeader int
	Votefor int
	CurrentTerm int
	Log []LogEntry
	voteCount int
	LastLeaderNotify time.Time
	applyCh chan ApplyMsg

}
// return CurrentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	rf.mu.Lock()
	term = rf.CurrentTerm
	isleader = false
	if rf.state == "leader"{
		isleader = true
	}
	rf.mu.Unlock()
	return term, isleader
}


// restore previously persisted state.


type RequestVoteArgs struct {
	Term int //candidate?s term
	CandidateId int //candidate requesting vote

}

type RequestVoteReply struct {
	Term int
	VoteGranted bool
}

type AppendEntries struct {
	Term int
	LeaderID int
	PrevLogIndex int
	PrevLogTerm int
	Entries [] LogEntry
	LeaderCommit int
}

type AppendReply struct {
	Term int
	Success bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.VoteGranted=false
	if(rf.CurrentTerm > args.Term  ){
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		log.Print("server id ",rf.me," dont vote to ",args.CandidateId," :(")
		return
	}
	reply.Term = args.Term
	if rf.Votefor !=-1{
		reply.VoteGranted = false
		return
	}
	rf.state = "follower"

	rf.Votefor = args.CandidateId
	rf.LastLeaderNotify = time.Now()
	reply.VoteGranted=true
	log.Print("server id ",rf.me,"  vote to ",args.CandidateId," :)")
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's Log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft Log, since the leader
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
	return index, term, isLeader

}

func (rf *Raft) becomeLeader() {
	rf.state = "leader"
}
func (rf *Raft) setTerm(term int) {
	rf.CurrentTerm = term
}


//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	rf.mu.Lock()
	rf.state = "NoState"
	rf.Votefor = 0
	rf.voteCount = 0
	rf.CurrentTerm =0

	rf.mu.Unlock()
}

func (rf * Raft) Heartbeat(args *AppendEntries , reply *AppendReply){

	rf.mu.Lock()
	defer rf .mu.Unlock()
	if args.Term < rf.CurrentTerm {

		reply.Term = rf.CurrentTerm
		reply.Success = false
		rf.Votefor =-1
		return
	} else  {

		rf.state = "follower"

		rf.Votefor = -1
		if rf.CurrentTerm < args.Term {
			rf.CurrentTerm = args.Term

		}
		reply.Term = args.Term
		rf.voteCount = 0
		rf.LastLeaderNotify = time.Now()
		return
	}
}



func (rf *Raft) sendHeartbeat( i int , ch chan string ){
	var server int=i

	if rf.state != "leader" {
		rf.mu.Unlock()

		ch <- "candidate"

		return
	}

	for {
		heartBeat := &AppendEntries{
			Term: rf.CurrentTerm ,
			LeaderID :rf.me  ,
		}
		reply := &AppendReply{}
		timer := make(chan bool, 1)
		rf.mu.Unlock()
		go func() {

			ok :=  rf.peers[server].Call("Raft.Heartbeat", heartBeat, reply)
			timer <- ok
			log.Print("server id ",server," recieve heartbeat :)")
		}()

		select {

		case <- time.After(time.Millisecond * 150):
			rf.mu.Lock()
			rf.Votefor = -1
			rf.mu.Unlock()
			ch <- "candidate"
			return
		}
	}
}




func(rf *Raft) election(){
    log.Print("start election function!")
	rf.mu.Lock()
	rf.state = "candidate"
	log.Print("server id ",rf.me," Change state from follower to candidate")
	rf.setTerm(rf.CurrentTerm + 1)
	rf.Votefor = rf.me
	rf.voteCount = 1 				//vote myself
	majorityVoters := len(rf.peers) / 2 +1
	args := &RequestVoteArgs{
		rf.CurrentTerm , rf.me}
	rf.mu.Unlock()
    log.Print("candidate send request vote to servers!")
	for i:= 0 ; i < len(rf.peers) ; i++ {
		if i == rf.me  {
			continue
		}
		go func(args *RequestVoteArgs, indx int, majorityVoters int){
			
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(indx , args , reply)
			if ok && reply.VoteGranted == true {
				rf.mu.Lock()
				rf.voteCount ++
				rf.mu.Unlock()
			}

			rf.mu.Lock()
			if rf.voteCount >= majorityVoters {

				rf.becomeLeader()
				log.Print("server id ",rf.me," become leader!")
				//rf.state = "leader"

				rf.Votefor = -1
				rf.voteCount = 0
				rf.mu.Unlock()
				for rf.state == "leader"{
					type ch chan string
					channels := make([]ch,0)
                    log.Print("leader send heartbeat .................")
					for i:= 0 ; i < len(rf.peers) ;i++{
						rf.mu.Lock()
						if i== rf.me{
							rf.mu.Unlock()
							continue
						}

						tempCh := make(chan string)
						channels = append(channels , tempCh)
						go  rf.sendHeartbeat( i , tempCh)
					}
					for i:= 0 ; i < len(channels) ; i++ {
                        channels[i]<-"leader"
						//temp :=	<- channels[i]
						//temp = "leader"


					}
					rf.mu.Lock()
					if rf.state == "leader"{
						rf.mu.Unlock()
						time.Sleep( 100* time.Millisecond)
					}else {
						rf.mu.Unlock()
					}
				}

			}else{
				rf.mu.Unlock()
			}
		}(args , i , majorityVoters)
	}
	rf.Votefor = -1
}


func( rf *Raft) weatherDo(){
    log.Print("start check weather do election or not!!depend on lastLeaderNotify time!")
	for {
		rnd := rand.Int63n(150)
		rnd  += 150

		rf.mu.Lock()
		rf.Votefor = -1
		rf.mu.Unlock()
		time.Sleep( time.Duration(rnd) * time.Millisecond)
		rf.mu.Lock()

		if time.Since(rf.LastLeaderNotify).Nanoseconds()/1000000 > rnd && rf.state != "leader" {
			rf.LastLeaderNotify = time.Now()
			rf.mu.Unlock()
			go rf.election()

		}	else{
			rf.mu.Unlock()
		}
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
		log.Print("start make successfully!")
	rf := &Raft{}
	rf.mu.Lock()
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.state = "follower"
	rf.CurrentTerm = 0
	rf.Votefor = -1
	rf.voteCount = 0
	rf.applyCh = applyCh
	rf.LastLeaderNotify = time.Now()
	rf.mu.Unlock()
	log.Print("initiallize rf *Raft stuct variables successfully in Make!")

	go rf.weatherDo()

	return rf
}
