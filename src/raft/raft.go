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
	"fmt"
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// for leader election
	term                  int
	state                 ServerState
	followerTimeout       time.Duration
	rand                  *rand.Rand
	electionTimeoutTime   time.Time
	heartsBeatDuration    time.Duration
	candidateStartingTime time.Time
	votedFor              int
}

type ServerState int

const (
	Leader ServerState = iota
	Follower
	Candidate
)

func (s ServerState) String() string {
	switch s {
	case Leader:
		return "Leader"
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	default:
		return fmt.Sprintf("%d", int(s))
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A).
	return rf.term, rf.state == Leader
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
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	From int
	Term int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Agree bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Printf("Node %v - Got **VoteRequest** from %v - term: %v, currentTerm: %v, votedFor: %v state: %v\n", rf.me, args.From, args.Term, rf.term, rf.votedFor, rf.state)
	if args.Term < rf.term {
		reply.Agree = false
		return
	}
	if args.Term == rf.term {
		if rf.state == Leader {
			reply.Agree = false
			return
		}
		if rf.state == Candidate {
			reply.Agree = false
			return
		}
		// must be follower below
	}
	// a new term comes, setting it as follower
	rf.state = Follower
	// when voting, do increase term?
	rf.term = args.Term
	rf.electionTimeoutTime = time.Now().Add(rf.getElectionTimeoutDuration()) // sleep after vote
	if rf.votedFor == -1 || rf.votedFor == args.From {
		rf.votedFor = args.From
		reply.Agree = true
		fmt.Printf("Node %v - Got **VoteRequest** from %v and vote True, votedFor: %v \n", rf.me, args.From, rf.votedFor)
	} else {
		reply.Agree = false
		fmt.Printf("Node %v - Got **VoteRequest** from %v and vote False, votedFor: %v \n", rf.me, args.From, rf.votedFor)
	}
	return
}

type HeartBeatRequest struct {
	From int
	Term int
}
type HeartBeatReply struct {
	Good bool
}

func (rf *Raft) HeartBeat(args *HeartBeatRequest, reply *HeartBeatReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// fmt.Printf("%v Got heatsBeat from %v - term: %v, currentTerm: %v, state: %v\n", rf.me, args.From, args.Term, rf.term, rf.state)
	// While waiting for votes, a candidate may receive an
	// AppendEntries RPC from another server claiming to be
	// leader. If the leader’s term (included in its RPC) is at least
	// as large as the candidate’s current term, then the candidate
	// recognizes the leader as legitimate and returns to follower
	// state. If the term in the RPC is smaller than the candidate’s
	// current term, then the candidate rejects the RPC and continues in candidate state.
	if args.Term >= rf.term {
		rf.state = Follower
		rf.votedFor = args.From
		rf.term = args.Term
		rf.electionTimeoutTime = time.Now().Add(rf.getElectionTimeoutDuration())
		reply.Good = true
		fmt.Printf("Node %v -> [HeatsBeat] from %v - term: %v, currentTerm: %v, state: %v, --- Reply to %v\n", rf.me, args.From, args.Term, rf.term, rf.state, args.From)
		return
	} else {
		reply.Good = false
		fmt.Printf("Node %v -> [HeatsBeat] from %v - term: %v, currentTerm: %v, state: %v, --- Ignore to %v\n", rf.me, args.From, args.Term, rf.term, rf.state, args.From)
		return // ignore
	}
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
	isLeader := true

	// Your code here (2B).

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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {

	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		// t := <-rf.electionTimeoutTime.C // wait to check state...
		rf.mu.Lock()
		if time.Now().Before(rf.electionTimeoutTime) { // don't go elect if rf.electionTimeoutTime is after now
			time.Sleep(time.Millisecond * 30) // sleep for a while
			rf.mu.Unlock()
			continue
		}

		fmt.Printf("Node %v as %v - DEBUG ticker - term: %v ;\n", rf.me, rf.state, rf.term)
		if rf.state == Leader {
			fmt.Printf("Leader %v sends heartsbeat to others... \n", rf.me)

			var wg sync.WaitGroup
			c1 := make(chan int)

			for i, other := range rf.peers {
				peer := other // needed for solving datarace
				if i != rf.me {
					wg.Add(1)
					var args *HeartBeatRequest = &HeartBeatRequest{
						From: rf.me,
						Term: rf.term,
					}
					var reply *HeartBeatReply = &HeartBeatReply{}
					go func() {
						defer wg.Done()
						if ok := peer.Call("Raft.HeartBeat", args, reply); ok {
							if reply.Good {
								c1 <- 1
								return
							}
						}
						c1 <- 0
						fmt.Printf("Leader %v sends heartsbeat to %v failed\n", rf.me, i)
					}()
				}
			}
			rf.mu.Unlock()

			heartBeatCount := 1 // vote for myself
			for i, _ := range rf.peers {
				if i != rf.me {
					select {
					case vote := <-c1:
						heartBeatCount = heartBeatCount + vote
					}
				}
			}
			wg.Wait()
			rf.mu.Lock()

			if heartBeatCount >= len(rf.peers)/2+1 {
				fmt.Printf("Leader %v got heartbeat count: %v - keep being leader\n", rf.me, heartBeatCount)
				rf.electionTimeoutTime = time.Now().Add(rf.heartsBeatDuration)
			} else {
				// leader doesn't get enough heartbeats from most of the candidate, step down
				fmt.Printf("Leader %v got heartbeat count: %v - setting back to follower\n", rf.me, heartBeatCount)
				rf.state = Follower
				rf.votedFor = -1
				rf.electionTimeoutTime = time.Now().Add(rf.getElectionTimeoutDuration())
			}
			rf.mu.Unlock()
			continue
		} else if rf.state == Follower {
			rf.term = rf.term + 1 // --------- only place that bumps term ------------
			rf.state = Candidate
			rf.votedFor = -1
			rf.candidateStartingTime = time.Now()
			fmt.Printf("----- %v step up as candidate -----\n", rf.me)
		} else if rf.state == Candidate {
			//  A candidate continues in this state until one of three things happens:
			//  (a) it wins the election,
			//  (b) another server establishes itself as leader, or
			//  (c) a period of time goes by with no winner

			if time.Now().After(rf.candidateStartingTime.Add(rf.getElectionTimeoutDuration())) {
				fmt.Printf("----- %v step dowm from candidate to follower as timeout -----\n", rf.me)
				rf.state = Follower
				rf.votedFor = -1
				rf.electionTimeoutTime = time.Now().Add(rf.getElectionTimeoutDuration())
				rf.mu.Unlock()
				continue
			}

			var wg sync.WaitGroup
			c1 := make(chan int)
			for i, other := range rf.peers {
				if i != rf.me {
					peer := other
					wg.Add(1)
					var args *RequestVoteArgs = &RequestVoteArgs{
						From: rf.me,
						Term: rf.term,
					}
					var reply *RequestVoteReply = &RequestVoteReply{}
					go func() {
						defer wg.Done()
						//fmt.Printf("--- go func --- start \n")
						if ok := peer.Call("Raft.RequestVote", args, reply); ok {
							if reply.Agree {
								c1 <- 1
								//fmt.Printf("--- go func --- end \n")
								return
							}
						}
						c1 <- 0
						// fmt.Printf("--- go func --- end \n")
					}()
				}
			}
			rf.mu.Unlock()

			voteCount := 1 // vote for myself
			for i, _ := range rf.peers {
				if i != rf.me {
					select {
					case vote := <-c1:
						voteCount = voteCount + vote
					}
				}
			}

			wg.Wait()
			rf.mu.Lock()

			if rf.state == Follower { // check whether this candidate has been set back to Follower
				rf.electionTimeoutTime = time.Now().Add(rf.getElectionTimeoutDuration())
				rf.mu.Unlock()
				continue
			}

			fmt.Printf("Node %v got vote count %v\n", rf.me, voteCount)
			if voteCount >= len(rf.peers)/2+1 {
				fmt.Printf("------L------ Setting %v as Leader\n", rf.me)
				rf.state = Leader
			} else {
				rf.electionTimeoutTime = time.Now().Add(rf.getElectionTimeoutDuration())
			}
		}
		rf.mu.Unlock()
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.followerTimeout = time.Millisecond * 150 // Follower Time Out
	rf.state = Follower
	rf.votedFor = -1
	rf.rand = rand.New(rand.NewSource(time.Now().UnixNano()))
	rf.electionTimeoutTime = time.Now().Add(rf.getElectionTimeoutDuration())
	rf.heartsBeatDuration = time.Millisecond * 100 // Heart Beat Duration
	rf.term = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func (rf *Raft) getElectionTimeoutDuration() time.Duration {
	intn := rf.rand.Intn(240)
	// fmt.Printf("intn-------%v \n", intn)
	return rf.followerTimeout + time.Millisecond*time.Duration(intn)
}
