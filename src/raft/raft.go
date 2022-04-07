package raft

// Raft:
// Committed -> Present in the future leader's logs
//   - Restrictions on commit
//   - Restrictions on leader election

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(Command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
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
// as each Raft peer becomes aware that successive log Entries are
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
	term                  int // Persistent state on all servers: (Updated on stable storage before responding to RPCs)
	votedFor              int // candidateId that received vote in current term (or null if none) Persistent state on all servers: (Updated on stable storage before responding to RPCs)
	state                 ServerState
	followerTimeout       time.Duration
	rand                  *rand.Rand
	electionTimeoutTime   time.Time
	heartsBeatDuration    time.Duration
	candidateStartingTime time.Time

	// for 2B - apply commit
	applyCh chan ApplyMsg
	log     []Entry // log Entries; each entry contains Command for state machine, and term when entry was received by leader (first index is 0 (on paper it is 1))
	// Also is Persistent state on all servers

	// Volatile state on all servers:
	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// Volatile state on Leaders: (Reinitialized after election)
	nextIndexes  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndexes []int // for each server, index of highest log entry known to be replicated on server(initialized to 0, increases monotonically)
}

type Entry struct {
	Command interface{}
	Term    int
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
		return "*Leader* "
	case Follower:
		return "Follower "
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
	From         int
	Term         int
	LastLogIndex int // index of candidate’s last log entry (§5.4) - for leader election restrictions
	LastLogTerm  int // Term of candidate’s last log entry (§5.4) - for leader election restrictions
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Agree bool
	Term  int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.DPrintf(TopicVR, "args.from:%v, args.Term:%v ", args.From, args.Term)
	if args.Term < rf.term {
		reply.Agree = false
		reply.Term = rf.term
		return
	} else {
		if args.Term > rf.term {
			// a new Term comes, setting it as follower
			rf.stepDownAsFollower(args.Term)
		}
		// added a safer check - so to avoid situations like 3 candidate doing triangle voting making 3 leaders?
		if args.Term == rf.term && rf.state != Follower {
			reply.Agree = false
			reply.Term = rf.term
			return
		}
		// must be a follower here
		if rf.state != Follower {
			panic("rf must be a Follower in this stage of RequestVote, but it is not.")
		}

		// leader restrictions [PAPER] RequestVote PRC - Receiver Impl 2.
		if (rf.lastLogTerm() > args.LastLogTerm) ||
			(rf.lastLogTerm() == args.LastLogTerm && rf.lastLogIndex() > args.LastLogIndex) {
			reply.Agree = false
			reply.Term = rf.term
			rf.DPrintf(TopicVR, "Leader restriction check did not pass, vote false for %v", args.From)
			return
		}

		notYetVoted := rf.votedFor == -1
		votedTheSameBefore := rf.votedFor == args.From
		if notYetVoted || votedTheSameBefore {
			rf.stepDownAsFollower(args.Term) // reset election timeout so if a follower is not
			rf.votedFor = args.From          // make sure this is set again after the step above
			reply.Agree = true
			reply.Term = rf.term
			rf.DPrintf(TopicVR, "Vote True for %v", args.From)
		}
	}
}

type HeartBeatRequest struct {
	Term         int     // leader’s Term
	From         int     // so follower can redirect clients
	PrevLogIndex int     // index of log entry immediately preceding new ones - for consistency check
	PrevLogTerm  int     // Term of prevLogIndex entry - for consistency check
	Entries      []Entry // log Entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int     // leader’s commitIndex
}
type HeartBeatReply struct {
	Good bool // true if follower contained entry matching prevLogIndex and prevLogTerm - TODO
	Term int
}

// HeartBeat : While waiting for votes, a candidate may receive an
// AppendEntries RPC from another server claiming to be
// leader. If the leader’s Term (included in its RPC) is at least
// as large as the candidate’s current Term, then the candidate
// recognizes the leader as legitimate and returns to follower
// state. If the Term in the RPC is smaller than the candidate’s
// current Term, then the candidate rejects the RPC and continues in candidate state.
func (rf *Raft) HeartBeat(args *HeartBeatRequest, reply *HeartBeatReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term >= rf.term {
		// [PAPER] AppendEntries-RPC - Receiver implementation: 2. Reply false if log doesn’t contain an entry at prevLogIndex
		// whose Term matches prevLogTerm (§5.3)
		// First entry case is also covered as rf.lastLogIndex()=0=args.PrevLogIndex and terms are the same as 0
		if rf.lastLogIndex() < args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			reply.Good = false
			reply.Term = rf.term
			rf.DPrintf(TopicHB, "false return as prevLogIndex/Term missmatch - args.From:%v, args.Term:%v, rf.lastLogIndex(): %v, prevLogIndex: %v\n",
				args.From, args.Term, rf.lastLogIndex(), args.PrevLogIndex)
			return
		}
		if args.Entries != nil {

			// [PAPER] AppendEntries-RPC - Receiver implementation: 3. If an existing entry conflicts with a new one (same index
			//  but different terms), delete the existing entry and all that follow it (§5.3)
			// leader:       [x, 1, 2, 3, 4] --- args.PrevLogIndex->3
			// follower now: [x, 1, 2, 3]    --- rf.lastLogIndex()->3
			// here we know rf.lastLogIndex() >= args.PrevLogIndex and rf.log[args.PrevLogIndex].Term == args.PrevLogTerm
			for i, v := range rf.log[args.PrevLogIndex+1 : rf.lastLogIndex()+1] {
				if i < len(args.Entries) && v.Term != args.Entries[i].Term {
					rf.log = rf.log[:args.PrevLogTerm+1+i]
					break
				}
			}
			// [PAPER] AppendEntries-RPC - Receiver implementation: 4. Append any new Entries not already in the log
			// leader:       [x, 1, 2, 3, 4] --- args.PrevLogIndex->2, Entries->[3,4]
			// follower now: [x, 1, 2, 3 ] --- rf.lastLogIndex()->3, diff->1
			diff := rf.lastLogIndex() - args.PrevLogIndex
			if diff > 0 {
				fmt.Printf("aaaaaaaaaaaaaaaaaaaaaaa\n")
			}
			for _, v := range args.Entries[diff:] {
				rf.log = append(rf.log, v)
			}
		}
		// [PAPER] AppendEntries-RPC - Receiver implementation: 5.  If LeaderCommit > commitIndex, set commitIndex = min(LeaderCommit, index of last new entry)
		// Follower commit
		if args.LeaderCommit > rf.commitIndex {
			newCommitIndex := min(args.LeaderCommit, rf.lastLogIndex())
			if rf.commitIndex != newCommitIndex {
				startIndex := rf.commitIndex + 1 // start from the new entry after previous committed value
				rf.commitIndex = newCommitIndex
				for i, v := range rf.log[startIndex : newCommitIndex+1] {
					rf.applyCh <- ApplyMsg{
						CommandValid: true,
						Command:      v.Command,
						CommandIndex: startIndex + i,
					}
					rf.DPrintf(TopicTickerLeader, "Follower committed new log with index %v\n", rf.commitIndex+i)
				}
			}
		}

		rf.stepDownAsFollower(args.Term) // reset election timeout elapses when receiving AppendEntries RPC from current leader (or a new leader with higher Term)
		rf.votedFor = args.From          // make sure this is set again after the step above

		reply.Good = true
		reply.Term = rf.term
		rf.DPrintf(TopicHB, "args.From:%v, args.Term:%v --- Reply Good\n", args.From, args.Term)
		return
	} else {
		// [PAPER] AppendEntries-RPC - Receiver implementation: 1. Reply false if Term < currentTerm (§5.1)
		// allow election timeout later when receiving AppendEntries RPC from low Term calls
		reply.Good = false
		reply.Term = rf.term
		rf.DPrintf(TopicHB, "args.From:%v, args.Term:%v --- Ignored\n", args.From, args.Term)
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

// Start the service using Raft (e.g. a k/v server) wants to start
// agreement on the next Command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise, start the
// agreement and return immediately. there is no guarantee that this
// Command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the Command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//
// [PAPER] Rules for Servers - Leaders: 2. If Command received from client: append entry to local log,
// respond after entry applied to state machine (§5.3)
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return -1, -1, false
	}

	// Your code here (2B).
	rf.log = append(rf.log, Entry{
		Command: command,
		Term:    rf.term,
	})

	return rf.lastLogIndex(), rf.term, true
}

// lastLogIndex returns 0 meaning empty log (include an initial dummy entry), otherwise return the last entry index
func (rf *Raft) lastLogIndex() int {
	return len(rf.log) - 1
}

// lastLogTerm returns 0 meaning empty log (include an initial dummy entry), otherwise return the last entry index
func (rf *Raft) lastLogTerm() int {
	index := len(rf.log) - 1
	if index == 0 {
		return index
	}
	return rf.log[index].Term
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
		rf.mu.Lock()
		if time.Now().Before(rf.electionTimeoutTime) { // don't go elect if rf.electionTimeoutTime is after now
			time.Sleep(time.Millisecond * 10) // sleep for a while
			rf.mu.Unlock()
			continue
		}

		if rf.state == Leader {
			rf.mu.Unlock()
			rf.tickerAsLeader()
			continue
		} else if rf.state == Follower {
			rf.term = rf.term + 1 // --------- only place that bumps Term ------------
			rf.state = Candidate
			rf.votedFor = -1
			rf.candidateStartingTime = time.Now()
			rf.DPrintf(TopicTickerFollower, "----- step up as candidate -----\n")
			rf.mu.Unlock()
			continue
		} else if rf.state == Candidate {
			rf.mu.Unlock()
			rf.tickerAsCandidate()
			continue
		}

	}
}

func (rf *Raft) tickerAsLeader() {
	rf.DPrintf(TopicTickerLeader, "Leader sends heartsbeats to others... \n")
	rf.mu.Lock()
	defer rf.mu.Unlock()
	c1 := make(chan *HeartBeatReply)
	nextIndexes := rf.nextIndexes
	matchIndexes := rf.matchIndexes

	for i, other := range rf.peers {
		peer := other // needed for solving data race
		if i != rf.me {
			var args *HeartBeatRequest = &HeartBeatRequest{
				From: rf.me,
				Term: rf.term,
			}
			var reply *HeartBeatReply = &HeartBeatReply{}

			// If last log index ≥ nextIndex for a follower: send
			// AppendEntries RPC with log Entries starting at nextIndex
			//  • If successful: update nextIndex and matchIndex for follower (§5.3)
			//  • If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)
			//
			//  [x, 10, 20, 30]
			//   0,  1,  2,  3  ---- rf.lastLogIndex()->3
			//                  ---- rf.nextIndexes[i]->3 (only has 2 element, but by default nextIndex=lastLogIndex+1)
			//                  ---- so args.Entries = rf.log[3, 3+1]
			if rf.lastLogIndex() >= rf.nextIndexes[i] {
				args.Entries = rf.log[rf.nextIndexes[i] : rf.lastLogIndex()+1]
				args.PrevLogIndex = rf.nextIndexes[i] - 1
				args.PrevLogTerm = rf.log[args.PrevLogIndex].Term

				nextIndexes[i] = rf.lastLogIndex() + 1
				matchIndexes[i] = rf.lastLogIndex() // TODO - is this what to specific for matchIndexes?
			}
			args.LeaderCommit = rf.commitIndex

			go func() {
				if ok := peer.Call("Raft.HeartBeat", args, reply); ok {
					if reply.Good {
						c1 <- reply
						return
					}
				}
				c1 <- reply
				rf.mu.Lock()
				rf.DPrintf(TopicTickerLeader, "Leader %v sends heartbeat Term %v to %v failed\n", args.From, args.Term, i)
				rf.mu.Unlock()
			}()
		}
	}
	currentTerm := rf.term
	rf.mu.Unlock()      // Unlock here to allow the Call method runnable in goroutines
	heartBeatCount := 1 // vote for myself
	for i, _ := range rf.peers {
		if i != rf.me {
			select {
			case reply := <-c1:
				if reply.Term > currentTerm {
					rf.mu.Lock()
					rf.stepDownAsFollower(reply.Term)
					return
				}
				if reply.Good { // Heartbeat/Commit Successful
					rf.nextIndexes[i] = nextIndexes[i]
					rf.matchIndexes[i] = matchIndexes[i]
					heartBeatCount = heartBeatCount + 1
				} else {
					rf.nextIndexes[i] = max(1, rf.nextIndexes[i]-1)
				}
			}
		}
	}
	rf.mu.Lock()

	if heartBeatCount >= len(rf.peers)/2+1 {
		rf.electionTimeoutTime = time.Now().Add(rf.heartsBeatDuration)

		// [PAPER] Rules for Servers - Leader 4. If there exists an N such that N > commitIndex, a majority
		// of matchIndex[i] ≥ N, and log[N].Term == currentTerm: set commitIndex = N (§5.3, §5.4).
		N := rf.commitIndex + 1
		for ; ; N += 1 { // make it more efficient, jump more with N
			numberOfMatchIndexLEtoN := 1
			for i, _ := range rf.peers {
				if i != rf.me && rf.matchIndexes[i] >= N {
					numberOfMatchIndexLEtoN = numberOfMatchIndexLEtoN + 1
				}
			}

			if numberOfMatchIndexLEtoN >= len(rf.peers)/2+1 && rf.log[N].Term == currentTerm {
				// Leader commit
				rf.commitIndex = N
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      rf.log[N].Command,
					CommandIndex: N,
				}
				rf.DPrintf(TopicTickerLeader, "Leader committed new log\n")
			} else {
				break
			}
		}

	} else {
		// leader doesn't get enough heartbeats from most of the candidate, step down
		rf.DPrintf(TopicTickerLeader, "Got heartbeat count: %v - setting back to follower", heartBeatCount)
		rf.stepDownAsFollower(rf.term)
	}
}
func (rf *Raft) tickerAsCandidate() {
	//  A candidate continues in this state until one of three things happens:
	//  (a) it wins the election,
	//  (b) another server establishes itself as leader, or
	//  (c) a period of time goes by with no winner

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if time.Now().After(rf.candidateStartingTime.Add(rf.getElectionTimeoutDuration())) {
		rf.DPrintf(TopicTickerCandidate, "Step down from candidate to follower as timeout\n")
		rf.stepDownAsFollower(rf.term)
		return
	}

	c1 := make(chan *RequestVoteReply)
	for i, other := range rf.peers {
		if i != rf.me {
			peer := other
			var args *RequestVoteArgs = &RequestVoteArgs{
				From:         rf.me,
				Term:         rf.term,
				LastLogIndex: rf.lastLogIndex(),
				LastLogTerm:  rf.lastLogTerm(),
			}
			var reply *RequestVoteReply = &RequestVoteReply{}
			go func() {
				if ok := peer.Call("Raft.RequestVote", args, reply); ok {
					if reply.Agree {
						c1 <- reply
						return
					}
				}
				c1 <- reply
			}()
		}
	}
	currentTerm := rf.term
	rf.mu.Unlock()
	voteCount := 1 // vote for myself
	needToLoop := true
	for i, _ := range rf.peers {
		if i != rf.me && needToLoop {
			select {
			case reply := <-c1:
				if reply.Term > currentTerm {
					rf.mu.Lock()
					rf.stepDownAsFollower(reply.Term)
					return
				}
				if reply.Agree {
					voteCount = voteCount + 1
				}
			}
			if voteCount >= len(rf.peers)/2+1 {
				needToLoop = false // IMPORTANT - do not wait for other remote calls if we already connected enough votes. Otherwise, it makes tests timeout
				continue
			}
		}
	}
	rf.mu.Lock()

	// check whether this candidate has been set back to Follower
	if rf.state == Follower {
		rf.DPrintf(TopicTickerCandidate, "Set back to Follower\n")
		rf.electionTimeoutTime = time.Now().Add(rf.getElectionTimeoutDuration())
		return
	}

	rf.DPrintf(TopicTickerCandidate, "Got vote count %v\n", voteCount)
	if voteCount >= len(rf.peers)/2+1 {
		rf.DPrintf(TopicTickerCandidate, "---L--- Step up as leader\n")
		rf.state = Leader
		for i, _ := range rf.peers {
			if i != rf.me {
				rf.nextIndexes[i] = rf.commitIndex + 1
				rf.matchIndexes[i] = 0
			}
		}
	} else {
		rf.electionTimeoutTime = time.Now().Add(rf.heartsBeatDuration)
	}
}

func (rf *Raft) stepDownAsFollower(term int) {
	rf.state = Follower
	rf.term = term
	rf.votedFor = -1 // IMPORTANT - need to clean votedFor when step down
	rf.electionTimeoutTime = time.Now().Add(rf.getElectionTimeoutDuration())
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
	rf.rand = rand.New(rand.NewSource(time.Now().UnixNano()))
	rf.heartsBeatDuration = time.Millisecond * 100 // Heart Beat Duration
	rf.applyCh = applyCh
	rf.nextIndexes = make([]int, len(peers))
	rf.matchIndexes = make([]int, len(peers))
	rf.log = make([]Entry, 1) // for log entry index start with 1?
	rf.log[0] = Entry{Term: 0}

	rf.stepDownAsFollower(0)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

// getElectionTimeoutDuration returns a duration between [rf.followerTimeout, rf.followerTimeout + 200)
func (rf *Raft) getElectionTimeoutDuration() time.Duration {
	intn := rf.rand.Intn(250)
	return rf.followerTimeout + time.Millisecond*time.Duration(intn)
}

func (rf *Raft) DPrintf(topic logTopic, format string, a ...interface{}) {
	rfTrace := fmt.Sprintf("Node<%v> [%v]|Term(%v)|VotedFor(%v)|CIndex(%v)|%v",
		rf.me, rf.state, rf.term, rf.votedFor, rf.commitIndex, rf.log)
	TPrintf(topic, rfTrace, format, a...)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
