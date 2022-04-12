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
	"6.824/labgob"
	"bytes"
	"context"
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
	term                      int // Persistent state on all servers: (Updated on stable storage before responding to RPCs)
	votedFor                  int // candidateId that received vote in current term (or null if none) Persistent state on all servers: (Updated on stable storage before responding to RPCs)
	state                     ServerState
	followerTimeout           time.Duration
	rand                      *rand.Rand
	electionTimeoutTime       time.Time
	heartsBeatDuration        time.Duration
	remoteCallTimeoutDuration time.Duration

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
	} else { // args.Term >= rf.term

		// Leader or Candidate do not vote for others
		if args.Term == rf.term && rf.state != Follower {
			reply.Agree = false
			reply.Term = rf.term
			return
		}
		// [PAPER] Rules for Servers - All servers: 2. If RPC request or response contains term T > currentTerm:
		//set currentTerm = T, convert to follower (§5.1)
		if args.Term > rf.term {
			rf.stepDownAsFollower(args.Term) // also clear up votedFor, as we haven't voted yet seems important
			rf.persist()                     // at RequestVote term updates
		}

		// must be a follower here
		if rf.state != Follower {
			panic("rf must be a Follower in this stage of RequestVote, but it is not.")
		}

		// leader restrictions [PAPER] RequestVote PRC - Receiver Impl 2. candidate’s log has to be
		// least as up-to-date as receiver’s log before grant vote (§5.2, §5.4)
		if (rf.lastLogTerm() > args.LastLogTerm) ||
			(rf.lastLogTerm() == args.LastLogTerm && rf.lastLogIndex() > args.LastLogIndex) {
			reply.Agree = false
			reply.Term = rf.term
			rf.DPrintf(TopicVR, "Leader restriction check did not pass, vote false for %v", args.From)
			return
		}

		// [PAPER] RequestVote PRC - Receiver Impl 2. If votedFor is null or candidateId, grant vote
		notYetVoted := rf.votedFor == -1
		votedTheSameBefore := rf.votedFor == args.From
		if notYetVoted || votedTheSameBefore {
			rf.stepDownAsFollower(args.Term) // reset election timeout so if a follower is not
			rf.votedFor = args.From          // make sure this is set again after the step above
			reply.Agree = true
			reply.Term = rf.term
			rf.persist() // at RequestVote term and votedFor updates
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
	Good         bool // true if follower contained entry matching prevLogIndex and prevLogTerm - TODO
	Term         int
	From         int
	LastLogIndex int // for speed up finding nextIndex
	LastLogTerm  int
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
	reply.From = rf.me

	if args.Term < rf.term {
		// [PAPER] AppendEntries-RPC - Receiver implementation: 1. Reply false if Term < currentTerm (§5.1)
		// allow election timeout later when receiving AppendEntries RPC from low Term calls
		reply.Good = false
		reply.Term = rf.term
		rf.DPrintf(TopicHB, "args.From:%v, args.Term:%v --- Ignored\n", args.From, args.Term)
		return // ignore
	} else { // args.Term >= rf.term
		if rf.state == Leader {
			rf.DPrintf(TopicHB, "!!!!!!! THIS SHOULD RARELY HAPPEN? !!!!!!! 2 leaders exist at the same time? rf.me: %v, rf.term: %v,  args.From: %v, args.Term: %v\n", rf.me, rf.term, args.From, args.Term)
			if rf.term == args.Term {
				rf.DPrintf(TopicHB, "!!!!!!! *** THIS SHOULD RARELY HAPPEN *** !!!!!!! 2 leaders exist at the same time with the same term. Only happens when no log difference on all nodes.\n")
				panic("!!! 2 leaders exist at the same time with the same term!!!")
			}
		}

		// Different with RequestVote, here we immediately refresh timeout
		// as when args.Term >= rf.term then means there is a valid leader exist!
		// No need to do leader log up-to-date check as if leader has some old log it would not be possible
		// to be elected as leader in the first place.
		oldTerm := rf.term
		rf.stepDownAsFollower(args.Term) // refresh election timeout
		rf.votedFor = args.From          // keep votedFor
		if args.Term > oldTerm {
			// rf.tickerContextCancel() // HeatBeat cancel context as args.Term > rf.term
			rf.persist() // at HeartBeat term updates
		}

		// [PAPER] AppendEntries-RPC - Receiver implementation: 2. Reply false if log doesn’t contain an entry at prevLogIndex
		// whose Term matches prevLogTerm (§5.3)
		// First entry case is also covered as rf.lastLogIndex()=0=args.PrevLogIndex and terms are the same as 0
		rf.DPrintf(TopicHB, "--------HeartBeat------ args.PrevLogIndex: %v, args.Entries:%v", args.PrevLogIndex, args.Entries)
		if rf.lastLogIndex() < args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			reply.Good = false
			reply.Term = rf.term

			// In the case of rf.log[args.PrevLogIndex].Term != args.PrevLogTerm, a good way to speed up is
			// delete the follower's log from the end with all the entries that having term as rf.log[args.PrevLogIndex].Term
			// And we should only deal this situation when rf.lastLogIndex() >= args.PrevLogIndex (follower can have much longer logs than args.PrevLogIndex)
			// Otherwise, if we don't do this, args.PrevLogIndex will only be reduced as 1 with each HeartBeat, that is too slow.
			if rf.lastLogIndex() >= args.PrevLogIndex {
				termToDelete := args.PrevLogTerm
				newEnd := args.PrevLogIndex - 1
				for ; newEnd > 0; newEnd-- {
					if rf.log[newEnd].Term <= termToDelete {
						break
					}
				}
				rf.log = rf.log[:newEnd+1] // remove/cut all logs having rf.log[i].Term > termToDelete
				// For the case of when rf.log[newEnd].Term < termToDelete, speed up is handled at the leader side
				// with information of reply.LastLogTerm
			}

			reply.LastLogIndex = rf.lastLogIndex() // speed up
			reply.LastLogTerm = rf.lastLogTerm()

			rf.DPrintf(TopicHB, "false return as prevLogIndex/Term missmatch - args.From:%v, args.Term:%v, rf.lastLogIndex(): %v, prevLogIndex: %v\n",
				args.From, args.Term, rf.lastLogIndex(), args.PrevLogIndex)
			rf.electionTimeoutTime = time.Now().Add(rf.getElectionTimeoutDuration())
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
					rf.log = rf.log[:args.PrevLogIndex+1+i] // here use Index Not Term!...
					rf.persist()                            // at HeartBeat log updates
					// SUBTLE BUG PLACE: when cutting rf.log, rf.nextIndexes[x] needs to be cut as well.
					// This is not really needed for now as when this cut happens the node is already a follower
					// Then rf.nextIndexes should not be used anymore and it always get updated when it steps up as a leader
					//for i, _ := range rf.peers {
					//	if rf.me != i {
					//		rf.nextIndexes[i] = min(rf.nextIndexes[i], rf.lastLogIndex()+1)
					//	}
					//}

					break
				}
			}

			// [PAPER] AppendEntries-RPC - Receiver implementation: 4. Append any new Entries not already in the log
			// leader:       [x, 1, 2, 3, 4] --- args.PrevLogIndex->2, Entries->[3,4]
			// follower now: [x, 1, 2, 3 ] --- rf.lastLogIndex()->3, diff->1
			diff := rf.lastLogIndex() - args.PrevLogIndex

			if diff >= len(args.Entries) {
				// for case like:
				// log: [0, 1, 2, 3, 4, 5, 6] -------------------------> PrevLogIndex=2,
				//               [3, 4,] ------------------------------> len(args.Entries)=2
				//         diff := rf.lastLogIndex() - args.PrevLogIndex = (7-2) = 5 > 2 will cause issue
				// So if after the above "cutting" step results this happens, we just do nothing as the logs has all the same element already.
			} else {
				if diff > 0 {
					fmt.Printf("aaaaaaaaaaaaaaaaaaaaaaa entries has some duplicates append, diff:%v log len:%v\n", diff, len(args.Entries[diff:]))
				}
				rf.log = append(rf.log, args.Entries[diff:]...)
				rf.persist() // at HeartBeat log updates
			}
		}
		// [PAPER] AppendEntries-RPC - Receiver implementation: 5.  If LeaderCommit > commitIndex, set commitIndex = min(LeaderCommit, index of last new entry)
		// Follower commit
		if args.LeaderCommit > rf.commitIndex {
			newCommitIndex := min(args.LeaderCommit, rf.lastLogIndex())
			if rf.commitIndex != newCommitIndex {
				// apply logs async
				rf.commitIndex = newCommitIndex
			}
		}

		rf.stepDownAsFollower(args.Term) // reset election timeout elapses when receiving AppendEntries RPC from current leader (or a new leader with higher Term)
		rf.votedFor = args.From          // make sure this is set again after the step above

		reply.Good = true
		reply.Term = rf.term
		rf.DPrintf(TopicHB, "args.From:%v, args.Term:%v --- Reply Good\n", args.From, args.Term)
		return
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
	rf.DPrintf(TopicStart, "A new command %v comes in\n", command)
	rf.log = append(rf.log, Entry{
		Command: command,
		Term:    rf.term,
	})
	rf.persist() // at Start log updates

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
			rf.mu.Unlock()
			time.Sleep(time.Millisecond * 10) // sleep for a while
			continue
		}

		if rf.state == Leader {
			rf.electionTimeoutTime = time.Now().Add(rf.heartsBeatDuration)
			term := rf.term
			rf.mu.Unlock()
			go rf.tickerAsLeader(term)
			continue
		} else if rf.state == Follower {
			rf.state = Candidate
			rf.votedFor = -1
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

// return true as we can be sure we are still leader
// currentTerm is the term when this leader ticker starts
func (rf *Raft) verifyLeaderState(currentTerm int) bool {
	if rf.term != currentTerm {
		votedFor := rf.votedFor
		rf.stepDownAsFollower(rf.term)
		rf.votedFor = votedFor // preserveVotedFor
		rf.persist()
		return false
	}
	// check whether this candidate has been set back to Follower
	if rf.state == Follower {
		rf.DPrintf(TopicTickerCandidate, "Set back to Follower\n")
		rf.electionTimeoutTime = time.Now().Add(rf.getElectionTimeoutDuration())
		return false
	}
	return true
}

// This method is expected to run async
func (rf *Raft) tickerAsLeader(currentTerm int) {
	// Setup tickerContext
	ctx := context.Background()
	ctx, _ = context.WithTimeout(ctx, time.Millisecond*2000)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// QUESTION - adding this part should avoid 2 leaders exist at the same time, but it fails TestFollowerFailure2B???
	// check term updated asynchronously, if so just return as follower
	// This solves some subtle bug such as 2 leader with the same term exist and ping each other or
	// index range issue as rf is step down as follower, rf.log gets cut, but rf.nextIndexes not changed, letting rf.log[rf.nextIndexes[i]-1] below out of range
	if ok := rf.verifyLeaderState(currentTerm); !ok {
		return
	}

	rf.DPrintf(TopicTickerLeader, "Leader sends heartsbeats to others... \n")
	c1 := make(chan *HeartBeatReply)
	// specify some temp values to hold these index while having the lock, as rf.log might get changed (new command arrives) when unlocked below to allow the Call method runnable in goroutines
	nextIndexesToBe := make([]int, len(rf.peers))
	copy(nextIndexesToBe, rf.nextIndexes)
	matchIndexesToBe := make([]int, len(rf.peers))
	copy(matchIndexesToBe, rf.matchIndexes)

	for i, other := range rf.peers {
		if i != rf.me {
			var args *HeartBeatRequest = &HeartBeatRequest{
				From:         rf.me,
				Term:         currentTerm, // Could be a SUBTLE BUG if rf.term is used, as tickerAsLeader runs async, when we reach here the rf state might already be changed,
				PrevLogIndex: rf.nextIndexes[i] - 1,
				PrevLogTerm:  rf.log[rf.nextIndexes[i]-1].Term, // SUBTLE BUG - as this runs async, rf.log could be cut and
			}
			var reply *HeartBeatReply = &HeartBeatReply{}

			// If last log index ≥ nextIndex for a follower: send
			// AppendEntries RPC with log Entries starting at nextIndex
			//  • If successful: update nextIndex and matchIndex for follower (§5.3)
			//  • If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)
			//
			//  [x, 10, 20, 30]
			//   0,  1,  2,  3  ---- rf.lastLogIndex()->3
			//                  ---- rf.nextIndexesTo[i]->3 (only has 2 element, but by default nextIndex=lastLogIndex+1)
			//                  ---- so args.Entries = rf.log[3, 3+1]
			rf.DPrintf(TopicTickerLeader, "Leader state before sending ------------ rf.lastLogIndex():%v rf.nextIndexes[%v]:%v \n", rf.lastLogIndex(), i, rf.nextIndexes[i])
			if rf.lastLogIndex() >= rf.nextIndexes[i] {
				// limit length of entries
				entryLength := min(128, rf.lastLogIndex()-rf.nextIndexes[i])
				// PITFALL? - try avoid data-race at runtime.slicecopy()
				entryCopy := make([]Entry, entryLength+1)
				copy(entryCopy, rf.log[rf.nextIndexes[i]:rf.nextIndexes[i]+entryLength+1])
				args.Entries = entryCopy
				nextIndexesToBe[i] = rf.nextIndexes[i] + entryLength + 1
				matchIndexesToBe[i] = rf.nextIndexes[i] + entryLength // TODO - is this what to specific for matchIndexesToBe?
			}
			args.LeaderCommit = rf.commitIndex

			go func(target int, peer *labrpc.ClientEnd) {
				ok := peer.Call("Raft.HeartBeat", args, reply)
				if ok {
					if reply.Good {
						c1 <- reply
						return
					}
				}
				c1 <- reply
				rf.mu.Lock()
				rf.DPrintf(TopicTickerLeader, "Leader<%v> sends heartbeat Term %v to Node<%v> failed\n", args.From, args.Term, target)
				rf.mu.Unlock()
			}(i, other)
		}
	}

	rf.mu.Unlock()      // Unlock here to allow the Call method runnable in goroutines
	heartBeatCount := 1 // Vote for myself
	replyCount := 1     // Count it the same way as heartBeat
	for p, _ := range rf.peers {
		if p != rf.me {
			select {
			case reply := <-c1:
				i := reply.From
				rf.mu.Lock()
				// SUBTLE - IMPORTANT - as tickerAsLeader runs async, we have to be sure it is still leader when getting a reply and Lock here again
				if ok := rf.verifyLeaderState(currentTerm); !ok {
					return
				}

				rf.DPrintf(TopicTickerLeader, "Leader call to peer %v replied\n", i)
				replyCount = replyCount + 1
				// For passing critical tests, we wait as long as possible,
				// So comment out ctx cancellation here, otherwise in production system it perhaps better to have this
				//if replyCount >= len(rf.peers)/2+1 {
				//	ctxCancelFunc()
				//}
				if reply.Good {
					// Heartbeat/Commit Successful
					// Only increase those indexes as some old Heartbeat might return slowly or after a newer beat comes back first
					rf.nextIndexes[i] = max(nextIndexesToBe[i], rf.nextIndexes[i])
					rf.matchIndexes[i] = max(matchIndexesToBe[i], rf.matchIndexes[i])
					heartBeatCount = heartBeatCount + 1
					rf.DPrintf(TopicTickerLeader, "Good -------------------------------------rf.nextIndexes[%v]=%v \n", i, rf.nextIndexes[i])
				} else {
					rf.DPrintf(TopicTickerLeader, "Bad --------------------------------------rf.nextIndexes[%v]=%v, reply.LastLogIndex=%v\n", i, rf.nextIndexes[i], reply.LastLogIndex)
					// speed up
					old := rf.nextIndexes[i] - 1
					newNextI := min(reply.LastLogIndex+1, rf.nextIndexes[i]-1)
					for ; newNextI > 1 && rf.log[newNextI].Term > reply.LastLogTerm; newNextI-- {
					}
					rf.nextIndexes[i] = max(1, newNextI)                             // speed up
					rf.nextIndexes[i] = max(rf.nextIndexes[i], rf.matchIndexes[i]+1) // Important, as leader ticker is async, do not reduce nextIndexes if matchIndexes already updated!
					if d := old - rf.nextIndexes[i]; d > 0 {
						fmt.Printf("----------------------- speed up ---------------- nextIndexes old-new :%v\n", d)
					}
					rf.DPrintf(TopicTickerLeader, "Leader call to peer %v reply not good, nextIndex mismatch? New nextIndex[%v]=%v; rf.log[rf.nextIndexes[i]].Term=%v, reply.LastLogTerm=%v\n", i, i, rf.nextIndexes[i], rf.log[rf.nextIndexes[i]-1].Term, reply.LastLogTerm)
				}
				// increase rf.commitIndex as soon as possible in case of some follower is offline for return very slow
				if heartBeatCount >= len(rf.peers)/2+1 {
					rf.checkAndUpdateCommitIndex(currentTerm)
				}
				rf.mu.Unlock()
			//// In practice, we could add timeout here, but for test to pass, we wait indefinitely?
			case <-ctx.Done(): // time.After(rf.remoteCallTimeoutDuration):
				rf.mu.Lock()
				rf.DPrintf(TopicTickerLeader, "Leader call to a peer timeout by rf.tickerContext.Done(), giving up calling\n")
				rf.mu.Unlock()
			}
		}
	}
	rf.mu.Lock()
}

func (rf *Raft) checkAndUpdateCommitIndex(currentTerm int) {

	// [PAPER] Rules for Servers - Leader 4. If there exists an N such that N > commitIndex, a majority
	// of matchIndex[i] ≥ N, and log[N].Term == currentTerm: set commitIndex = N (§5.3, §5.4).
	// We have to use a `validNExist` to firstly find a valid N then do the commit because there could be
	// a special case that some uncommitted logs has a lower term should not be committed if there does not exist
	// an uncommitted log with current term and also replicated. see https://youtu.be/YbZ3zDzDnrw?t=2136
	// For example:
	// If term=3 commitIndex=5, and log=[{<nil> 0} {101 1} {102 1} {103 1} {104 1} {105 1} {106 1}], then {106 1} should not be committed
	// Then later if term=3 commitIndex=5, and log=[{<nil> 0} {101 1} {102 1} {103 1} {104 1} {105 1} {106 1} {106 3}], then {106 1} and {106 3} should both be committed when {106 3} is replicated
	N := rf.commitIndex + 1
	//validNExist := false
	for ; N <= rf.lastLogIndex(); N = N + 1 { // make it more efficient, jump more with N
		numberOfMatchIndexLEtoN := 1
		for i, _ := range rf.peers {
			if i != rf.me && rf.matchIndexes[i] >= N {
				numberOfMatchIndexLEtoN = numberOfMatchIndexLEtoN + 1
			}
		}
		if numberOfMatchIndexLEtoN >= len(rf.peers)/2+1 {
			if rf.log[N].Term == currentTerm {
				//validNExist = true
				rf.commitIndex = N
			}
		} else {
			break
		}
	}
	//if validNExist {
	//	rf.commitIndex = N
	//}
}

func (rf *Raft) sendApplyMsg() {
	for {
		time.Sleep(time.Millisecond * 100)
		rf.mu.Lock()
		toIndex := rf.commitIndex
		rf.mu.Unlock()

		for i := rf.lastApplied + 1; i <= toIndex; i++ {
			// Leader commit
			rf.mu.Lock()
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i].Command,
				CommandIndex: i,
			}
			rf.DPrintf(TopicAsyncCommit, "%v[%v] committed new log on index[%v]\n", rf.state, rf.me, i)
			rf.lastApplied = i
			rf.mu.Unlock()

			rf.applyCh <- msg
		}
	}

}

func (rf *Raft) tickerAsCandidate() {
	//  A candidate continues in this state until one of three things happens:
	//  (a) it wins the election,
	//  (b) another server establishes itself as leader, or
	//  (c) a period of time goes by with no winner

	// Setup tickerContext
	ctx := context.Background()
	ctx, ctxCancelFunc := context.WithTimeout(ctx, time.Millisecond*2000)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.term = rf.term + 1 // --------- only place that bumps Term ------------

	rf.DPrintf(TopicTickerCandidate, "Candidate calls all others for vote\n")
	c1 := make(chan *RequestVoteReply)
	for i, other := range rf.peers {
		if i != rf.me {
			var args *RequestVoteArgs = &RequestVoteArgs{
				From:         rf.me,
				Term:         rf.term,
				LastLogIndex: rf.lastLogIndex(),
				LastLogTerm:  rf.lastLogTerm(),
			}
			var reply *RequestVoteReply = &RequestVoteReply{}
			go func(peer *labrpc.ClientEnd) {
				if ok := peer.Call("Raft.RequestVote", args, reply); ok {
					if reply.Agree {
						c1 <- reply
						return
					}
				}
				c1 <- reply
			}(other)
		}
	}
	currentTerm := rf.term
	rf.mu.Unlock()
	voteCount := 1 // vote for myself
	replyMaxTerm := 0
	for p, _ := range rf.peers {
		if p != rf.me {
			select {
			case reply := <-c1:
				replyMaxTerm = max(replyMaxTerm, reply.Term)
				if reply.Agree {
					voteCount = voteCount + 1
					if voteCount >= len(rf.peers)/2+1 {
						ctxCancelFunc()
					}
				}
			// In practice, we could add timeout here, but for test to pass, we wait indefinitely?
			case <-ctx.Done(): // time.After(rf.remoteCallTimeoutDuration):
				rf.mu.Lock()
				rf.DPrintf(TopicTickerCandidate, "Candidate call for vote to a peer timeout, giving up calling\n")
				rf.mu.Unlock()
			}
		}
	}
	rf.mu.Lock()
	// check termUpdated asynchronously, if so just return as follower
	if replyMaxTerm > currentTerm {
		votedFor := rf.votedFor
		rf.stepDownAsFollower(replyMaxTerm)
		rf.votedFor = votedFor // preserve VotedFor
		rf.persist()
		return
	}
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
				rf.nextIndexes[i] = rf.lastLogIndex() + 1
				rf.matchIndexes[i] = 0
			}
		}
	} else {
		// reset election timer, it is okay to step down here as the election timeout it will be up as a candidate again
		// with a new term
		rf.stepDownAsFollower(rf.term)
	}
}

func (rf *Raft) stepDownAsFollower(term int) {
	rf.state = Follower
	rf.term = max(rf.term, term) // IMPORTANT
	rf.votedFor = -1             // IMPORTANT - need to clean votedFor when step down
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
	rf.followerTimeout = time.Millisecond * 250 // Follower Time Out, should be 2 or 3 times larger than heartsBeatDuration, otherwise seen frequent re-election
	rf.rand = rand.New(rand.NewSource(time.Now().UnixNano()))
	rf.heartsBeatDuration = time.Millisecond * 100 // Heart Beat Duration, seems to be above 100ms to allow test work well otherwise datarace?
	rf.remoteCallTimeoutDuration = time.Millisecond * 2000
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

	// start goroutine to apply messages
	go rf.sendApplyMsg()

	return rf
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:

	if rf.state == Candidate {
		panic("########################## Persist with Candidate state...?")
	}

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.me)
	e.Encode(rf.term)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var me int
	var term int
	var votedFor int
	var log []Entry
	if d.Decode(&me) != nil ||
		d.Decode(&term) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		rf.DPrintf(TopicPersistError, "readPersist failed?!")
	} else {
		rf.me = me
		rf.term = term
		rf.votedFor = votedFor
		rf.log = log
	}
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

// getElectionTimeoutDuration returns a duration between [rf.followerTimeout, rf.followerTimeout + 200)
func (rf *Raft) getElectionTimeoutDuration() time.Duration {
	intn := rf.rand.Intn(300)
	return rf.followerTimeout + time.Millisecond*time.Duration(intn)
}

func (rf *Raft) DPrintf(topic logTopic, format string, a ...interface{}) {
	rfTrace := fmt.Sprintf("Node<%v> [%v]|Term(%v)|VotedFor(%v)|CIndex(%v)|%v",
		rf.me, rf.state, rf.term, rf.votedFor, rf.commitIndex, rf.log)
	rfDebug := fmt.Sprintf("Node<%v> [%v]|Term(%v)|VotedFor(%v)|CIndex(%v)|logLen(%v)",
		rf.me, rf.state, rf.term, rf.votedFor, rf.commitIndex, len(rf.log))
	TPrintf(topic, rfTrace, rfDebug, format, a...)
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
