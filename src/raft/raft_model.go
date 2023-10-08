package raft

import "sync"
import "../labrpc"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//

type State byte

const (
	Leader State = iota
	Follower
	Candidate
)

type LogEntry struct {
	Term    int
	Command interface{}

	Index int
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

	// Your Data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	term     int
	votedFor int
	log      []LogEntry

	commitIndex int
	lastApplied int

	state     State
	voteCount int

	nextIndex  []int
	matchIndex []int

	applyChan       chan ApplyMsg
	chanGrantVote   chan bool
	chanWinElection chan bool
	chanHeartBeat   chan bool
}

func (rf *Raft) changeStateConcurrentlyArgs(
	foo func(var1 int),
	var1 int,
) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	foo(var1)
}

func (rf *Raft) changeStateConcurrently(
	foo func(),
) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	foo()
}

func (rf *Raft) changeStateToCandidate() {
	defer rf.persist()

	rf.term++
	rf.votedFor = rf.me
	rf.voteCount = 1
}

func (rf *Raft) changeToLeaderState() {
	defer rf.persist()

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	nextLogIndex := rf.getLastLogIndex() + 1

	for i := range rf.nextIndex {
		rf.nextIndex[i] = nextLogIndex
	}
}

func (rf *Raft) voteCandidate(candidateId int) {
	defer rf.persist()

	rf.votedFor = candidateId
	rf.chanGrantVote <- true
}

func (rf *Raft) changeToFollowerState(term int) {
	defer rf.persist()

	rf.state = Follower
	rf.votedFor = -1
	rf.term = term
}

func (rf *Raft) isUpToDate(candidateTerm int, candidateIndex int) bool {
	term, index := rf.getLastLogTerm(), rf.getLastLogIndex()
	return candidateTerm > term || (candidateTerm == term && candidateIndex >= index)
}

func (rf *Raft) getLastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) getLastLogIndex() int {
	return rf.log[len(rf.log)-1].Index
}
