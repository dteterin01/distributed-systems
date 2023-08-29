package raft

import (
	"math/rand"
	"time"
)

const RandomWaitIncrement int32 = 400
const Wait int32 = 300

const RandomWaitElectionIncrement int32 = 400
const WaitElection int32 = 300

const BroadcastHeartBeat = 60

func (rf *Raft) eventLoop() {
	for !rf.killed() {
		switch rf.state {
		case Follower:
			select {
			case <-rf.chanGrantVote:
			case <-rf.chanHeartBeat:
			case <-time.After(time.Millisecond * time.Duration(rand.Int31()%RandomWaitIncrement+Wait)):
				rf.state = Candidate
			}
		case Candidate:
			rf.changeStateToCandidate()
			go rf.broadcastRequestVote()

			select {
			case <-rf.chanHeartBeat:
				rf.state = Follower
			case <-rf.chanWinElection:
				rf.changeStageToLeader()
			case <-time.After(time.Millisecond * time.Duration(rand.Int31()%RandomWaitElectionIncrement+WaitElection)):
			}
		case Leader:
			go rf.broadcastHeartbeat()
			time.Sleep(time.Microsecond * BroadcastHeartBeat)
		}
	}
}

func (rf *Raft) changeStageToLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = Leader
}

func (rf *Raft) changeStateToCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.term++
	rf.votedFor = rf.me
	rf.voteCount = 1
}
