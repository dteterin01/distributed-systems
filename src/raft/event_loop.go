package raft

import (
	"math/rand"
	"time"
)

const RandomWaitIncrement int32 = 1000
const Wait int32 = 500

const RandomWaitElectionIncrement int32 = 400
const WaitElection int32 = 300

const BroadcastHeartBeat = 200

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
			rf.changeStateConcurrently(rf.changeStateToCandidate)
			go rf.broadcastRequestVote()
			select {
			case <-rf.chanHeartBeat:
				rf.state = Follower
			case <-rf.chanWinElection:
				rf.changeStateConcurrently(rf.changeToLeaderState)
			case <-time.After(time.Millisecond * time.Duration(rand.Int31()%RandomWaitElectionIncrement+WaitElection)):
			}
		case Leader:
			go rf.broadcastAppendEntries()
			time.Sleep(time.Millisecond * BroadcastHeartBeat)
		}
	}
}
