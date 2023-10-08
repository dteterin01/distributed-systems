package raft

import (
	"bytes"
)
import "../labgob"

type Snapshot struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type SnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *Snapshot, reply *SnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.term {
		reply.Term = rf.term
	}

	if args.Term > rf.term {
		// become follower and update current Term
		rf.changeToFollowerState(args.Term)
	}

	// confirm heartbeat to refresh timeout
	rf.chanHeartBeat <- true

	reply.Term = rf.term

	if args.LastIncludedIndex > rf.commitIndex {
		rf.log = rf.trimLog(args.LastIncludedIndex, args.LastIncludedTerm)
		rf.lastApplied = args.LastIncludedIndex
		rf.commitIndex = args.LastIncludedIndex
		rf.persister.SaveStateAndSnapshot(rf.getRaftState(), args.Data)

		msg := ApplyMsg{IsUseSnapshot: true, Snapshot: args.Data}

		rf.applyChan <- msg
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *Snapshot, reply *SnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !ok || rf.state != Leader || args.Term != rf.term {
		// invalid request
		return ok
	}
	if reply.Term > rf.term {
		rf.changeToFollowerState(reply.Term)
		return ok
	}

	rf.nextIndex[server] = args.LastIncludedIndex + 1
	rf.matchIndex[server] = args.LastIncludedIndex
	return ok
}

func (rf *Raft) buildSnapshotRequest(snapshot []byte) *Snapshot {
	args := &Snapshot{}
	args.Term = rf.term
	args.LeaderId = rf.me
	args.LastIncludedIndex = rf.log[0].Index
	args.LastIncludedTerm = rf.log[0].Term
	args.Data = snapshot
	return args
}

func (rf *Raft) persistState() {
	data := rf.getRaftState()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) CreateSnapshot(kvSnapshot []byte, index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	baseIndex, lastIndex := rf.log[0].Index, rf.getLastLogIndex()
	if index <= baseIndex || index > lastIndex {
		return
	}

	rf.log = rf.trimLog(index, rf.log[index-baseIndex].Term)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.log[0].Index)
	e.Encode(rf.log[0].Term)
	snapshot := append(w.Bytes(), kvSnapshot...)

	rf.persister.SaveStateAndSnapshot(rf.getRaftState(), snapshot)
}

func (rf *Raft) recoverFromSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	var lastIncludedIndex, lastIncludedTerm int
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	d.Decode(&lastIncludedIndex)
	d.Decode(&lastIncludedTerm)

	rf.lastApplied = lastIncludedIndex
	rf.commitIndex = lastIncludedIndex
	rf.log = rf.trimLog(lastIncludedIndex, lastIncludedTerm)

	msg := ApplyMsg{IsUseSnapshot: true, Snapshot: snapshot}

	rf.applyChan <- msg
}

func (rf *Raft) trimLog(lastIncludedIndex int, lastIncludedTerm int) []LogEntry {
	newLog := make([]LogEntry, 0)
	newLog = append(newLog, LogEntry{Index: lastIncludedIndex, Term: lastIncludedTerm})

	for i := len(rf.log) - 1; i >= 0; i-- {
		if rf.log[i].Index == lastIncludedIndex && rf.log[i].Term == lastIncludedTerm {
			newLog = append(newLog, rf.log[i+1:]...)
			break
		}
	}
	return newLog
}
