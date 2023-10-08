package raft

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
	Log      []LogEntry

	// for second lab
	PrevLogIndex int
	PrevLogTerm  int
	CommitIndex  int
}

type AppendEntriesReply struct {
	Term     int
	Success  bool
	TryIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Success = false

	if args.Term < rf.term {
		reply.Term = rf.term
		reply.TryIndex = rf.getLastLogIndex() + 1
		return
	}

	if args.Term > rf.term {
		rf.changeToFollowerState(args.Term)
	}

	rf.chanHeartBeat <- true
	reply.Term = rf.term

	if args.PrevLogIndex > rf.getLastLogIndex() {
		reply.TryIndex = rf.getLastLogIndex() + 1
		return
	}

	index := rf.log[0].Index

	if args.PrevLogIndex >= index && args.PrevLogTerm != rf.log[args.PrevLogIndex-index].Term {
		term := rf.log[args.PrevLogIndex-index].Term
		for i := args.PrevLogIndex - 1; i >= index; i-- {
			if rf.log[i-index].Term != term {
				reply.TryIndex = i + 1
				break
			}
		}
	} else if args.PrevLogIndex >= index-1 {
		rf.log = rf.log[:args.PrevLogIndex-index+1]
		rf.log = append(rf.log, args.Log...)

		reply.Success = true
		reply.TryIndex = args.PrevLogIndex + len(args.Log)

		if rf.commitIndex < args.CommitIndex {
			rf.commitIndex = min(args.CommitIndex, rf.getLastLogIndex())
			go rf.commitLog()
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !ok || rf.state != Leader || args.Term != rf.term {
		return ok
	}

	if reply.Term > rf.term {
		rf.changeToFollowerState(reply.Term)
		return ok
	}

	if reply.Success {
		if len(args.Log) > 0 {
			rf.nextIndex[server] = args.Log[len(args.Log)-1].Index + 1
			rf.matchIndex[server] = rf.nextIndex[server] - 1
		}
	} else {
		rf.nextIndex[server] = min(reply.TryIndex, rf.getLastLogIndex())
	}

	rf.commitApplicableLogs()

	return ok
}

func (rf *Raft) commitApplicableLogs() {
	index := rf.log[0].Index

	for possibleIndex := rf.getLastLogIndex(); possibleIndex > rf.commitIndex && rf.log[possibleIndex-index].Term == rf.term; possibleIndex-- {
		count := 1
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= possibleIndex {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = possibleIndex
			go rf.commitLog()
			break
		}
	}
}

func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var snapshot []byte

	baseIndex := rf.log[0].Index

	for server := range rf.peers {
		if server != rf.me && rf.state == Leader {
			if rf.nextIndex[server] > baseIndex {
				args := rf.buildHeartBeatRequest(server, baseIndex)
				go rf.sendAppendEntries(server, args, &AppendEntriesReply{})
			} else {
				if snapshot == nil {
					snapshot = rf.persister.ReadSnapshot()
				}
				args := rf.buildSnapshotRequest(snapshot)
				go rf.sendInstallSnapshot(server, args, &SnapshotReply{})
			}

		}
	}
}

func (rf *Raft) buildHeartBeatRequest(server int, index int) *AppendEntriesArgs {
	args := &AppendEntriesArgs{}
	args.Term = rf.term
	args.LeaderId = rf.me
	args.PrevLogIndex = rf.nextIndex[server] - 1
	if args.PrevLogIndex >= index {
		args.PrevLogTerm = rf.log[args.PrevLogIndex-index].Term
	}
	if rf.nextIndex[server] <= rf.getLastLogIndex() {
		args.Log = rf.log[rf.nextIndex[server]-index:]
	}
	args.CommitIndex = rf.commitIndex

	return args
}
