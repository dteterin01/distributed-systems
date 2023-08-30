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

	if args.Term < rf.term {
		reply.Term = rf.term
		reply.Success = false
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

	if args.PrevLogIndex >= 0 && args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
		term := rf.log[args.PrevLogIndex].Term
		for i := args.PrevLogIndex - 1; i >= 0 && rf.log[i].Term == term; i-- {
			reply.TryIndex = i + 1
		}
	} else {
		var restLog []LogEntry
		defer rf.persist()

		rf.log, restLog = rf.log[:args.PrevLogIndex+1], rf.log[args.PrevLogIndex+1:]
		if existConflictingEntry(restLog, args.Log) || len(restLog) < len(args.Log) {
			rf.log = append(rf.log, args.Log...)
		} else {
			rf.log = append(rf.log, restLog...)
		}

		reply.Success = true
		reply.TryIndex = args.PrevLogIndex

		if rf.commitIndex < args.CommitIndex {
			rf.commitIndex = min(args.CommitIndex, rf.getLastLogIndex())
			go rf.commitLog()
		}
	}
}

func existConflictingEntry(localLog []LogEntry, leaderLog []LogEntry) bool {
	for i := 0; i < min(len(leaderLog), len(localLog)); i++ {
		if leaderLog[i].Term != localLog[i].Term {
			return true
		}
	}
	return false
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
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Log)
		rf.nextIndex[server] = rf.matchIndex[server] + 1
	} else {
		rf.nextIndex[server] = reply.TryIndex
	}

	rf.commitApplicableLogs()

	return ok
}

func (rf *Raft) commitApplicableLogs() {
	for possibleIndex := rf.getLastLogIndex(); possibleIndex > rf.commitIndex && rf.log[possibleIndex].Term == rf.term; possibleIndex-- {
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

	for server := range rf.peers {
		if server != rf.me && rf.state == Leader {
			args := rf.buildHeartBeatRequest(server)
			go rf.sendAppendEntries(server, args, &AppendEntriesReply{})
		}
	}
}

func (rf *Raft) buildHeartBeatRequest(server int) *AppendEntriesArgs {
	args := &AppendEntriesArgs{
		Term:         rf.term,
		LeaderId:     rf.me,
		PrevLogIndex: rf.nextIndex[server] - 1,
		CommitIndex:  rf.commitIndex,
	}

	if args.PrevLogIndex >= 0 {
		args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
	}

	if rf.nextIndex[server] <= rf.getLastLogIndex() {
		args.Log = rf.log[rf.nextIndex[server]:]
	}

	return args
}
