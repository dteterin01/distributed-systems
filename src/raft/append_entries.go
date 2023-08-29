package raft

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
	Log      []LogEntry

	// for second lab
	PrevLogIndex int
	PrevLogTerm  int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
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
		rf.state = Follower
		rf.term = args.Term
		rf.votedFor = -1
	}

	rf.chanHeartBeat <- true
	reply.Success = true
	reply.Term = rf.term
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !ok || rf.state != Leader || args.Term != rf.term {
		return ok
	}

	if rf.term < reply.Term {
		rf.term = reply.Term
		rf.state = Follower
		rf.votedFor = -1
		return ok
	}
	return ok
}

func (rf *Raft) broadcastHeartbeat() {
	args := rf.buildHeartBeatRequest()
	for peer := range rf.peers {
		if peer != rf.me && rf.state == Leader {
			go rf.sendAppendEntries(peer, args, &AppendEntriesReply{})
		}
	}
}

func (rf *Raft) buildHeartBeatRequest() *AppendEntriesArgs {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return &AppendEntriesArgs{
		Term:     rf.term,
		LeaderId: rf.me,
	}
}
