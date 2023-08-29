package raft

type RequestVoteArgs struct {
	Term        int
	CandidateId int

	//unused before second lab
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false
	reply.Term = rf.term

	if args.Term >= rf.term {
		if args.Term > rf.term {
			rf.state = Follower
			rf.votedFor = -1
			rf.term = args.Term
		}

		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			reply.VoteGranted = true
			reply.Term = args.Term
			rf.votedFor = args.CandidateId
			rf.chanGrantVote <- true
		}
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

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !ok || rf.state != Candidate || rf.term != args.Term {
		return ok
	}

	if reply.VoteGranted {
		rf.voteCount++
		if rf.voteCount > len(rf.peers)/2 {
			rf.state = Leader
			rf.chanWinElection <- true
		}
	}

	if rf.term < reply.Term {
		rf.term = reply.Term
		rf.state = Follower
		rf.votedFor = -1
	}

	return ok
}

func (rf *Raft) broadcastRequestVote() {
	args := rf.buildRequestVote()
	for peer := range rf.peers {
		if peer != rf.me && rf.state == Candidate {
			go rf.sendRequestVote(peer, args, &RequestVoteReply{})
		}
	}
}

func (rf *Raft) buildRequestVote() *RequestVoteArgs {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return &RequestVoteArgs{
		Term:        rf.term,
		CandidateId: rf.me,
	}
}
