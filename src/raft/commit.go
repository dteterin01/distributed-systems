package raft

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	Snapshot      []byte
	IsUseSnapshot bool
}

//
// commit log [lastApplied + 1, commitIndex]
//
func (rf *Raft) commitLog() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := rf.log[0].Index

	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		msg := ApplyMsg{
			CommandIndex: i,
			CommandValid: true,
			Command:      rf.log[i-index].Command,
		}
		rf.applyChan <- msg
	}
	rf.lastApplied = rf.commitIndex
}
