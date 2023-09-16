package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	GET    = "GET"
	PUT    = "PUT"
	APPEND = "APPEND"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	// command info
	Op    string // get | put | append
	Key   string
	Value string

	// client info
	RequestId int64
	ClientId  int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvStorage     map[string]string
	lastRequestId map[int64]int64
	operationChan map[int]chan Op // op by raft log index
}

func (kv *KVServer) appendOperationToEntry(op Op) bool {
	idx, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return false
	}

	kv.mu.Lock()
	ch, ok := kv.operationChan[idx]
	if !ok {
		ch = make(chan Op, 1)
		kv.operationChan[idx] = ch
	}
	kv.mu.Unlock()

	select {
	case appliedRaftEntry := <-ch:
		return op == appliedRaftEntry
	case <-time.After(1000 * time.Millisecond):
		return false
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	entry := Op{}
	entry.Op = GET
	entry.Key = args.Key

	entry.ClientId = args.ClientId
	entry.RequestId = args.RequestId

	ok := kv.appendOperationToEntry(entry)
	if !ok {
		reply.Err = ErrWrongLeader
		return
	}
	reply.Err = OK

	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.kvStorage[args.Key]
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	entry := Op{}
	entry.Op = args.Op
	entry.Key = args.Key
	entry.Value = args.Value

	entry.ClientId = args.ClientId
	entry.RequestId = args.RequestId

	ok := kv.appendOperationToEntry(entry)
	if !ok {
		reply.Err = ErrWrongLeader
	} else {
		reply.Err = OK
	}
}

func (kv *KVServer) stateMachine(op Op) {
	switch op.Op {
	case PUT:
		kv.kvStorage[op.Key] = op.Value
	case APPEND:
		kv.kvStorage[op.Key] += op.Value
	}
}

func (kv *KVServer) deduplicate(op Op) bool {
	lastSavedRid, ok := kv.lastRequestId[op.ClientId]
	if ok {
		return lastSavedRid >= op.RequestId
	}
	return false
}

func (kv *KVServer) kernelEventLoop() {
	for {
		msg := <-kv.applyCh
		op := msg.Command.(Op)

		kv.mu.Lock()
		if !kv.deduplicate(op) {
			kv.stateMachine(op)
			kv.lastRequestId[op.ClientId] = op.RequestId
		}

		ch, ok := kv.operationChan[msg.CommandIndex]
		if ok {
			select {
			case <-ch: // drain bad data
			default:
			}
		} else {
			ch = make(chan Op, 1)
			kv.operationChan[msg.CommandIndex] = ch
		}
		ch <- op
		kv.mu.Unlock()
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvStorage = make(map[string]string)
	kv.lastRequestId = make(map[int64]int64)
	kv.operationChan = make(map[int]chan Op)

	go kv.kernelEventLoop()
	return kv
}
