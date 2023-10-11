package shardmaster

import (
	"../raft"
	"time"
)
import "../labrpc"
import "sync"
import "../labgob"

const (
	JOIN  = "JOIN"
	QUERY = "QUERY"
	LEAVE = "LEAVE"
	MOVE  = "MOVE"
)

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	lastRequestId map[int64]int64
	operationChan map[int]chan Op // op by raft log index
	configs       []Config        // indexed by config num
}

type Op struct {
	Command string
	Args    interface{}

	Request RequestCommon
}

func (kv *ShardMaster) appendOperationToEntry(op Op) bool {
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
		return op.Request.ClientId == appliedRaftEntry.Request.ClientId &&
			op.Request.RequestId == appliedRaftEntry.Request.RequestId
	case <-time.After(1000 * time.Millisecond):
		return false
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	entry := Op{}
	entry.Command = JOIN
	entry.Request = args.Request

	joinArgs := JoinArgs{}
	joinArgs.Servers = args.Servers
	joinArgs.Request = args.Request

	entry.Args = joinArgs

	if !sm.appendOperationToEntry(entry) {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	reply.Err = OK
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	entry := Op{}
	entry.Command = LEAVE
	entry.Request = args.Request

	leaveArgs := LeaveArgs{}
	leaveArgs.GIDs = args.GIDs
	leaveArgs.Request = args.Request

	entry.Args = leaveArgs

	if !sm.appendOperationToEntry(entry) {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	reply.Err = OK
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	entry := Op{}
	entry.Command = MOVE
	entry.Request = args.Request

	moveArgs := MoveArgs{}
	moveArgs.GID = args.GID
	moveArgs.Shard = args.Shard
	moveArgs.Request = args.Request

	entry.Args = moveArgs

	if !sm.appendOperationToEntry(entry) {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	reply.Err = OK
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	entry := Op{}
	entry.Command = QUERY
	entry.Request = args.Request

	queryArgs := QueryArgs{}
	queryArgs.Num = args.Num
	queryArgs.Request = args.Request

	entry.Args = queryArgs

	if !sm.appendOperationToEntry(entry) {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	reply.Err = OK

	sm.mu.Lock()
	defer sm.mu.Unlock()

	if args.Num == -1 || args.Num >= len(sm.configs) {
		reply.Config = sm.configs[len(sm.configs)-1]
	} else {
		reply.Config = sm.configs[args.Num]
	}
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func (sm *ShardMaster) deduplicate(op Op) bool {
	lastSavedRid, ok := sm.lastRequestId[op.Request.ClientId]
	if ok {
		return lastSavedRid >= op.Request.RequestId
	}
	return false
}

func (sm *ShardMaster) stateMachine(op Op) {
	switch op.Command {
	case QUERY:
	case LEAVE:
		sm.createNewConfig()
		sm.applyLeave(op.Args.(LeaveArgs))
	case MOVE:
		sm.createNewConfig()
		sm.applyMove(op.Args.(MoveArgs))
	case JOIN:
		sm.createNewConfig()
		sm.applyJoin(op.Args.(JoinArgs))
	}
	sm.lastRequestId[op.Request.ClientId] = op.Request.RequestId
}

func (sm *ShardMaster) applyLeave(args LeaveArgs) {
	config := &sm.configs[len(sm.configs)-1]

	tempGid := 0
	for gid := range config.Groups {
		flag := true
		for _, deletedGid := range args.GIDs {
			if gid == deletedGid {
				flag = false
			}
		}
		if flag {
			tempGid = gid
			break
		}
	}

	for _, gid := range args.GIDs {
		for i := 0; i < len(config.Shards); i++ {
			if config.Shards[i] == gid {
				config.Shards[i] = tempGid
			}
		}
		delete(config.Groups, gid)
	}

	if len(config.Groups) == 0 {
		for i := 0; i < len(config.Shards); i++ {
			config.Shards[i] = 0
		}
	} else {
		gidToShards := sm.makeGidToShards()

		maxGid := getGidWithMaxShards(gidToShards)
		minGid := getGidWithMinShards(gidToShards)

		for len(gidToShards[maxGid])-len(gidToShards[minGid]) > 1 {
			config.Shards[gidToShards[maxGid][0]] = minGid
			gidToShards[minGid] = append(gidToShards[minGid], gidToShards[maxGid][0])
			gidToShards[maxGid] = gidToShards[maxGid][1:]

			maxGid = getGidWithMaxShards(gidToShards)
			minGid = getGidWithMinShards(gidToShards)
		}
	}
}

func (sm *ShardMaster) applyMove(args MoveArgs) {
	config := &sm.configs[len(sm.configs)-1]
	config.Shards[args.Shard] = args.GID
}

func (sm *ShardMaster) applyJoin(args JoinArgs) {
	config := &sm.configs[len(sm.configs)-1]

	for gid, servers := range args.Servers {
		config.Groups[gid] = servers
		for i := range config.Shards {
			if config.Shards[i] == 0 {
				config.Shards[i] = gid
			}
		}
	}

	if len(config.Groups) > 0 {
		gidToShards := sm.makeGidToShards()

		maxGid := getGidWithMaxShards(gidToShards)
		minGid := getGidWithMinShards(gidToShards)

		for len(gidToShards[maxGid])-len(gidToShards[minGid]) > 1 {
			config.Shards[gidToShards[maxGid][0]] = minGid
			gidToShards[minGid] = append(gidToShards[minGid], gidToShards[maxGid][0])
			gidToShards[maxGid] = gidToShards[maxGid][1:]

			maxGid = getGidWithMaxShards(gidToShards)
			minGid = getGidWithMinShards(gidToShards)
		}
	}
}

func getGidWithMaxShards(gidToShards map[int][]int) int {
	maxGid := -1
	for gid, shards := range gidToShards {
		if maxGid == -1 || len(gidToShards[maxGid]) < len(shards) {
			maxGid = gid
		}
	}
	return maxGid
}

func getGidWithMinShards(gidToShards map[int][]int) int {
	minGid := -1
	for gid, shards := range gidToShards {
		if minGid == -1 || len(gidToShards[minGid]) > len(shards) {
			minGid = gid
		}
	}
	return minGid
}

func (sm *ShardMaster) makeGidToShards() map[int][]int {
	config := &sm.configs[len(sm.configs)-1]

	gidToShards := make(map[int][]int)
	for gid := range config.Groups {
		gidToShards[gid] = make([]int, 0)
	}
	for shard, gid := range config.Shards {
		gidToShards[gid] = append(gidToShards[gid], shard)
	}
	return gidToShards
}

func (sm *ShardMaster) createNewConfig() {
	prevConfig := sm.configs[len(sm.configs)-1]

	nextConfig := Config{}
	nextConfig.Num = prevConfig.Num + 1
	nextConfig.Shards = prevConfig.Shards
	nextConfig.Groups = map[int][]string{}

	for gid, servers := range prevConfig.Groups {
		nextConfig.Groups[gid] = servers
	}
	sm.configs = append(sm.configs, nextConfig)
}

func (sm *ShardMaster) kernelEventLoop() {
	for {
		msg := <-sm.applyCh
		sm.mu.Lock()

		op := msg.Command.(Op)

		if !sm.deduplicate(op) {
			sm.stateMachine(op)
		}

		ch, ok := sm.operationChan[msg.CommandIndex]
		if ok {
			select {
			case <-ch: // drain bad data
			default:
			}
		} else {
			ch = make(chan Op, 1)
			sm.operationChan[msg.CommandIndex] = ch
		}
		ch <- op
		sm.mu.Unlock()
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})
	labgob.Register(JoinReply{})
	labgob.Register(LeaveReply{})
	labgob.Register(MoveReply{})
	labgob.Register(QueryReply{})

	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	sm.lastRequestId = make(map[int64]int64)
	sm.operationChan = make(map[int]chan Op)

	go sm.kernelEventLoop()

	return sm
}
