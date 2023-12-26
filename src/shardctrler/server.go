package shardctrler

import (
	"log"
	"sync"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type ClientStatus struct {
	mu         sync.Mutex
	cond       *sync.Cond
	lastSeqNum int64
}

func (cs *ClientStatus) init() {
	cs.cond = sync.NewCond(&cs.mu)
}

func (cs *ClientStatus) done(seqNum int64) bool {
	return cs.lastSeqNum >= seqNum
}

func (cs *ClientStatus) updateSeqNum(seqNum int64) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.lastSeqNum = seqNum
	cs.cond.Broadcast()
}

const (
	JOIN  = "JOIN"
	LEAVE = "LEAVE"
	MOVE  = "MOVE"
	QUERY = "QUERY"
)

type Op struct {
	Type string
	Args interface{}
}

type Configs struct {
	mu      sync.RWMutex
	entries []Config // indexed by config num
}

func (configs *Configs) init() {
	configs.entries = make([]Config, 1)
	configs.entries[0].Groups = map[int][]string{}

}

func (configs *Configs) append(config Config) {
	configs.mu.Lock()
	defer configs.mu.Unlock()
	config.Num = len(configs.entries)
	configs.entries = append(configs.entries, config)
}

func (configs *Configs) getConfig(index int) *Config {
	configs.mu.RLock()
	defer configs.mu.RUnlock()
	if index >= 0 && index < len(configs.entries) {
		return &configs.entries[index]
	}
	return &configs.entries[len(configs.entries)-1]
}

func (configs *Configs) getLatestConfig() *Config {
	configs.mu.RLock()
	defer configs.mu.RUnlock()
	return &configs.entries[len(configs.entries)-1]
}

type ShardCtrler struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	configs       Configs
	clientsStatus map[int64]*ClientStatus
}

func (sc *ShardCtrler) getClientStatus(clientId int64) *ClientStatus {
	sc.mu.RLock()
	status, ok := sc.clientsStatus[clientId]
	sc.mu.RUnlock()
	if ok {
		return status
	}

	sc.mu.Lock()
	defer sc.mu.Unlock()
	status, ok = sc.clientsStatus[clientId]
	if !ok {
		status = new(ClientStatus)
		status.init()
		sc.clientsStatus[clientId] = status
	}
	return status
}

func (sc *ShardCtrler) operate(opType string, clientId int64, seqNum int64, args interface{}) (isLeader bool) {
	clientStatus := sc.getClientStatus(clientId)
	clientStatus.mu.Lock()
	defer clientStatus.mu.Unlock()
	if clientStatus.done(seqNum) {
		DPrintf(DEBUG, "S%d %v has been done. args= %+v\n", sc.me, opType, args)
		return true
	}
	op := Op{Type: opType, Args: args}
	if _, _, ok := sc.rf.Start(op); ok {
		DPrintf(DEBUG, "S%d %v start. args= %+v\n", sc.me, opType, args)
		for !clientStatus.done(seqNum) {
			clientStatus.cond.Wait()
		}
		return true
	} else {
		DPrintf(DEBUG, "S%d operate ErrWrongLeader args= %+v\n", sc.me, args)
		return false
	}
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *Reply) {
	isLeader := sc.operate(JOIN, args.ClientId, args.SeqNum, *args)
	if isLeader {
		reply.Err = OK
	} else {
		reply.Err = WrongLeader
	}

}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *Reply) {
	isLeader := sc.operate(LEAVE, args.ClientId, args.SeqNum, *args)
	if isLeader {
		reply.Err = OK
	} else {
		reply.Err = WrongLeader
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *Reply) {
	isLeader := sc.operate(MOVE, args.ClientId, args.SeqNum, *args)
	if isLeader {
		reply.Err = OK
	} else {
		reply.Err = WrongLeader
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	isLeader := sc.operate(QUERY, args.ClientId, args.SeqNum, *args)
	if isLeader {
		reply.Err = OK
		reply.Config = *sc.configs.getConfig(args.Num)
		DPrintf(DEBUG, "S%d query result=%+v. args= %+v\n", sc.me, reply.Config, args)
	} else {
		reply.Err = WrongLeader
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) applier() {
	for m := range sc.applyCh {
		if m.CommandValid {

			var op Op = m.Command.(Op)
			switch op.Type {
			case JOIN:
				args := op.Args.(JoinArgs)
				DPrintf(DEBUG, "S%d apply join : %+v\n", sc.me, args)
				clientStatus := sc.getClientStatus(args.ClientId)
				if !clientStatus.done(args.SeqNum) {
					config := sc.configs.getLatestConfig().Clone()
					if config.Num == 0 { // the first time
						config.Groups = args.Groups
						config.ResetShards()
					} else {
						config.JoinGroups(args.Groups)
					}
					sc.configs.append(config)
					clientStatus.updateSeqNum(args.SeqNum)
				}
			case LEAVE:
				args := op.Args.(LeaveArgs)
				DPrintf(DEBUG, "S%d apply leave : %+v\n", sc.me, args)
				clientStatus := sc.getClientStatus(args.ClientId)
				if !clientStatus.done(args.SeqNum) {
					config := sc.configs.getLatestConfig().Clone()
					config.RemoveGroups(args.GIDs)
					sc.configs.append(config)
					clientStatus.updateSeqNum(args.SeqNum)
				}
			case MOVE:
				args := op.Args.(MoveArgs)
				DPrintf(DEBUG, "S%d apply move : %+v\n", sc.me, args)
				clientStatus := sc.getClientStatus(args.ClientId)
				if !clientStatus.done(args.SeqNum) {
					config := sc.configs.getLatestConfig().Clone()
					config.MoveShard(args.Shard, args.GID)
					sc.configs.append(config)
					clientStatus.updateSeqNum(args.SeqNum)
				}
			case QUERY:
				args := op.Args.(QueryArgs)
				DPrintf(DEBUG, "S%d apply query : %+v\n", sc.me, args)
				clientStatus := sc.getClientStatus(args.ClientId)
				if !clientStatus.done(args.SeqNum) {
					clientStatus.updateSeqNum(args.SeqNum)
				}
			default:
				log.Fatalf("I don't know about type %v!\n", op.Type)
			}
		}
	}
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	labgob.Register(Op{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})

	sc := new(ShardCtrler)
	sc.me = me
	sc.configs.init()
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.clientsStatus = make(map[int64]*ClientStatus)
	go sc.applier()
	return sc
}
