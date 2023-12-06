package kvraft

import (
	"sync"
	"sync/atomic"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const (
	PUT    = "Put"
	APPEND = "Append"
	GET    = "Get"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type     string
	Key      string
	Value    string
	SeqNum   int64
	ClientId int64
}

type ClientStatus struct {
	mu         sync.Mutex
	pending    map[int64]*sync.Cond
	lastSeqNum int64
}

func (cs *ClientStatus) init() {
	cs.pending = make(map[int64]*sync.Cond)
}

func (cs *ClientStatus) done(seqNum int64) bool {
	return cs.lastSeqNum >= seqNum
}

func (cs *ClientStatus) setPendingCond(seqNum int64) *sync.Cond {
	cond, ok := cs.pending[seqNum]
	if !ok {
		cond = sync.NewCond(&cs.mu)
		cs.pending[seqNum] = cond
	}
	return cond
}

type Store struct {
	mu   sync.RWMutex
	data map[string]string
}

func (s *Store) init() {
	s.data = make(map[string]string)
}
func (s *Store) get(key string) (value string, exist bool) {
	s.mu.RLock()
	s.mu.RUnlock()
	value, exist = s.data[key]
	return
}

func (s *Store) put(key string, value string) {
	s.mu.Lock()
	s.mu.Unlock()
	s.data[key] = value
}

func (s *Store) append(key string, value string) {
	s.mu.Lock()
	s.mu.Unlock()
	if val, exist := s.data[key]; exist {
		s.data[key] = val + value
	} else {
		s.data[key] = value
	}
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	store Store

	clientsStatus map[int64]*ClientStatus
}

func (kv *KVServer) getClientStatus(clientId int64) *ClientStatus {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	status, ok := kv.clientsStatus[clientId]
	if !ok {
		status = new(ClientStatus)
		status.pending = make(map[int64]*sync.Cond)
		kv.clientsStatus[clientId] = status
	}
	return status
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	clientStatus := kv.getClientStatus(args.ClientId)
	clientStatus.mu.Lock()
	defer clientStatus.mu.Unlock()
	// DPrintf("Get  args= %+v\n", args)
	if clientStatus.done(args.SeqNum) {
		DPrintf("S%d Get done args= %+v\n", kv.me, args)
		reply.Err = OK
		reply.Value, _ = kv.store.get(args.Key)
		return
	}

	op := Op{Type: GET, Key: args.Key, SeqNum: args.SeqNum, ClientId: args.ClientId}
	if _, _, ok := kv.rf.Start(op); ok {
		DPrintf("S%d Get start args= %+v\n", kv.me, args)
		cond := clientStatus.setPendingCond(args.SeqNum)
		for !clientStatus.done(args.SeqNum) {
			cond.Wait()
		}
		reply.Err = OK
		reply.Value, _ = kv.store.get(args.Key)

	} else {
		DPrintf("S%d Get ErrWrongLeader args= %+v\n", kv.me, args)
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	clientStatus := kv.getClientStatus(args.ClientId)
	clientStatus.mu.Lock()
	defer clientStatus.mu.Unlock()
	if clientStatus.done(args.SeqNum) {
		DPrintf("S%d PutAppend done args= %+v\n", kv.me, args)
		reply.Err = OK
		return
	}
	op := Op{Type: args.Op, Key: args.Key, Value: args.Value, SeqNum: args.SeqNum, ClientId: args.ClientId}
	if _, _, ok := kv.rf.Start(op); ok {
		DPrintf("S%d PutAppend start args= %+v\n", kv.me, args)
		cond := clientStatus.setPendingCond(args.SeqNum)
		for !clientStatus.done(args.SeqNum) {
			cond.Wait()
		}
		reply.Err = OK
	} else {
		DPrintf("S%d PutAppend ErrWrongLeader args= %+v\n", kv.me, args)
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) applier() {
	for m := range kv.applyCh {
		// kv.mu.Lock()
		// log.Printf("S%d applierSnap -> applyCh msg= %v\n", i, &m)
		if m.SnapshotValid {
			// err_msg = cfg.ingestSnap(i, m.Snapshot, m.SnapshotIndex)
		} else if m.CommandValid {
			var op Op = m.Command.(Op)
			DPrintf("S%d apply op= %+v\n", kv.me, op)
			clientStatus := kv.getClientStatus(op.ClientId)

			if !clientStatus.done(op.SeqNum) {
				switch op.Type {
				case PUT:
					kv.store.put(op.Key, op.Value)
				case APPEND:
					kv.store.append(op.Key, op.Value)
				}

				clientStatus.mu.Lock()
				clientStatus.lastSeqNum = op.SeqNum
				if cond, ok := clientStatus.pending[op.SeqNum]; ok {
					cond.Broadcast()
					delete(clientStatus.pending, op.SeqNum)
				}
				clientStatus.mu.Unlock()
			}

			// if (m.CommandIndex+1)%kv.maxraftstate == 0 {
			// 	kv.snapshot()
			// }
		}
		// kv.mu.Unlock()
	}
}

func (kv *KVServer) snapshot() {
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(m.CommandIndex)
	// var xlog []interface{}
	// for j := 0; j <= m.CommandIndex; j++ {
	// 	xlog = append(xlog, cfg.logs[i][j])
	// }
	// e.Encode(xlog)
	// kv.rf.Snapshot(m.CommandIndex, w.Bytes())
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

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
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.store = Store{}
	kv.store.init()
	kv.clientsStatus = make(map[int64]*ClientStatus)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.applier()
	return kv
}
