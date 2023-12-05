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
	Type   string
	Key    string
	Value  string
	SeqNum int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	store map[string]string

	pendingMu sync.Mutex
	pending   map[int64]*sync.Cond
	done      map[int64]bool
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	// DPrintf("Get  args= %+v\n", args)
	if kv.done[args.SeqNum] {
		DPrintf("S%d Get done args= %+v\n", kv.me, args)
		reply.Err = OK
		reply.Value = kv.store[args.Key]
		kv.mu.Unlock()
		return
	}

	op := Op{Type: GET, Key: args.Key, SeqNum: args.SeqNum}
	kv.mu.Unlock()
	if _, _, ok := kv.rf.Start(op); ok {
		DPrintf("S%d Get start args= %+v\n", kv.me, args)
		kv.mu.Lock()
		cond, ok := kv.pending[args.SeqNum]
		if !ok {
			cond = sync.NewCond(&kv.mu)
			kv.pending[args.SeqNum] = cond
		}

		for !kv.done[args.SeqNum] {
			cond.Wait()
		}
		reply.Err = OK
		reply.Value = kv.store[args.Key]
		kv.mu.Unlock()

	} else {
		DPrintf("S%d Get ErrWrongLeader args= %+v\n", kv.me, args)
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	// DPrintf("PutAppend  args= %+v\n", args)
	if kv.done[args.SeqNum] {
		DPrintf("S%d PutAppend done args= %+v\n", kv.me, args)
		kv.mu.Unlock()
		reply.Err = OK
		return
	}

	op := Op{Type: args.Op, Key: args.Key, Value: args.Value, SeqNum: args.SeqNum}

	kv.mu.Unlock()
	if _, _, ok := kv.rf.Start(op); ok {
		DPrintf("S%d PutAppend start args= %+v\n", kv.me, args)
		kv.mu.Lock()
		cond, ok := kv.pending[args.SeqNum]
		if !ok {
			cond = sync.NewCond(&kv.mu)
			kv.pending[args.SeqNum] = cond
		}
		for !kv.done[args.SeqNum] {
			cond.Wait()
		}
		kv.mu.Unlock()
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
			kv.mu.Lock()
			if !kv.done[op.SeqNum] {
				switch op.Type {
				case PUT:
					kv.store[op.Key] = op.Value
				case APPEND:
					if val, exist := kv.store[op.Key]; exist {
						kv.store[op.Key] = val + op.Value
					} else {
						kv.store[op.Key] = op.Value
					}
				}

				kv.done[op.SeqNum] = true
			}
			kv.mu.Unlock()
			if cond, ok := kv.pending[op.SeqNum]; ok {
				cond.Broadcast()
				delete(kv.pending, op.SeqNum)
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
	kv.store = make(map[string]string)
	kv.pending = make(map[int64]*sync.Cond)
	kv.done = make(map[int64]bool)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.applier()
	return kv
}
