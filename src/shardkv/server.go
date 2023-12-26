package shardkv

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters, otherwise RPC will break.
	Type string
	Ch   chan (Err)
	Args interface{}
}

type ClientStatus struct {
	mu         sync.RWMutex
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

type ClientStatusMap struct {
	mu   sync.RWMutex
	data map[int64]*ClientStatus
}

func (cm *ClientStatusMap) init() {
	cm.data = make(map[int64]*ClientStatus)
}

func (cm *ClientStatusMap) getClientSeqNumMap() map[int64]int64 {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	clientSeqNumMap := make(map[int64]int64)
	for seqNum, clientStatus := range cm.data {
		clientSeqNumMap[seqNum] = clientStatus.lastSeqNum
	}
	return clientSeqNumMap
}

func (cm *ClientStatusMap) getClientStatus(clientId int64) *ClientStatus {
	cm.mu.RLock()
	status, ok := cm.data[clientId]
	cm.mu.RUnlock()
	if ok {
		return status
	}

	cm.mu.Lock()
	defer cm.mu.Unlock()
	status, ok = cm.data[clientId]
	if !ok {
		status = new(ClientStatus)
		status.init()
		cm.data[clientId] = status
	}
	return status
}

type ShardKV struct {
	// mu           sync.RWMutex
	me           int
	raft         *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	mu               sync.RWMutex
	cond             *sync.Cond
	config           shardctrler.Config
	store            [shardctrler.NShards]map[string]string
	restoreCompleted bool

	ctrlClerk *shardctrler.Clerk
	persister *raft.Persister

	clientStatusMap ClientStatusMap

	seqNum         int64
	clientId       int64
	groupLeaderMap GroupLeaderMap

	killChan chan bool
}

// ===================== store =======================
func (kv *ShardKV) initStore() {
	for i := 0; i < len(kv.store); i++ {
		kv.store[i] = make(map[string]string)
	}
}

func (kv *ShardKV) get(key string) (string, Err) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	shard := key2shard(key)
	if kv.gid != kv.config.Shards[shard] {
		return "", ErrWrongGroup
	}
	value, exist := kv.store[shard][key]
	if !exist {
		return "", ErrNoKey
	}
	return value, OK
}

func (kv *ShardKV) put(key string, value string) Err {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	shard := key2shard(key)
	if kv.gid != kv.config.Shards[shard] {
		return ErrWrongGroup
	}
	kv.store[shard][key] = value
	return OK
}

func (kv *ShardKV) append(key string, value string) Err {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	shard := key2shard(key)
	if kv.gid != kv.config.Shards[shard] {
		return ErrWrongGroup
	}
	if val, exist := kv.store[key2shard(key)][key]; exist {
		kv.store[shard][key] = val + value
	} else {
		kv.store[shard][key] = value
	}
	return OK
}

func (kv *ShardKV) setStore(store [shardctrler.NShards]map[string]string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.store = store
}

func (kv *ShardKV) putShards(shards map[int]map[string]string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	for shard, data := range shards {
		kv.store[shard] = make(map[string]string)
		for k, v := range data {
			kv.store[shard][k] = v
		}
	}
}

func (kv *ShardKV) getShards(shards []int) map[int]map[string]string {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	data := map[int]map[string]string{}
	for _, shard := range shards {
		data[shard] = kv.store[shard]
	}
	return data
}

// ===================== store end =======================

func (kv *ShardKV) getConfig() shardctrler.Config {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	return kv.config
}

func (kv *ShardKV) waitUntilRestoreCompleted() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// Debug(InfoEvent, "G%d S%d ===waitUntilRestoreCompleted 1", kv.gid, kv.me)
	for !kv.restoreCompleted {
		// Debug(InfoEvent, "G%d S%d ===waitUntilRestoreCompleted 2", kv.gid, kv.me)
		kv.cond.Wait()
	}
	// Debug(InfoEvent, "G%d S%d ===waitUntilRestoreCompleted 3", kv.gid, kv.me)
}

func (kv *ShardKV) isRestoreCompleted() bool {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	return kv.restoreCompleted
}

func (kv *ShardKV) completedRestore() {
	kv.mu.Lock()
	kv.restoreCompleted = true
	kv.mu.Unlock()
	kv.cond.Broadcast()
	Debug(InfoEvent, "G%d S%d ===completedRestore", kv.gid, kv.me)
}

func (kv *ShardKV) operate(opType string, clientId int64, seqNum int64, args interface{}) Err {
	if !kv.isRestoreCompleted() {
		kv.waitUntilRestoreCompleted()
	}

	clientStatus := kv.clientStatusMap.getClientStatus(clientId)
	clientStatus.mu.Lock()
	defer clientStatus.mu.Unlock()
	if clientStatus.done(seqNum) {
		Debug(InfoEvent, "G%d S%d %s has been done. args= %+v", kv.gid, kv.me, opType, args)
		return OK
	}
	op := Op{Type: opType, Ch: make(chan (Err), 1), Args: args}
	if _, _, ok := kv.raft.Start(op); ok {
		Debug(InfoEvent, "G%d S%d %s start args= %+v, config=%+v", kv.gid, kv.me, opType, args, kv.config)
		for !clientStatus.done(seqNum) {
			clientStatus.cond.Wait()
		}
		return OK
		// select {
		// case err := <-op.Ch:
		// 	return err
		// case <-time.After(2 * time.Second):
		// 	Debug(InfoEvent, "G%d S%d %v timeout", kv.gid, kv.me, opType)
		// }
	}
	Debug(InfoEvent, "G%d S%d %s ErrWrongLeader args= %+v, config=%+v", kv.gid, kv.me, opType, args, kv.config)
	return ErrWrongLeader

}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	reply.Err = kv.operate(GET, args.ClientId, args.SeqNum, *args)
	if reply.Err == OK {
		reply.Value, reply.Err = kv.get(args.Key)
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {

	reply.Err = kv.operate(args.Op, args.ClientId, args.SeqNum, *args)
}

func (kv *ShardKV) GetShards(args *GetShardsArgs, reply *GetShardsReply) {
	// Debug(MigrationEvent, "G%d S%d  GetShards, args=%+v, config=%+v",
	// 	kv.gid, kv.me, args, kv.config)
	reply.Err = kv.operate(GET_SHARDS, args.ClientId, args.SeqNum, *args)
	if reply.Err != OK {
		return
	}
	// Debug(MigrationEvent, "G%d S%d  GetShards ok args=%+v, config=%+v ",
	// 	kv.gid, kv.me, args, kv.config)
	reply.Err = kv.changeConfig(args.Config)
	if reply.Err != OK {
		return
	}
	// kv.configStatus.removeShards(args.Shards) // stop serve args.Shards
	reply.Shards = kv.getShards(args.Shards)
	Debug(MigrationEvent, "G%d S%d  GetShards ok 2. reply=%+v, config=%+v",
		kv.gid, kv.me, reply, kv.config)

}

func (kv *ShardKV) PutShards(args *PutShardsArgs, reply *PutShardsReply) {
	reply.Err = kv.operate(PUT_SHARDS, args.ClientId, args.SeqNum, *args)
}

func (kv *ShardKV) applier() {
	// for !kv.Killed() {
	// 	msg := <-kv.applyCh
	// 	kv.apply(msg)
	// }

	const timeout = 2000 * time.Millisecond
	t := time.NewTimer(timeout)
	Debug(TraceEvent, "G%d S%d applier begin", kv.gid, kv.me)
	for !kv.Killed() {
		select {
		case <-kv.killChan:
			return
		case msg := <-kv.applyCh:
			Debug(TraceEvent, "G%d S%d apply msg ", kv.gid, kv.me)
			kv.apply(msg)
			if !kv.isRestoreCompleted() {
				t.Reset(timeout)
			}
		case <-t.C:
			kv.completedRestore()

		}
	}
}

func (kv *ShardKV) apply(msg raft.ApplyMsg) {
	// log.Printf("S%d applierSnap -> applyCh msg= %v\n", i, &m)
	if msg.SnapshotValid {
		kv.applySnap(msg.Snapshot, msg.SnapshotIndex)
	} else if msg.CommandValid {
		var op Op = msg.Command.(Op)
		kv.applyOp(op)

		if kv.maxraftstate > 0 && kv.persister.RaftStateSize() >= kv.maxraftstate {
			kv.snapshot(msg.CommandIndex)
		}
	}
}

func (kv *ShardKV) applyOp(op Op) {
	err := OK
	switch op.Type {
	case PUT:
		args := op.Args.(PutAppendArgs)
		Debug(PutEvent, "G%d S%d apply PUT : args=%+v, config=%+v\n", kv.gid, kv.me, args, kv.config)
		clientStatus := kv.clientStatusMap.getClientStatus(args.ClientId)
		if !clientStatus.done(args.SeqNum) {
			err = kv.put(args.Key, args.Value)
			if err == OK {
				clientStatus.updateSeqNum(args.SeqNum)
			}

		}
	case APPEND:
		args := op.Args.(PutAppendArgs)
		Debug(PutEvent, "G%d S%d apply APPEND : args=%+v, config=%+v\n", kv.gid, kv.me, args, kv.config)
		clientStatus := kv.clientStatusMap.getClientStatus(args.ClientId)
		if !clientStatus.done(args.SeqNum) {
			err = kv.append(args.Key, args.Value)
			if err == OK {
				clientStatus.updateSeqNum(args.SeqNum)
			}

		}
	case GET:
		args := op.Args.(GetArgs)
		Debug(GetEvent, "G%d S%d apply GET : args=%+v, config=%+v\n", kv.gid, kv.me, args, kv.config)
		clientStatus := kv.clientStatusMap.getClientStatus(args.ClientId)
		if !clientStatus.done(args.SeqNum) {

			clientStatus.updateSeqNum(args.SeqNum)
		}
	case GET_SHARDS:
		args := op.Args.(GetShardsArgs)
		Debug(MigrationEvent, "G%d S%d apply GET_SHARDS : args=%+v, config=%+v\n", kv.gid, kv.me, args, kv.config)
		clientStatus := kv.clientStatusMap.getClientStatus(args.ClientId)
		if !clientStatus.done(args.SeqNum) {
			clientStatus.updateSeqNum(args.SeqNum)
		}
	case PUT_SHARDS:
		args := op.Args.(PutShardsArgs)
		// Debug(MigrationEvent, "G%d S%d apply %s , args=%+v, config=%+v\n", kv.gid, kv.me, op.Type, args, kv.config)
		clientStatus := kv.clientStatusMap.getClientStatus(args.ClientId)
		if !clientStatus.done(args.SeqNum) {
			// Debug(MigrationEvent, "G%d S%d apply %s 1, args=%+v, config=%+v\n", kv.gid, kv.me, op.Type, args, kv.config)
			kv.putShards(args.Shards)
			Debug(MigrationEvent, "G%d S%d apply %s 2, args=%+v, config=%+v, kv.Killed() =%v", kv.gid, kv.me, op.Type, args, kv.config, kv.Killed())
			clientStatus.updateSeqNum(args.SeqNum)
		}

	case SYNC_CONFIG:
		args := op.Args.(SetConfigArgs)
		Debug(ConfigEvent, "G%d S%d apply SYNC_CONFIG,  old config=%+v, new config=%+v",
			kv.gid, kv.me, kv.config, args.Config)
		clientStatus := kv.clientStatusMap.getClientStatus(args.ClientId)
		if !clientStatus.done(args.SeqNum) {
			// 这里不需要锁，因为kv.config只有在appler这里会改变，而appler是单线程的
			if args.Config.Num > kv.config.Num {
				kv.config = args.Config.Clone()
				// Debug(ConfigEvent, "G%d S%d apply SYNC_CONFIG, config=%+v", kv.gid, kv.me, kv.config)
				clientStatus.updateSeqNum(args.SeqNum)
			}
		}
	default:
		log.Fatalf("I don't know about type %v!\n", op.Type)
	}
	// op.Ch <- err
}

func (kv *ShardKV) applySnap(snapshot []byte, index int) {
	if snapshot == nil {
		log.Fatalf("nil snapshot")
		return
	}
	Debug(SnapEvent, "G%d S%d applySnap\n", kv.gid, kv.me)
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var lastIncludedIndex int
	var data [shardctrler.NShards]map[string]string
	var clientSeqNumMap map[int64]int64
	if d.Decode(&lastIncludedIndex) != nil {
		log.Fatalf("snapshot decode lastIncludedIndex error")
		return
	}
	if d.Decode(&data) != nil {
		log.Fatalf("snapshot decode data error")
		return
	}
	if d.Decode(&clientSeqNumMap) != nil {
		log.Fatalf("snapshot decode clientSeqNumMap error")
		return
	}
	if index != -1 && index != lastIncludedIndex {
		log.Fatalf("snapshot doesn't match m.SnapshotIndex")
		return
	}
	kv.setStore(data)
	for clientId, seqNum := range clientSeqNumMap {
		clientStatus := kv.clientStatusMap.getClientStatus(clientId)
		clientStatus.updateSeqNum(seqNum)
	}
}

func (kv *ShardKV) snapshot(logIndex int) {
	Debug(SnapEvent, "G%d S%d snapshot logIndex= %v\n", kv.gid, kv.me, logIndex)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(logIndex)
	e.Encode(kv.store)
	e.Encode(kv.clientStatusMap.getClientSeqNumMap())
	kv.raft.Snapshot(logIndex, w.Bytes())
	go func() {
		kv.raft.Snapshot(logIndex, w.Bytes())
	}()

}

func (kv *ShardKV) callOtherGroup(method string, gid int, servers []string, args interface{}) interface{} {

	for {
		// try each known server.
		leaderId := kv.groupLeaderMap.getLeaderOfGroup(gid)
		for off := 0; off < len(servers); off++ {
			si := (leaderId + off) % len(servers)
			srv := kv.make_end(servers[si])
			switch method {
			case "ShardKV.GetShards":
				var reply GetShardsReply
				ok := srv.Call(method, args, &reply)
				Debug(MigrationEvent, "G%d S%d callOtherGroup %d ShardKV.GetShards reply=%+v", kv.gid, kv.me, gid, reply)
				if ok && reply.Err != ErrWrongLeader {
					kv.groupLeaderMap.setLeaderOfGroup(gid, si)
					return &reply
				}
			case "ShardKV.PutShards":
				var reply PutShardsReply
				ok := srv.Call(method, args, &reply)
				if ok && reply.Err != ErrWrongLeader {
					kv.groupLeaderMap.setLeaderOfGroup(gid, si)
					return &reply
				}
			default:
				panic(fmt.Sprintf("unknown method %s", method))
			}

		}
		time.Sleep(100 * time.Millisecond)
	}

}

func (kv *ShardKV) callGetShards(gid int, servers []string, args *GetShardsArgs) map[int]map[string]string {

	Debug(MigrationEvent, "G%d S%d callGetShards gid=%d, args=%+v", kv.gid, kv.me, gid, args)
	reply := kv.callOtherGroup("ShardKV.GetShards", gid, servers, args).(*GetShardsReply)
	return reply.Shards
}

func (kv *ShardKV) callPutShards(gid int, servers []string, shards map[int]map[string]string) {
	args := PutShardsArgs{
		Shards:   shards,
		ClientId: kv.clientId,
		SeqNum:   atomic.AddInt64(&kv.seqNum, 1),
	}
	kv.callOtherGroup("ShardKV.PutShards", gid, servers, &args)
}

func (kv *ShardKV) syncConfig(config *shardctrler.Config) Err {
	args := SetConfigArgs{
		Config:   config.Clone(),
		ClientId: kv.clientId,
		SeqNum:   atomic.AddInt64(&kv.seqNum, 1),
	}
	Debug(ConfigEvent, "G%d S%d syncConfig, old config=%+v, new config=%+v",
		kv.gid, kv.me, kv.config, config)
	return kv.operate(SYNC_CONFIG, args.ClientId, args.SeqNum, args)

}

func (kv *ShardKV) syncShards(shards map[int]map[string]string) Err {
	args := PutShardsArgs{
		Shards:   shards,
		ClientId: kv.clientId,
		SeqNum:   atomic.AddInt64(&kv.seqNum, 1),
	}
	Debug(MigrationEvent, "G%d S%d syncShards before args=%+v, config=%+v", kv.gid, kv.me, args, kv.config)
	err := kv.operate(PUT_SHARDS, args.ClientId, args.SeqNum, args)
	Debug(MigrationEvent, "G%d S%d syncShards after args=%+v, err=%v", kv.gid, kv.me, args, err)
	return err
}

func contains(arr []int, o int) bool {
	for _, v := range arr {
		if v == o {
			return true
		}
	}
	return false
}

func (kv *ShardKV) pollConfig() {
	for !kv.Killed() {
		if !kv.isRestoreCompleted() {
			kv.waitUntilRestoreCompleted()
		}
		if _, isLeader := kv.raft.GetState(); isLeader {
			kv.changeConfig(kv.ctrlClerk.Query(-1))
			time.Sleep(10000 * time.Millisecond)
		}
		time.Sleep(100 * time.Millisecond)
	}

}

func (kv *ShardKV) changeConfig(newConfig shardctrler.Config) Err {

	oldConfig := kv.getConfig()
	if newConfig.Num > oldConfig.Num {
		Debug(ConfigEvent, "G%d S%d changeConfig, oldConfig=%+v, newConfig=%+v", kv.gid, kv.me, oldConfig, newConfig)
		if oldConfig.Num == 0 {
			return kv.syncConfig(&newConfig)
		} else {
			oldShards := oldConfig.GetGroupShardsMap()[kv.gid]
			newShards := newConfig.GetGroupShardsMap()[kv.gid]
			var migrationShards []int
			for _, newSd := range newShards {
				if !contains(oldShards, newSd) {
					migrationShards = append(migrationShards, newSd)
				}
			}

			migrationGroupShardsMap := make(map[int][]int)
			for _, shard := range migrationShards {
				gid := oldConfig.Shards[shard]
				Assert(gid != kv.gid, "gid %d == kv.gid %d", gid, kv.gid)
				if gid != 0 {
					migrationGroupShardsMap[gid] = append(migrationGroupShardsMap[gid], shard)
				}
			}
			Debug(ConfigEvent, "G%d S%d migrationGroupShardsMap = %+v", kv.gid, kv.me, migrationGroupShardsMap)
			for gid, shards := range migrationGroupShardsMap {
				args := GetShardsArgs{
					Config:   newConfig,
					Shards:   shards,
					ClientId: kv.clientId,
					SeqNum:   atomic.AddInt64(&kv.seqNum, 1),
				}
				shardsData := kv.callGetShards(gid, oldConfig.Groups[gid], &args)
				err := kv.syncShards(shardsData)
				if err != OK {
					return err
				}
			}

			return kv.syncConfig(&newConfig)
		}
	}
	return OK
}

func (kv *ShardKV) run() {
	const timeout = 1000 * time.Millisecond
	timer := time.NewTimer(timeout)
	for !kv.Killed() {
		// Debug(InfoEvent, "=====run")
		select {
		case msg := <-kv.applyCh:
			timer.Reset(timeout)
			kv.apply(msg)
		case <-timer.C:
			_, isLeader := kv.raft.GetState()
			// Debug(ConfigEvent, "G%d S%d isLeader=%v=====run timeout", kv.gid, kv.me, isLeader)
			if isLeader {
				kv.changeConfig(kv.ctrlClerk.Query(-1))
			}
			timer.Reset(timeout)
		}
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.raft.Kill()
	// Your code here, if desired.
	kv.killChan <- true
	Debug(InfoEvent, "G%d S%d killed", kv.gid, kv.me)
}

func (kv *ShardKV) Killed() bool {
	return kv.raft.Killed()
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(GetArgs{})
	labgob.Register(GetShardsArgs{})
	labgob.Register(PutShardsArgs{})
	labgob.Register(SetConfigArgs{})
	labgob.Register(shardctrler.Config{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.raft = raft.Make(servers, me, persister, kv.applyCh)
	// Your initialization code here.
	kv.persister = persister
	kv.clientId = nrand()
	kv.cond = sync.NewCond(&kv.mu)
	kv.initStore()
	kv.killChan = make(chan bool, 1)
	// kv.restoreCompleted = true

	kv.clientStatusMap.init()
	kv.groupLeaderMap.init()

	// Use something like this to talk to the shardctrler:
	kv.ctrlClerk = shardctrler.MakeClerk(kv.ctrlers)
	go kv.applier()
	go kv.pollConfig()

	return kv
}
