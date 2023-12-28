package shardkv

import (
	"bytes"
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
	if seqNum > cs.lastSeqNum {
		cs.lastSeqNum = seqNum
		cs.cond.Broadcast()
	}

}

type ClientStatusMap struct {
	mu   sync.RWMutex
	data map[int64]*ClientStatus
}

func (m *ClientStatusMap) init() {
	m.data = make(map[int64]*ClientStatus)
}

func (m *ClientStatusMap) getClientSeqNumMap() map[int64]int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	clientSeqNumMap := make(map[int64]int64)
	for seqNum, clientStatus := range m.data {
		clientSeqNumMap[seqNum] = clientStatus.lastSeqNum
	}
	return clientSeqNumMap
}

func (m *ClientStatusMap) putClientSeqNumMap(clientSeqNumMap map[int64]int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for clientId, seqNum := range clientSeqNumMap {
		status, ok := m.data[clientId]
		if !ok {
			status = new(ClientStatus)
			status.init()
			m.data[clientId] = status
		}
		status.updateSeqNum(seqNum)
	}
}

func (m *ClientStatusMap) get(clientId int64) *ClientStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	status, ok := m.data[clientId]
	if !ok {
		status = new(ClientStatus)
		status.init()
		m.data[clientId] = status
	}
	return status
}

type ConfigInPrecessingMap struct {
	mu      sync.RWMutex
	cond    *sync.Cond
	entries map[int]bool
}

func (m *ConfigInPrecessingMap) init() {
	m.entries = map[int]bool{}
	m.cond = sync.NewCond(&m.mu)
}

func (m *ConfigInPrecessingMap) put(num int) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.entries[num] {
		return false
	}
	m.entries[num] = true
	return true
}

func (m *ConfigInPrecessingMap) get(num int) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.entries[num]
}

type ShardKV struct {
	me           int
	raft         *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	mu         sync.RWMutex
	cond       *sync.Cond
	config     shardctrler.Config
	nextConfig shardctrler.Config
	store      [shardctrler.NShards]map[string]string

	ctrlClerk *shardctrler.Clerk
	persister *raft.Persister

	clientStatusMap ClientStatusMap

	seqNum                int64
	clientId              int64
	groupLeaderMap        GroupLeaderMap
	configInPrecessingMap ConfigInPrecessingMap

	servers []*labrpc.ClientEnd
	// killChan chan bool
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

func (kv *ShardKV) setStore(store [shardctrler.NShards]map[string]string, config *shardctrler.Config) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.initStore()
	for shard, data := range store {
		for k, v := range data {
			kv.store[shard][k] = v
		}
	}
	kv.config = (*config).Clone()

}

func (kv *ShardKV) putShards(shards map[int]map[string]string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	for shard, data := range shards {
		if kv.config.Shards[shard] == kv.gid {
			// This condition may be true if the group was shut down and then restarted while the migration was still in progress."
			continue
		}
		kv.store[shard] = make(map[string]string)
		for k, v := range data {
			kv.store[shard][k] = v
		}
		kv.config.Shards[shard] = kv.gid
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

func (kv *ShardKV) deleteShards(configNum int, shards []int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for _, shard := range shards {
		// The condition 'kv.nextConfig.Shards[shard] != kv.gid' serves to prevent a situation where,
		// when G1 leaves and moves shards to G2, then rejoins,
		// some of G2's delete requests, which have experienced latency, arrive while G1 is in the process of migrating data from another group.
		// In this scenario, the shard might be deleted since the migration is still in progress and the new configuration hasn't been updated."
		if kv.config.Shards[shard] != kv.gid && kv.nextConfig.Shards[shard] != kv.gid {
			kv.store[shard] = make(map[string]string)
		}
	}
}

// ===================== store end =======================

func (kv *ShardKV) getConfig() shardctrler.Config {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	return kv.config
}

func (kv *ShardKV) setConfig(config shardctrler.Config) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if config.Num > kv.config.Num {
		kv.config = config
		kv.cond.Broadcast()
	}
}

func (kv *ShardKV) operate(opType string, clientId int64, seqNum int64, args interface{}) Err {
	clientStatus := kv.clientStatusMap.get(clientId)
	clientStatus.mu.Lock()
	defer clientStatus.mu.Unlock()
	if clientStatus.done(seqNum) {
		Debug(InfoEvent, "G%d S%d %s has been done. args= %+v", kv.gid, kv.me, opType, args)
		return OK
	}

	op := Op{Type: opType, Args: args}
	if _, _, ok := kv.raft.Start(op); ok {
		Debug(InfoEvent, "G%d S%d %s start args= %+v, config=%+v", kv.gid, kv.me, opType, args, kv.config)
		for !clientStatus.done(seqNum) {
			clientStatus.cond.Wait()
		}
		return OK
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

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *Reply) {

	reply.Err = kv.operate(args.Op, args.ClientId, args.SeqNum, *args)
}

func (kv *ShardKV) SetConfig(args *SetConfigArgs, reply *Reply) {
	reply.Err = kv.operate(SET_CONFIG, args.ClientId, args.SeqNum, *args)
}

func (kv *ShardKV) GetShards(args *GetShardsArgs, reply *GetShardsReply) {
	// Debug(MigrationEvent, "G%d S%d  GetShards, args=%+v, config=%+v",
	// 	kv.gid, kv.me, args, kv.config)
	kv.mu.Lock()
	for kv.config.Num < args.ConfigNum {
		kv.cond.Wait()
	}
	kv.mu.Unlock()

	reply.Err = kv.operate(GET_SHARDS, args.ClientId, args.SeqNum, *args)
	if reply.Err != OK {
		return
	}
	Debug(MigrationEvent, "G%d S%d  GetShards args=%+v, config=%+v ",
		kv.gid, kv.me, args, kv.config)

	reply.Shards = kv.getShards(args.Shards)
	reply.ClientSeqNumMap = kv.clientStatusMap.getClientSeqNumMap()

}

func (kv *ShardKV) PutShards(args *PutShardsArgs, reply *Reply) {
	reply.Err = kv.operate(PUT_SHARDS, args.ClientId, args.SeqNum, *args)
}

func (kv *ShardKV) DeleteShards(args *DeleteShardsArgs, reply *Reply) {
	reply.Err = kv.operate(DELETE_SHARDS, args.ClientId, args.SeqNum, *args)
}

func (kv *ShardKV) applier() {
	for !kv.Killed() {
		msg := <-kv.applyCh
		kv.apply(msg)
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
		clientStatus := kv.clientStatusMap.get(args.ClientId)
		if !clientStatus.done(args.SeqNum) {
			err = kv.put(args.Key, args.Value)
			if err != ErrWrongGroup {
				clientStatus.updateSeqNum(args.SeqNum)
			}
		}
	case APPEND:
		args := op.Args.(PutAppendArgs)
		Debug(PutEvent, "G%d S%d apply APPEND : args=%+v, config=%+v\n", kv.gid, kv.me, args, kv.config)
		clientStatus := kv.clientStatusMap.get(args.ClientId)
		if !clientStatus.done(args.SeqNum) {
			err = kv.append(args.Key, args.Value)
			if err != ErrWrongGroup {
				clientStatus.updateSeqNum(args.SeqNum)
			}

		}
	case GET:
		args := op.Args.(GetArgs)
		Debug(GetEvent, "G%d S%d apply GET : args=%+v, config=%+v\n", kv.gid, kv.me, args, kv.config)
		clientStatus := kv.clientStatusMap.get(args.ClientId)
		if !clientStatus.done(args.SeqNum) {
			_, err = kv.get(args.Key)
			if err != ErrWrongGroup {
				clientStatus.updateSeqNum(args.SeqNum)
			}

		}
	case GET_SHARDS:
		args := op.Args.(GetShardsArgs)
		Debug(MigrationEvent, "G%d S%d apply GET_SHARDS : args=%+v, config=%+v\n", kv.gid, kv.me, args, kv.config)
		clientStatus := kv.clientStatusMap.get(args.ClientId)
		if !clientStatus.done(args.SeqNum) {
			clientStatus.updateSeqNum(args.SeqNum)
		}
	case PUT_SHARDS:
		args := op.Args.(PutShardsArgs)
		Debug(MigrationEvent, "G%d S%d apply %s , args=%+v, config=%+v\n", kv.gid, kv.me, op.Type, args, kv.config)
		clientStatus := kv.clientStatusMap.get(args.ClientId)
		if !clientStatus.done(args.SeqNum) {
			// Debug(MigrationEvent, "G%d S%d apply %s 1, args=%+v, config=%+v\n", kv.gid, kv.me, op.Type, args, kv.config)
			kv.putShards(args.Shards)
			kv.clientStatusMap.putClientSeqNumMap(args.ClientSeqNumMap)
			clientStatus.updateSeqNum(args.SeqNum)
		}
	case DELETE_SHARDS:
		args := op.Args.(DeleteShardsArgs)
		Debug(MigrationEvent, "G%d S%d apply DELETE_SHARDS : args=%+v, config=%+v\n", kv.gid, kv.me, args, kv.config)
		clientStatus := kv.clientStatusMap.get(args.ClientId)
		if !clientStatus.done(args.SeqNum) {
			kv.deleteShards(args.ConfigNum, args.Shards)
			clientStatus.updateSeqNum(args.SeqNum)
		}

	case SET_CONFIG:
		args := op.Args.(SetConfigArgs)
		Debug(ConfigEvent, "G%d S%d apply SET_CONFIG,  old config=%+v, new config=%+v",
			kv.gid, kv.me, kv.config, args.Config)
		clientStatus := kv.clientStatusMap.get(args.ClientId)
		if !clientStatus.done(args.SeqNum) {
			kv.setConfig(args.Config.Clone())
			clientStatus.updateSeqNum(args.SeqNum)
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
	var store [shardctrler.NShards]map[string]string
	var config shardctrler.Config
	var clientSeqNumMap map[int64]int64
	if d.Decode(&lastIncludedIndex) != nil {
		log.Fatalf("snapshot decode lastIncludedIndex error")
		return
	}
	if d.Decode(&store) != nil {
		log.Fatalf("snapshot decode store error")
		return
	}
	if d.Decode(&config) != nil {
		log.Fatalf("snapshot decode config error")
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
	kv.setStore(store, &config)
	for clientId, seqNum := range clientSeqNumMap {
		clientStatus := kv.clientStatusMap.get(clientId)
		clientStatus.updateSeqNum(seqNum)
	}
}

func (kv *ShardKV) snapshot(logIndex int) {
	Debug(SnapEvent, "G%d S%d snapshot logIndex= %v\n", kv.gid, kv.me, logIndex)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(logIndex)
	e.Encode(kv.store)
	e.Encode(kv.config)
	e.Encode(kv.clientStatusMap.getClientSeqNumMap())
	kv.raft.Snapshot(logIndex, w.Bytes())
}

func (kv *ShardKV) callGetShardsFromGroup(gid int, servers []string, args *GetShardsArgs) *GetShardsReply {
	Debug(MigrationEvent, "G%d S%d callGetShardsFromGroup gid=%d, args=%+v", kv.gid, kv.me, gid, args)
	reply := kv.callOtherGroup("ShardKV.GetShards", gid, servers, args).(*GetShardsReply)
	return reply
}

func (kv *ShardKV) callDeleteShardsFromGroup(gid int, servers []string, args *DeleteShardsArgs) *Reply {
	Debug(MigrationEvent, "G%d S%d callGetShardsFromGroup gid=%d, args=%+v", kv.gid, kv.me, gid, args)
	reply := kv.callOtherGroup("ShardKV.DeleteShards", gid, servers, args).(*Reply)
	return reply
}

func (kv *ShardKV) callPutShardsToGroup(gid int, servers []string, args *PutShardsArgs) {
	Debug(ConfigEvent, "G%d S%d callPutShardsToGroup gid=%d, args=%+v", kv.gid, kv.me, gid, args)
	kv.callOtherGroup("ShardKV.PutShards", gid, servers, args)
}

func (kv *ShardKV) callSetConfigToGroup(gid int, servers []string, args *SetConfigArgs) {
	Debug(ConfigEvent, "G%d S%d callSetConfigToGroup gid=%d, args=%+v", kv.gid, kv.me, gid, args)
	kv.callOtherGroup("ShardKV.SetConfig", gid, servers, args)
}

func (kv *ShardKV) callOtherGroup(method string, gid int, servers []string, args interface{}) interface{} {
	type result struct {
		serverID int
		reply    interface{}
	}

	const timeout = 100 * time.Millisecond
	t := time.NewTimer(timeout)
	defer t.Stop()

	doneCh := make(chan result, len(servers))
	leaderId := kv.groupLeaderMap.get(gid)
	var r result
	var srvs []*labrpc.ClientEnd
	if gid == kv.gid {
		srvs = append(srvs, kv.servers...)
		Assert(len(srvs) > 0, "len(srvs)=%d, len(kv.servers)=%d", len(srvs), len(kv.servers))
	} else {
		for _, server := range servers {
			srvs = append(srvs, kv.make_end(server))
		}
	}

	for off := 0; ; off++ {
		si := (leaderId + off) % len(srvs)
		srv := srvs[si]

		var reply interface{}
		switch method {
		case "ShardKV.GetShards":
			reply = &GetShardsReply{}
		default:
			reply = &Reply{}
		}

		go func() {
			if srv.Call(method, args, reply) {
				doneCh <- result{si, reply}
			}
		}()

		select {
		case r = <-doneCh:
			switch method {
			case "ShardKV.GetShards":
				reply := r.reply.(*GetShardsReply)
				if reply.Err == ErrWrongLeader {
					continue
				}
			default:
				reply := r.reply.(*Reply)
				if reply.Err == ErrWrongLeader {
					continue
				}
			}
			goto End
		case <-t.C:
			// timeout
			t.Reset(timeout)
		}
	}

End:
	kv.groupLeaderMap.put(gid, r.serverID)
	return r.reply
}

func (kv *ShardKV) pollConfig() {
	// var config shardctrler.Config
	for !kv.Killed() {
		if _, isLeader := kv.raft.GetState(); isLeader {
			kv.changeConfig()
		}
		time.Sleep(100 * time.Millisecond)
	}

}

func (kv *ShardKV) changeConfig() {
	oldConfig := kv.getConfig()
	if kv.nextConfig.Num <= oldConfig.Num {
		kv.nextConfig = kv.ctrlClerk.Query(oldConfig.Num + 1)
	}
	newConfig := kv.nextConfig
	// Debug(ConfigEvent, "G%d S%d 1changeConfig, oldConfig=%+v, newConfig=%+v", kv.gid, kv.me, oldConfig, newConfig)
	if newConfig.Num == oldConfig.Num {
		return
	}
	if !kv.configInPrecessingMap.put(newConfig.Num) {
		return
	}
	Assert(newConfig.Num > oldConfig.Num, "newConfig.Num=%d, oldConfig.Num=%d", newConfig.Num, oldConfig.Num)
	Debug(ConfigEvent, "G%d S%d changeConfig, oldConfig=%+v, newConfig=%+v", kv.gid, kv.me, oldConfig, newConfig)
	if oldConfig.Num > 0 {
		kv.migration(&oldConfig, &newConfig)
	}

	setConfigArgs := SetConfigArgs{
		Config:   newConfig,
		ClientId: kv.clientId,
		SeqNum:   atomic.AddInt64(&kv.seqNum, 1),
	}
	kv.callSetConfigToGroup(kv.gid, oldConfig.Groups[kv.gid], &setConfigArgs)

}

func (kv *ShardKV) migration(oldConfig, newConfig *shardctrler.Config) {
	groupShardsMap := kv.getMigrationFromGroupShardsMap(oldConfig, newConfig)
	if len(groupShardsMap) < 0 {
		return
	}
	type result struct {
		gid   int
		reply *GetShardsReply
	}
	ch1 := make(chan result)
	for gid, shards := range groupShardsMap {
		go func(gid int, shards []int) {
			getShardsArgs := GetShardsArgs{
				ConfigNum: newConfig.Num,
				Shards:    shards,
				ClientId:  kv.clientId,
				SeqNum:    atomic.AddInt64(&kv.seqNum, 1),
			}
			getShardsReply := kv.callGetShardsFromGroup(gid, oldConfig.Groups[gid], &getShardsArgs)
			ch1 <- result{gid, getShardsReply}
		}(gid, shards)

	}

	ch2 := make(chan bool)
	for i := 0; i < len(groupShardsMap); i++ {
		r := <-ch1
		putShardsArgs := PutShardsArgs{
			Shards:          r.reply.Shards,
			ClientSeqNumMap: r.reply.ClientSeqNumMap,
			ClientId:        kv.clientId,
			SeqNum:          atomic.AddInt64(&kv.seqNum, 1),
		}
		kv.callPutShardsToGroup(kv.gid, newConfig.Groups[kv.gid], &putShardsArgs)

		go func(gid int, shards []int) {
			deleteShardsArgs := DeleteShardsArgs{
				ConfigNum: newConfig.Num,
				Shards:    shards,
				ClientId:  kv.clientId,
				SeqNum:    atomic.AddInt64(&kv.seqNum, 1),
			}
			kv.callDeleteShardsFromGroup(gid, oldConfig.Groups[gid], &deleteShardsArgs)
			ch2 <- true
		}(r.gid, groupShardsMap[r.gid])
	}

	for i := 0; i < len(groupShardsMap); i++ {
		<-ch2
	}
}

func (kv *ShardKV) getMigrationFromGroupShardsMap(oldConfig, newConfig *shardctrler.Config) map[int][]int {
	oldShards := oldConfig.GetGroupShardsMap()[kv.gid]
	newShards := newConfig.GetGroupShardsMap()[kv.gid]
	var migrationShards []int
	for _, newSd := range newShards {
		if !contains(oldShards, newSd) {
			migrationShards = append(migrationShards, newSd)
		}
	}

	migrationFromGroupShardsMap := make(map[int][]int)
	for _, shard := range migrationShards {
		gid := oldConfig.Shards[shard]
		Assert(gid != kv.gid, "gid %d == kv.gid %d", gid, kv.gid)
		if gid != 0 {
			migrationFromGroupShardsMap[gid] = append(migrationFromGroupShardsMap[gid], shard)
		}
	}
	return migrationFromGroupShardsMap
}

func contains(arr []int, o int) bool {
	for _, v := range arr {
		if v == o {
			return true
		}
	}
	return false
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.raft.Kill()
	// Your code here, if desired.
	// kv.killChan <- true
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
	labgob.Register(DeleteShardsArgs{})
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
	// Use something like this to talk to the shardctrler:
	kv.servers = servers
	kv.ctrlClerk = shardctrler.MakeClerk(kv.ctrlers)
	kv.persister = persister
	kv.clientId = nrand()
	kv.cond = sync.NewCond(&kv.mu)
	kv.initStore()
	kv.configInPrecessingMap.init()

	kv.clientStatusMap.init()
	kv.groupLeaderMap.init()
	kv.groupLeaderMap.put(kv.gid, kv.me)

	go kv.applier()
	go kv.pollConfig()

	return kv
}
