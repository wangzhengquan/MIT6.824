package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labrpc"
	"6.5840/shardctrler"
)

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type GroupLeaderMap struct {
	mu      sync.RWMutex
	entries map[int]int
}

func (groups *GroupLeaderMap) init() {
	groups.entries = map[int]int{}
}

func (groups *GroupLeaderMap) get(gid int) int {
	groups.mu.RLock()
	defer groups.mu.RUnlock()
	return groups.entries[gid]
}

func (groups *GroupLeaderMap) put(gid, leader int) {
	groups.mu.Lock()
	defer groups.mu.Unlock()
	groups.entries[gid] = leader
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	mu             sync.Mutex
	seqNum         int64
	clientId       int64
	groupLeaderMap GroupLeaderMap
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.groupLeaderMap.init()
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key:      key,
		SeqNum:   atomic.AddInt64(&ck.seqNum, 1),
		ClientId: ck.clientId,
	}

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			reply := ck.callOtherGroup("ShardKV.Get", gid, servers, &args)
			if reply != nil {
				getReply := reply.(*GetReply)
				if getReply.Err == OK {
					return getReply.Value
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}

	// return ""
}

// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		SeqNum:   atomic.AddInt64(&ck.seqNum, 1),
		ClientId: ck.clientId,
	}

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			reply := ck.callOtherGroup("ShardKV.PutAppend", gid, servers, &args)
			if reply != nil && reply.(*Reply).Err == OK {
				return
			}

		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PUT)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, APPEND)
}

func (ck *Clerk) callOtherGroup(method string, gid int, servers []string, args interface{}) interface{} {
	type result struct {
		serverID int
		reply    interface{}
	}

	const timeout = 100 * time.Millisecond
	t := time.NewTimer(timeout)
	defer t.Stop()

	doneCh := make(chan result, len(servers))
	leaderId := ck.groupLeaderMap.get(gid)
	var r result

	for off := 0; off < len(servers); off++ {
		si := (leaderId + off) % len(servers)
		srv := ck.make_end(servers[si])
		var reply interface{}
		switch method {
		case "ShardKV.GetShards":
			reply = &GetShardsReply{}
		case "ShardKV.Get":
			reply = &GetReply{}
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
			case "ShardKV.Get":
				reply := r.reply.(*GetReply)
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
	ck.groupLeaderMap.put(gid, r.serverID)
	return r.reply
}
