package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	mu       sync.Mutex
	seqNum   int64
	clientId int64
	leaderId int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.clientId = nrand()
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := QueryArgs{
		Num:      num,
		SeqNum:   atomic.AddInt64(&ck.seqNum, 1),
		ClientId: ck.clientId,
	}
	reply := ck.call("ShardCtrler.Query", &args).(*QueryReply)
	return reply.Config

}

func (ck *Clerk) Join(groups map[int][]string) {
	args := JoinArgs{
		Groups:   groups,
		SeqNum:   atomic.AddInt64(&ck.seqNum, 1),
		ClientId: ck.clientId,
	}
	ck.call("ShardCtrler.Join", &args)
}

func (ck *Clerk) Leave(gids []int) {
	args := LeaveArgs{
		GIDs:     gids,
		SeqNum:   atomic.AddInt64(&ck.seqNum, 1),
		ClientId: ck.clientId,
	}
	ck.call("ShardCtrler.Leave", &args)
}

func (ck *Clerk) Move(shard int, gid int) {
	args := MoveArgs{
		Shard:    shard,
		GID:      gid,
		SeqNum:   atomic.AddInt64(&ck.seqNum, 1),
		ClientId: ck.clientId,
	}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	ck.call("ShardCtrler.Move", &args)

}

func (ck *Clerk) call(method string, args interface{}) interface{} {
	type result struct {
		serverID int
		reply    interface{}
	}

	const timeout = 100 * time.Millisecond
	t := time.NewTimer(timeout)
	defer t.Stop()

	doneCh := make(chan result, len(ck.servers))

	ck.mu.Lock()
	leaderId := ck.leaderId
	ck.mu.Unlock()
	var r result

	for off := 0; ; off++ {
		id := (leaderId + off) % len(ck.servers)
		var reply interface{}
		switch method {
		case "ShardCtrler.Query":
			reply = &QueryReply{}
		default:
			reply = &Reply{}
		}

		go func() {
			if ck.servers[id].Call(method, args, reply) {
				doneCh <- result{id, reply}
			}
		}()

		select {
		case r = <-doneCh:
			switch method {
			case "ShardCtrler.Query":
				reply := r.reply.(*QueryReply)
				if reply.Err == WrongLeader {
					continue
				}
			default:
				reply := r.reply.(*Reply)
				if reply.Err == WrongLeader {
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
	ck.mu.Lock()
	ck.leaderId = r.serverID
	ck.mu.Unlock()
	return r.reply
}
