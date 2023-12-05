package kvraft

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"sync"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	mu      sync.Mutex
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	// seqNum   int64
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
	// You'll have to add code here.
	return ck
}
func (ck *Clerk) call(method string, args interface{}) interface{} {
	type result struct {
		serverID int
		reply    interface{}
	}

	const timeout = 1000 * time.Millisecond
	t := time.NewTimer(timeout)
	defer t.Stop()

	doneCh := make(chan result, len(ck.servers))

	ck.mu.Lock()
	leaderId := ck.leaderId
	ck.mu.Unlock()
	var r result

	for off := 0; ; off++ {
		id := (leaderId + off) % len(ck.servers)
		go func() {
			switch method {
			case "KVServer.Get":
				reply := GetReply{}
				if ck.servers[id].Call(method, args, &reply) {
					doneCh <- result{id, reply}
				}
			case "KVServer.PutAppend":
				reply := PutAppendReply{}
				if ck.servers[id].Call(method, args, &reply) {
					doneCh <- result{id, reply}
				}
			default:
				panic(fmt.Sprintf("I don't know about method %v!\n", method))
			}

		}()

		select {
		case r = <-doneCh:
			switch method {
			case "KVServer.Get":
				reply := r.reply.(GetReply)
				if reply.Err == ErrWrongLeader {
					continue
				}
			case "KVServer.PutAppend":
				reply := r.reply.(PutAppendReply)
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
	// r = <-doneCh
End:
	ck.mu.Lock()
	ck.leaderId = r.serverID
	ck.mu.Unlock()
	return r.reply
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := GetArgs{
		Key:    key,
		SeqNum: nrand(),
	}

	reply := ck.call("KVServer.Get", &args).(GetReply)

	if reply.Err == OK {
		return reply.Value
	}

	return ""
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:    key,
		Value:  value,
		Op:     op,
		SeqNum: nrand(),
	}

	reply := ck.call("KVServer.PutAppend", &args).(PutAppendReply)

	if reply.Err == OK {
		return
	} else {
		panic(reply.Err)
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
