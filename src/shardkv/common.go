package shardkv

import "6.5840/shardctrler"

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

type Err string

const (
	OK             Err = "OK"
	ErrNoKey       Err = "ErrNoKey"
	ErrWrongGroup  Err = "ErrWrongGroup"
	ErrWrongLeader Err = "ErrWrongLeader"
)

const (
	PUT        = "Put"
	APPEND     = "Append"
	GET        = "Get"
	SET_CONFIG = "SetConfig"
	GET_SHARDS = "GetShards"
	PUT_SHARDS = "PutShards"
	// SYNC_SHARDS = "SyncShards"
)

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	SeqNum   int64
	ClientId int64
}

type Reply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	SeqNum   int64
	ClientId int64
}

type GetReply struct {
	Err   Err
	Value string
}

type GetShardsArgs struct {
	ConfigNum int
	Shards    []int
	SeqNum    int64
	ClientId  int64
}

type GetShardsReply struct {
	Err             Err
	Shards          map[int]map[string]string
	ClientSeqNumMap map[int64]int64
}

type PutShardsArgs struct {
	Shards          map[int]map[string]string
	ClientSeqNumMap map[int64]int64
	SeqNum          int64
	ClientId        int64
}

type SetConfigArgs struct {
	Config   shardctrler.Config
	SeqNum   int64
	ClientId int64
}
