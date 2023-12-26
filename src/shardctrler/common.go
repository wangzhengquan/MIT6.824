package shardctrler

import (
	"fmt"
	"sort"
)

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(groups) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// =============== Config ===========================================================
// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

func GetSortedKeysOfGroups(groups map[int][]string) []int {
	var gids []int
	for gid, _ := range groups {
		gids = append(gids, gid)
	}
	sort.Ints(gids)
	return gids
}

func (config *Config) GetGroupShardsMap() map[int][]int {
	groupShardsMap := make(map[int][]int)
	for gid, _ := range config.Groups {
		groupShardsMap[gid] = make([]int, 0)
	}
	for shard, gid := range config.Shards {
		groupShardsMap[gid] = append(groupShardsMap[gid], shard)
	}
	return groupShardsMap
}

func (config *Config) GetGidWithCmpNumShards(groupShardsMap map[int][]int, cmp func(a, b int) bool) int {
	if len(groupShardsMap) == 0 {
		return 0
	}
	var gids []int
	for gid, _ := range groupShardsMap {
		gids = append(gids, gid)
	}
	sort.Ints(gids) // make sure map iteration is deterministic.

	m_gid, m_ShardsCount := gids[0], len(groupShardsMap[gids[0]])
	for i := 1; i < len(gids); i++ {
		gid := gids[i]
		shards := groupShardsMap[gid]
		if cmp(len(shards), m_ShardsCount) {
			m_gid, m_ShardsCount = gid, len(shards)
		}
	}

	return m_gid
}

func (config *Config) GetGidWithMinimumShards(goupShardMap map[int][]int) int {
	return config.GetGidWithCmpNumShards(goupShardMap, func(a, b int) bool {
		return a < b
	})
}

func (config *Config) GetGidWithMaximumShards(goupShardMap map[int][]int) int {
	if _, ok := goupShardMap[0]; ok {
		return 0
	}
	return config.GetGidWithCmpNumShards(goupShardMap, func(a, b int) bool {
		return a > b
	})
}

func (config *Config) Clone() Config {
	newConfig := Config{Num: config.Num, Shards: config.Shards, Groups: make(map[int][]string)}
	for gid, servers := range config.Groups {
		newConfig.Groups[gid] = make([]string, len(servers))
		copy(newConfig.Groups[gid], servers)
	}
	return newConfig
}

func (config *Config) ResetShards() {
	gids := GetSortedKeysOfGroups(config.Groups) // make sure map iteration is deterministic.
	for shard := 0; shard < NShards; {
		for _, gid := range gids {
			config.Shards[shard] = gid
			shard++
			if shard >= NShards {
				break
			}
		}
	}

	// log.Printf("resetShards: config= %+v", config)
}

func (config *Config) JoinGroups(groups map[int][]string) {
	gids := GetSortedKeysOfGroups(groups) // make sure map iteration is deterministic.
	for _, new_gid := range gids {
		new_servers := groups[new_gid]
		if _, ok := config.Groups[new_gid]; !ok {
			for cnt := 0; cnt < NShards; cnt++ {
				// find the goup with maximum shards, move one of that goup's shards to new gid
				groupShardsMap := config.GetGroupShardsMap()
				delete(groupShardsMap, new_gid) // delete new_gid itself to keep from getGidWithMaximumShards returning this gid
				maxGid := config.GetGidWithMaximumShards(groupShardsMap)
				maxGroupShards := groupShardsMap[maxGid]
				if cnt >= len(maxGroupShards) && maxGid != 0 {
					break
				}
				config.Shards[maxGroupShards[0]] = new_gid
			}
		}
		config.Groups[new_gid] = append(config.Groups[new_gid], new_servers...)
	}
	// log.Printf("joinGroups: groups=%+v, config=%+v", groups, config)
}

func (config *Config) RemoveGroups(gids []int) {
	for _, gid := range gids {
		if _, ok := config.Groups[gid]; ok {
			removeShards := config.GetGroupShardsMap()[gid]
			delete(config.Groups, gid)
			for _, rmShard := range removeShards {
				// find the goup with minimum shards, move the removed group's shards to that group
				groupShardsMap := config.GetGroupShardsMap()
				delete(groupShardsMap, gid) // delete gid itself to keep from getGidWithMinimumShards returning this gid
				config.Shards[rmShard] = config.GetGidWithMinimumShards(groupShardsMap)
			}
		}
	}

	// log.Printf("removeGroups: %+v, config=%+v", gids, config)
}

func (config *Config) MoveShard(shard int, gid int) {
	config.Shards[shard] = gid
	// log.Printf("moveShard: shard=%v,gid=%v, config=%+v", shard, gid, config)
}

// =============== Config End===========================================================

type Err int

const (
	OK          Err = iota
	WrongLeader Err = iota
)

type JoinArgs struct {
	Groups   map[int][]string // new GID -> servers mappings
	SeqNum   int64
	ClientId int64
}

func (args *JoinArgs) String() string {
	var pic string = "{Groups:["
	for gid, _ := range args.Groups {
		pic += fmt.Sprintf("%d, ", gid)
	}
	pic += "], "
	pic += fmt.Sprintf("SeqNum=%v, ClientId=%v}", args.SeqNum, args.ClientId)
	return pic
}

type LeaveArgs struct {
	GIDs     []int
	SeqNum   int64
	ClientId int64
}

type MoveArgs struct {
	Shard    int
	GID      int
	SeqNum   int64
	ClientId int64
}

type QueryArgs struct {
	Num      int // desired config number
	SeqNum   int64
	ClientId int64
}

type Reply struct {
	Err Err
}

type QueryReply struct {
	Err    Err
	Config Config
}
