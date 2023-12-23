package shardctrler

import (
	"fmt"
	"log"
	"testing"
)

func printArray(arr []int) {
	for _, v := range arr {
		fmt.Printf("%v ", v)
	}
	fmt.Println()
}

func disable_TestArray(t *testing.T) {
	arr := [3]int{1, 2, 3}
	var arr2 [3]int
	arr2 = arr
	arr2[0] = 8
	printArray(arr[:])
	printArray(arr2[:])
	// printArray(arr[:])

}

func disable_TestConfig(t *testing.T) {
	config := Config{}
	config.Groups = map[int][]string{1: {"a", "b"}}
	config.resetShards()
	log.Printf("config=%+v", config)

	config.joinGroups(map[int][]string{2: {"a", "b"}})
	log.Printf("config=%+v", config)

	config.joinGroups(map[int][]string{3: {"a", "b"}})
	log.Printf("config=%+v", config)

	config.joinGroups(map[int][]string{4: {"a", "b"}})
	log.Printf("config=%+v", config)

	config.joinGroups(map[int][]string{5: {"a", "b"}})
	log.Printf("config=%+v", config)

	config.joinGroups(map[int][]string{6: {"a", "b"}})
	log.Printf("config=%+v", config)

	config.joinGroups(map[int][]string{7: {"a", "b"}})
	log.Printf("config=%+v", config)

	config.joinGroups(map[int][]string{8: {"a", "b"}})
	log.Printf("config=%+v", config)

	config.joinGroups(map[int][]string{9: {"a", "b"}})
	log.Printf("config=%+v", config)

	config.joinGroups(map[int][]string{10: {"a", "b"}})
	log.Printf("config=%+v", config)

	config.joinGroups(map[int][]string{11: {"a", "b"}})
	log.Printf("config=%+v", config)

	// config.removeGroups([]int{1, 2, 3})
	// log.Printf("config=%+v", config)

	// config.joinGroups(map[int][]string{1: {"a", "b"}})
	// log.Printf("config=%+v", config)
}

func TestMove(t *testing.T) {
	const nservers = 3
	cfg := make_config(t, nservers, false)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.All())
	fmt.Printf("Test: Move ...\n")
	var gid3 int = 503
	ck.Join(map[int][]string{gid3: []string{"3a", "3b", "3c"}})
	var gid4 int = 504
	ck.Join(map[int][]string{gid4: []string{"4a", "4b", "4c"}})
	for i := 0; i < NShards; i++ {
		cf := ck.Query(-1)
		if i < NShards/2 {
			ck.Move(i, gid3)
			if cf.Shards[i] != gid3 {
				cf1 := ck.Query(-1)
				if cf1.Num <= cf.Num {
					t.Fatalf("Move should increase Config.Num")
				}
			}
		} else {
			ck.Move(i, gid4)
			if cf.Shards[i] != gid4 {
				cf1 := ck.Query(-1)
				if cf1.Num <= cf.Num {
					t.Fatalf("Move should increase Config.Num")
				}
			}
		}
	}
	cf2 := ck.Query(-1)
	for i := 0; i < NShards; i++ {
		if i < NShards/2 {
			if cf2.Shards[i] != gid3 {
				t.Fatalf("expected shard %v on gid %v actually %v",
					i, gid3, cf2.Shards[i])
			}
		} else {
			if cf2.Shards[i] != gid4 {
				t.Fatalf("expected shard %v on gid %v actually %v",
					i, gid4, cf2.Shards[i])
			}
		}
	}
	ck.Leave([]int{gid3})
	ck.Leave([]int{gid4})
	fmt.Printf("  ... Passed\n")
}

func TestConcurrentLeaveJoin(t *testing.T) {
	const nservers = 3
	cfg := make_config(t, nservers, false)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.All())
	fmt.Printf("Test: Concurrent leave/join ...\n")

	const npara = 10
	var cka [npara]*Clerk
	for i := 0; i < len(cka); i++ {
		cka[i] = cfg.makeClient(cfg.All())
	}
	gids := make([]int, npara)
	ch := make(chan bool)
	for xi := 0; xi < npara; xi++ {
		gids[xi] = int((xi * 10) + 100)
		go func(i int) {
			defer func() { ch <- true }()
			var gid int = gids[i]
			var sid1 = fmt.Sprintf("s%da", gid)
			var sid2 = fmt.Sprintf("s%db", gid)
			cka[i].Join(map[int][]string{gid + 1000: []string{sid1}})
			cka[i].Join(map[int][]string{gid: []string{sid2}})
			cka[i].Leave([]int{gid + 1000})
		}(xi)
	}
	for i := 0; i < npara; i++ {
		<-ch
	}
	check(t, gids, ck)

	fmt.Printf("  ... Passed\n")
}
