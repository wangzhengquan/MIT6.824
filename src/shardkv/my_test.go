package shardkv

import (
	"fmt"
	"log"
	"strconv"
	"testing"
	"time"
)

// const linearizabilityCheckTimeout = 1 * time.Second

func init() {
	debugInit()
}

// test static 2-way sharding, without shard movement.
func Disalbed_Test1(t *testing.T) {
	fmt.Printf("Test: static shards ...\n")

	cfg := make_config(t, 3, false, -1)
	defer cfg.cleanup()
	cfg.join(0)

	ck := cfg.makeClient()
	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		// log.Printf("=====1-%d", i)
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = randstring(20)
		ck.Put(ka[i], va[i])
	}

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	log.Printf("====ShutdownGroup\n\n")
	cfg.ShutdownGroup(0)
	cfg.checklogs() // forbid snapshots

	log.Printf("=======StartGroup\n\n")
	cfg.StartGroup(0)
	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}
	// time.Sleep(5 * time.Second)

	fmt.Printf("  ... Passed\n")
}

func TestJoinLeav2(t *testing.T) {
	fmt.Printf("Test: join, and leave 2...\n")

	cfg := make_config(t, 3, false, -1)
	defer cfg.cleanup()

	ck := cfg.makeClient()

	cfg.join(0)

	n := 30
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = randstring(20)
		ck.Put(ka[i], va[i])
	}
	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	cfg.join(1)
	cfg.join(2)
	cfg.leave(0)

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
		x := randstring(20)
		ck.Append(ka[i], x)
		va[i] += x
	}

	cfg.leave(1)
	cfg.join(0)

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
		x := randstring(20)
		ck.Append(ka[i], x)
		va[i] += x
	}

	time.Sleep(1 * time.Second)

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	time.Sleep(1 * time.Second)

	cfg.checklogs()

	cfg.ShutdownGroup(0)
	cfg.ShutdownGroup(1)
	cfg.ShutdownGroup(2)

	cfg.StartGroup(0)
	cfg.StartGroup(1)
	cfg.StartGroup(2)

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	fmt.Printf("  ... Passed\n")
}
