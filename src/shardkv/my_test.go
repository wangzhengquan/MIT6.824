package shardkv

import (
	"fmt"
	"log"
	"strconv"
	"sync/atomic"
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

func Disalbed_TestJoinLeav2(t *testing.T) {
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

func Disalbed_TestMyUnreliable(t *testing.T) {
	fmt.Printf("Test: my unreliable 2...\n")

	cfg := make_config(t, 3, false, 100)
	defer cfg.cleanup()

	ck := cfg.makeClient()

	cfg.join(0)

	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = randstring(5)
		ck.Put(ka[i], va[i])
	}

	var done int32
	ch := make(chan bool)

	ff := func(i int) {
		defer func() { ch <- true }()
		ck1 := cfg.makeClient()
		for atomic.LoadInt32(&done) == 0 {
			x := randstring(5)
			ck1.Append(ka[i], x)
			va[i] += x
		}
	}

	for i := 0; i < n; i++ {
		go ff(i)
	}

	time.Sleep(150 * time.Millisecond)
	cfg.join(1)
	time.Sleep(500 * time.Millisecond)
	cfg.join(2)
	time.Sleep(500 * time.Millisecond)
	cfg.leave(0)
	time.Sleep(500 * time.Millisecond)
	cfg.leave(1)
	time.Sleep(500 * time.Millisecond)
	cfg.join(1)
	cfg.join(0)

	log.Printf("=========Sleep")
	time.Sleep(2 * time.Second)
	log.Printf("=========Sleep end")

	atomic.StoreInt32(&done, 1)
	// cfg.net.Reliable(true)
	for i := 0; i < n; i++ {
		<-ch
	}

	log.Printf("=========Check")
	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}
	log.Printf("=========Check end")
	fmt.Printf("  ... Passed\n")
}
