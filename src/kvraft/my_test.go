package kvraft

import (
	"fmt"
	"log"
	"strconv"
	"testing"
	"time"
)

func Disable_TestRand(t *testing.T) {
	for i := 0; i < 100; i++ {
		fmt.Printf("%v ", nrand())
	}
	fmt.Println()
}

func Disable_TestMain(t *testing.T) {
	cs1 := ClientStatus{lastSeqNum: 11}
	cs2 := cs1
	cs2.lastSeqNum = 22
	fmt.Println(cs1, cs2)
}

func Disable_TestSimple(t *testing.T) {
	title := "Test: Simple"
	nservers := 3
	crash := true
	maxraftstate := -1
	unreliable := false

	cfg := make_config(t, nservers, unreliable, maxraftstate)
	defer cfg.cleanup()

	cfg.begin(title)

	ck := cfg.makeClient(cfg.All())
	for i := 0; i < 1000; i++ {
		ck.Put(strconv.Itoa(i), strconv.Itoa(i))
	}

	key := "999"
	if ck.Get(key) != key {
		t.Fatalf("get %s != %s", key, ck.Get(key))
	}
	if crash {
		log.Printf("=====shutdown servers\n")
		for i := 0; i < nservers; i++ {
			cfg.ShutdownServer(i)
		}
		// Wait for a while for servers to shutdown, since
		// shutdown isn't a real crash and isn't instantaneous
		time.Sleep(electionTimeout)
		// log.Printf("restart servers\n")
		// crash and re-start all
		log.Printf("=====start servers\n")
		for i := 0; i < nservers; i++ {
			cfg.StartServer(i)
		}
		cfg.ConnectAll()
	}
	log.Printf("===== sleep ... \n")
	time.Sleep(5 * time.Second)
	// log.Printf("===== check \n")
	// if ck.Get(key) != key {
	// 	t.Fatalf("get %s != %s", key, ck.Get(key))
	// }
	cfg.end()
}
