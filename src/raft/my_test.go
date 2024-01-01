package raft

//
// Raft tests.
//
// we will use the original test_test.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.
//

import (
	"fmt"
	"log"
	"math/rand"
	"testing"
	"time"
)

// The tester generously allows solutions to complete elections in one second
// (much more than the paper's range of timeouts).
// const RaftElectionTimeout = 1000 * time.Millisecond

func disable_TestNetworkFailure2a(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2A): election after network failure")

	leader1 := cfg.checkOneLeader()

	// if the leader disconnects, a new one should be elected.
	Debug(TestEvent, leader1, "if the leader disconnects, a new one should be elected.")
	cfg.disconnect(leader1)
	cfg.checkOneLeader()

	// if the old leader rejoins, that shouldn't
	// disturb the new leader. and the old leader
	// should switch to follower.
	Debug(TestEvent, leader1, "the old leader rejoins, it should switch to follower")
	cfg.connect(leader1)
	leader2 := cfg.checkOneLeader()

	// if there's no quorum, no new leader should
	// be elected.
	Debug(TestEvent, leader2, "S%d if there's no quorum, no new leader should be elected.", (leader2+1)%servers)
	cfg.disconnect(leader2)
	cfg.disconnect((leader2 + 1) % servers)
	time.Sleep(2 * RaftElectionTimeout)

	// check that the one connected server
	// does not think it is the leader.
	cfg.checkNoLeader()

	// if a quorum arises, it should elect a leader.
	Debug(TestEvent, (leader2+1)%servers, "if a quorum arises, it should elect a leader.")
	cfg.connect((leader2 + 1) % servers)
	cfg.checkOneLeader()

	// re-join of last node shouldn't prevent leader from existing.
	Debug(TestEvent, leader2, "re-join of last node shouldn't prevent leader from existing.")
	cfg.connect(leader2)
	cfg.checkOneLeader()

	cfg.end()
}

func disable_TestBackup2b(t *testing.T) {
	servers := 5
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2B): leader backs up quickly over incorrect follower logs")

	cfg.one(rand.Int(), servers, true)

	// put leader and one follower in a partition
	leader1 := cfg.checkOneLeader()
	Debug(TestEvent, leader1, "%d put leader and one follower in a partition", (leader1+1)%servers)
	cfg.disconnect((leader1 + 2) % servers)
	cfg.disconnect((leader1 + 3) % servers)
	cfg.disconnect((leader1 + 4) % servers)

	// submit lots of commands that won't commit
	Debug(TestEvent, leader1, "submit lots of commands that won't commit")
	for i := 0; i < 50; i++ {
		cfg.rafts[leader1].Start(rand.Int())
	}

	time.Sleep(RaftElectionTimeout / 2)

	cfg.disconnect((leader1 + 0) % servers)
	cfg.disconnect((leader1 + 1) % servers)

	// allow other partition to recover
	Debug(TestEvent, (leader1+2)%servers, "%d %d allow other partition to recover",
		(leader1+3)%servers, (leader1+4)%servers)
	cfg.connect((leader1 + 2) % servers)
	cfg.connect((leader1 + 3) % servers)
	cfg.connect((leader1 + 4) % servers)

	// lots of successful commands to new group.
	Debug(TestEvent, (leader1+2)%servers, "%d %d lots of successful commands to new group.",
		(leader1+3)%servers, (leader1+4)%servers)
	for i := 0; i < 50; i++ {
		cfg.one(rand.Int(), 3, true)
	}

	// now another partitioned leader and one follower
	leader2 := cfg.checkOneLeader()
	other := (leader1 + 2) % servers
	if leader2 == other {
		other = (leader2 + 1) % servers
	}
	Debug(TestEvent, other, "network disconnect")
	Debug(TestEvent, leader2, "now another partitioned leader and one follower")
	cfg.disconnect(other)

	// lots more commands that won't commit
	Debug(TestEvent, leader2, "lots more commands that won't commit")
	for i := 0; i < 50; i++ {
		cfg.rafts[leader2].Start(rand.Int())
	}

	time.Sleep(RaftElectionTimeout / 2)

	// bring original leader back to life,
	for i := 0; i < servers; i++ {
		cfg.disconnect(i)
	}
	Debug(TestEvent, leader1, "%d %d  reconnected, others two disconnected", (leader1+1)%servers, other)
	cfg.connect((leader1 + 0) % servers)
	cfg.connect((leader1 + 1) % servers)
	cfg.connect(other)

	// lots of successful commands to new group.
	Debug(TestEvent, leader1, "%d %d  lots of successful commands to new group.", (leader1+1)%servers, other)
	for i := 0; i < 50; i++ {
		cfg.one(rand.Int(), 3, true)
	}

	// now everyone
	Debug(TestEvent, leader1, "everyone connect.")
	for i := 0; i < servers; i++ {
		cfg.connect(i)
	}
	cfg.one(rand.Int(), servers, true)

	cfg.end()
}

func run(ch chan int) {
	log.Printf("run begin\n")
	defer log.Printf("run end\n")
	i := 0
	for {
		var v int = -1
		var ok bool
		select {
		case v, ok = <-ch:
			if !ok {
				break
			}
		case <-time.After(2 * time.Millisecond):
			log.Printf("time after=%v", time.Now())
		}
		log.Printf("%d value=%v", i, v)
		i++
		time.Sleep(3 * time.Millisecond)
	}
}

func run2(ch chan int) {
	log.Printf("run begin\n")
	defer log.Printf("run end\n")
	var timeout time.Duration = 2 * time.Millisecond
	t := time.NewTimer(timeout)
	i := 0
	for {
		var v int = -1
		var ok bool
		defer t.Stop()
		select {
		case v, ok = <-ch:
			if !ok {
				return
			}
			// t.Reset(timeout)
		case <-t.C:
			// t.Reset(timeout)
			log.Printf("timeout=%v", time.Now())
		}
		log.Printf("%d value=%v", i, v)
		i++
		time.Sleep(3 * time.Millisecond)
		t.Reset(timeout)
	}
}

func triggerCh(ch chan int, v int) {
	select {
	case ch <- v:
		log.Printf("trigger suc %d", v)
	default:
		log.Printf("trigger fail %d", v)
	}
}

func closeCh(ch chan int) {
	close(ch)
	ch = nil
}

func D_TestChan(t *testing.T) {
	ch := make(chan int, 1)

	fmt.Printf("TestChan begin length ch=%d\n", len(ch))
	defer fmt.Printf("TestChan end\n")

	go run(ch)
	// time.Sleep(1 * time.Millisecond)
	// triggerCh(ch, 1)
	// time.Sleep(1 * time.Millisecond)
	// triggerCh(ch, 2)
	// time.Sleep(1 * time.Millisecond)
	// triggerCh(ch, 3)
	for i := 0; i < 5; i++ {
		time.Sleep(1 * time.Millisecond)
		triggerCh(ch, i)

		// ch <- i

	}

	// t.Fatalf("TestChan")
	// time.Sleep(20 * time.Millisecond)

}
