package raft

//
// Raft tests.
//
// we will use the original test_test.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.
//

import (
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
