package kvraft

import (
	"fmt"
	"testing"
)

func Disable_TestRand(t *testing.T) {
	for i := 0; i < 100; i++ {
		fmt.Printf("%v ", nrand())
	}
	fmt.Println()
}

func TestMain(t *testing.T) {
	cs1 := ClientStatus{lastSeqNum: 11}
	cs2 := cs1
	cs2.lastSeqNum = 22
	fmt.Println(cs1, cs2)
}
