package kvraft

import (
	"fmt"
	"testing"
)

func TestRand(t *testing.T) {
	for i := 0; i < 100; i++ {
		fmt.Printf("%v ", nrand())
	}
	fmt.Println()
}
