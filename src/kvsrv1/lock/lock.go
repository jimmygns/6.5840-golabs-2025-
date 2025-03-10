package lock

import (
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type lockState string

const (
	unlocked lockState = "-1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	lockKey string

	acquired        bool
	acquiredVersion rpc.Tversion
	lockID          int
}

func lockStateToInt(s lockState) int {
	i, _ := strconv.ParseInt(string(s), 10, 32)
	return int(i)
}

func intToLockState(i int) lockState {
	return lockState(strconv.Itoa(i))
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck, lockKey: l, lockID: rand.Intn(10000)}
	// You may add code here
	return lk
}

func (lk *Lock) Acquire() {
	if lk.acquired {
		return
	}
	// Your code here
	for {
		state, version, err := lk.ck.Get(lk.lockKey)
		if err == rpc.ErrNoKey {
			state = string(intToLockState(lk.lockID))
			err = lk.ck.Put(lk.lockKey, state, 0)
			if err == rpc.OK {
				lk.acquired = true
				lk.acquiredVersion = 1
				break
			} else if err == rpc.ErrMaybe {
				s, v, _ := lk.ck.Get(lk.lockKey)
				if v == 1 && s == string(intToLockState(lk.lockID)) {
					lk.acquired = true
					lk.acquiredVersion = 1
					break
				}
			}
		} else if err == rpc.OK {
			if state == string(unlocked) {
				err = lk.ck.Put(lk.lockKey, string(intToLockState(lk.lockID)), version)
				if err == rpc.OK {
					lk.acquired = true
					lk.acquiredVersion = version + 1
					break
				} else if err == rpc.ErrMaybe {
					s, v, _ := lk.ck.Get(lk.lockKey)
					if v == version+1 && s == string(intToLockState(lk.lockID)) {
						lk.acquired = true
						lk.acquiredVersion = version + 1
						break
					}
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	// Your code here
	if !lk.acquired {
		return
	}
	if err := lk.ck.Put(lk.lockKey, string(unlocked), lk.acquiredVersion); err == rpc.OK || err == rpc.ErrMaybe {
		lk.acquired = false
		lk.acquiredVersion = 0
		return
	}
	fmt.Printf("Release failed\n")
}
