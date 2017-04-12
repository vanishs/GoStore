package redis

import (
	"testing"
	"github.com/seewindcn/GoStore/store"
	"github.com/seewindcn/GoStore"
	"time"
	"log"
)

func preStore(t *testing.T) *store.Store{
	s := store.New()
	println("new store", s)
	if err := s.NewCache("redis"); err != nil {
		t.Error("NewCache error:", err)
	}
	if err := s.Start(nil, GoStore.RedisTestConfig); err != nil {
		t.Error("store Start error:", err)
	}
	s.NewLockMgr("redis", 0, 0, 0)
	return s
}

func TestRedisLock(t *testing.T) {
	s := preStore(t)
	lock := s.LockMgr.NewLock("lock_test")
	lock.Lock()
	defer lock.Unlock()
	for i := 0; i < 3; i++ {
		time.Sleep(7 * time.Second)
		if !lock.Extend() {
			t.Error("lock Extend error")
		}
		log.Printf("*****:%s\n", i)
	}
}
