package redis

import (
	"log"
	"testing"
	"time"

	"github.com/seewindcn/GoStore"
	"github.com/seewindcn/GoStore/store"
)

func preStore(t *testing.T) *store.Store {
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
	expiry := time.Duration(5)

	s := preStore(t)
	lock := s.NewLockEx("lock_test", expiry, 0, 0)
	lock.Lock()
	defer lock.Unlock()
	for i := 0; i < 3; i++ {
		time.Sleep(expiry * time.Second)
		if !lock.Extend() {
			t.Error("lock Extend error")
		}
		log.Printf("*****:%s\n", i)
	}
}
