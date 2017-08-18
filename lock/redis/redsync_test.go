package redis

import (
	"fmt"
	"testing"
	"time"

	"github.com/vanishs/GoStore"
	"github.com/vanishs/GoStore/store"
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
	expiry := 5 * time.Second

	s := preStore(t)
	lock := s.NewLockEx("lock_test", expiry, 0, 0)
	err := lock.Lock()
	if err != nil {
		t.Error("lock error")
	}
	defer lock.Unlock()

	lock2 := s.NewLockEx("lock_test", expiry, 0, 0)
	err2 := lock2.Lock()
	if err2 == nil {
		t.Error("lock2 wth")
	}
	fmt.Println("should be:", err2)

	time.Sleep(6 * time.Second)

	err2 = lock2.Lock()
	if err2 != nil {
		t.Error("lock error")
	}

	defer lock2.Unlock()

	time.Sleep(3 * time.Second)
	v := lock2.Extend()
	if !v {
		t.Error("Extend error")
	}

	time.Sleep(3 * time.Second)
	err = lock.Lock()
	if err == nil {
		t.Error("lock wth")
	}
	fmt.Println("should be:", err)

}
