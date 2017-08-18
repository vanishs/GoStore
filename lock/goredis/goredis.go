package goredis

import (
	"errors"
	"time"

	"github.com/go-redis/redis"
	"github.com/vanishs/GoStore"
	"github.com/vanishs/GoStore/lock"

	golock "github.com/bsm/redis-lock"

	goredis "github.com/vanishs/GoStore/cache/goredis"
)

const (
	// LOCK_PRE LOCK_PRE
	LOCK_PRE = "_lk_"
)

// RedisDriver RedisDriver
type RedisDriver struct {
	mgr *lock.LockMgr
	cc  redis.UniversalClient
	opt golock.LockOptions
}

// New New
func New(mgr *lock.LockMgr, st interface{}) lock.Driver {
	c := GoStore.GetValue(st).FieldByName("Cache").Interface().(*goredis.RedisCache)
	//s := st.(*store.Store)
	//c := s.Cache.(*redis.RedisCache)
	d := &RedisDriver{
		mgr: mgr,
		cc:  c.GetCC(),
	}
	d.Init()
	return d
}

// Init Init
func (self *RedisDriver) Init() {
	self.opt = golock.LockOptions{
		LockTimeout: self.mgr.Expiry,
		WaitRetry:   self.mgr.Delay,
		WaitTimeout: self.mgr.Delay * time.Duration(self.mgr.Tries),
	}
}

type myLock struct {
	L *golock.Lock
}

func (gl *myLock) Lock() error {

	ok, err := gl.L.Lock()
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("could no lock")
	}
	return nil

}
func (gl *myLock) Unlock() bool {
	err := gl.L.Unlock()
	if err != nil {
		return false
	}
	return true
}
func (gl *myLock) Extend() bool {
	ok, err := gl.L.Lock()
	if err != nil {
		return false
	}
	if !ok {
		return false
	}
	return true
}

// NewLock NewLock
func (self *RedisDriver) NewLock(name string) lock.Lock {
	mx := golock.NewLock(self.cc, LOCK_PRE+name, &self.opt)
	rv := &myLock{
		L: mx,
	}
	return rv
}

// NewLockEx NewLockEx
func (self *RedisDriver) NewLockEx(name string, expiry time.Duration, tries int, delay time.Duration) lock.Lock {
	optex := golock.LockOptions{
		LockTimeout: expiry,
		WaitRetry:   delay,
		WaitTimeout: delay * time.Duration(tries),
	}
	mx := golock.NewLock(self.cc, LOCK_PRE+name, &optex)
	rv := &myLock{
		L: mx,
	}
	return rv
}

func init() {
	lock.Register("goredis", New)
}
