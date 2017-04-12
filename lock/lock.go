package lock

import (
	"time"
)


// lock
type Lock interface {
	Lock() error
	Unlock() bool
	//Extend resets the mutex's expiry and returns the status of expiry extension. It is a run-time error if m is not locked on entry to Extend.
	Extend() bool
}

type Driver interface {
	NewLock(string) Lock
}

//distributed lock mgr
type LockMgr struct {
	d Driver
	Expiry time.Duration
	Tries int
	Delay time.Duration
}

type NewDriver func(mgr *LockMgr, store interface{}) Driver

func New() *LockMgr {
	return &LockMgr{}
}


func (self *LockMgr) Init(store interface{}, driver string, expiry time.Duration, tries int, delay time.Duration) {
	if expiry <= 0 {
		expiry = 8 * time.Second
	}
	if tries <= 0 {
		tries = 32
	}
	if delay <= 0 {
		delay = 500 * time.Millisecond
	}
	self.Expiry = expiry
	self.Tries = tries
	self.Delay = delay
	self.d = drivers[driver](self, store)
}

func (self *LockMgr) NewLock(name string) Lock {
	return self.d.NewLock(name)
}

var drivers = make(map[string]NewDriver)

func Register(name string, new NewDriver) {
	if new == nil {
		panic("lock: Register newFunc is nil")
	}
	if _, ok := drivers[name]; ok {
		panic("lock: Register called twice for driver" + name)
	}
	drivers[name] = new
}

