package store


import (
	"github.com/seewindcn/GoStore/lock"
	"time"
)

type ServiceSingleton struct {
	name string
	expiry time.Duration
	lk lock.Lock
	locked bool
}

func NewServiceSingleton(store *Store, name string, expiry time.Duration) *ServiceSingleton {
	sss := &ServiceSingleton{
		name: name,
		expiry: expiry,
		//lk: store.NewLock(name),
		lk: store.NewLockEx(name, expiry, 1, 0),
	}
	return sss
}


func (self *ServiceSingleton) Start() bool {
	if self.locked {
		return true
	}
	err := self.lk.Lock()
	if err != nil {
		return false
	}
	self.locked = true
	go func() {
		for {
			time.Sleep(self.expiry / 2)
			if !self.locked || !self.lk.Extend() {
				break
			}
		}
		self.locked = false
	}()
	return true
}

func (self *ServiceSingleton) Stop() {
	self.lk.Unlock()
	self.locked = false
}

func (self *ServiceSingleton) CheckSingleton() bool {
	return self.locked
}


