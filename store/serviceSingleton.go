package store

import (
	"log"
	"sync"
	"time"

	"github.com/vanishs/GoStore/lock"
)

type ServiceSingleton struct {
	sync.Mutex
	name    string
	expiry  time.Duration
	lk      lock.Lock
	locked  bool
	svcFunc SvcFunc
}

type SvcFunc func(*bool)

func NewServiceSingleton(store *Store, name string, expiry time.Duration, svcFunc SvcFunc) *ServiceSingleton {
	sss := &ServiceSingleton{
		name:   name,
		expiry: expiry,
		//lk: store.NewLock(name),
		lk:      store.NewLockEx(name, expiry, 1, 0),
		svcFunc: svcFunc,
	}
	return sss
}

func (self *ServiceSingleton) Start() bool {
	self.Lock()
	defer self.Unlock()
	if self.locked {
		return true
	}
	err := self.lk.Lock()
	if err != nil {
		return false
	}
	self.locked = true
	go self._loop()
	return true
}

func (self *ServiceSingleton) _loop() {
	looped := true
	defer func() {
		looped = false
		self.Stop()
		self.Lock()
		self.locked = false
		self.Unlock()
	}()
	go func() {
		for {
			time.Sleep(self.expiry / 2)
			if !looped || !self.lk.Extend() {
				log.Println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
				break
			}
		}
		looped = false
	}()
	self.svcFunc(&looped)
}

func (self *ServiceSingleton) Stop() {
	self.lk.Unlock()
}
