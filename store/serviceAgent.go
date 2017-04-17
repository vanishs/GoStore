package store

import (
	. "github.com/seewindcn/GoStore"
	"sync"
	"time"
	"log"
)

type _Service struct {
	Name string
	Service string
	Ip string
	Port int
	LoadCount int
	updateFunc ServiceStateUpdate
}

type StoreServiceAgent struct {
	sync.Mutex
	names map[string]*_Service
	store *Store
}

func NewStoreServiceAgent(store *Store) *StoreServiceAgent {
	sss := &StoreServiceAgent{
		store: store,
		names: make(map[string]*_Service),
	}
	go sss._loop()
	return sss
}

func (self *StoreServiceAgent) Register(name, service, ip string, port int, stateUpdate ServiceStateUpdate) {
	svc := &_Service{
		Name: name,
		Service: service,
		Ip: ip,
		Port: port,
		LoadCount: 0,
		updateFunc: stateUpdate,
	}
	self.Lock()
	self.names[name] = svc
	self.Unlock()
	self._update(svc)
}

func (self *StoreServiceAgent) UnRegister(name string) {
	self.Lock()
	delete(self.names, name)
	self.Unlock()
}

func (self *StoreServiceAgent) Dns(service string) (ip string, port int) {
	return "", 0
}

func (self *StoreServiceAgent) _update(svc *_Service) {
	defer func() {
		if r := recover(); r != nil {
			PrintRecover(r)
			log.Printf("[StoreServiceAgent]_update error:%s", r)
		}
	}()
	if svc.updateFunc != nil {
		svc.LoadCount = svc.updateFunc()
	}

}

func (self *StoreServiceAgent) _loop() {
	for {
		if len(self.names) {
			self.Lock()
			svcs := make([]*_Service, 0, len(self.names))
			for _,v := range self.names {
				svcs = append(svcs, v)
			}
			self.Unlock()
			for _,v := range svcs {
				self._update(v)
			}
		}
		time.Sleep(30 * time.Second)
	}
}
