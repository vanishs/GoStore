package store

import (
	. "github.com/seewindcn/GoStore"
	"sync"
	"time"
	"log"
	"encoding/json"
	"math/rand"
)

const (
	ServiceTable = "service"
	LoopTime = 10
)

type StoreServiceAgent struct {
	sync.Mutex
	names map[string]*Service	//{name:Service}
	all map[string]*Service	//{key:Service}
	store *Store
	allUpdateTime int64
}

func NewStoreServiceAgent(store *Store) *StoreServiceAgent {
	sss := &StoreServiceAgent{
		store: store,
		all: make(map[string]*Service),
		names: make(map[string]*Service),
	}
	return sss
}

func (self *StoreServiceAgent) Start() {
	go self._loop()
}

func (self *StoreServiceAgent) Register(name, service, addr string, stateUpdate ServiceStateUpdate) {
	svc := &Service{
		Name: name,
		Service: service,
		Addr: addr,
		LoadCount: 0,
		UpdateFunc: stateUpdate,
	}
	self.Lock()
	self.names[name] = svc
	self.Unlock()
	self._update(svc)
}

func (self *StoreServiceAgent) _delete(svc *Service) {
	self.store.StCache.DelStFields(ServiceTable, "", svc.GetKey())
}

func (self *StoreServiceAgent) UnRegister(name string) {
	self.Lock()
	svc, ok := self.names[name]
	if !ok {
		self.Unlock()
		return
	}
	delete(self.names, name)
	self.Unlock()
	self._delete(svc)
}

func (self *StoreServiceAgent) Dns(service string) *Service {
	self.refresh()
	svc := self._dnsService(service)
	return svc
}

func (self *StoreServiceAgent) DnsByName(service, name string) *Service {
	self.refresh()
	svc, ok := self.all[GetServiceKey(service, name)]
	return If(ok, svc, nil).(*Service)
}

func (self *StoreServiceAgent) refresh() {
	ctime := time.Now().Unix()
	if ctime - self.allUpdateTime < LoopTime {
		return
	}
	self.allUpdateTime = ctime
	for k := range self.all {
		delete(self.all, k)
	}
	fields, err := self.store.StCache.GetStAllFields(ServiceTable, "")
	if err != nil {
		return
	}
	for _,v := range fields {
		var svc Service
		err = json.Unmarshal(v, &svc)
		if err == nil && &svc != nil {
			//log.Println("~~~", k, &svc)
			if ctime - svc.UpdateTime < LoopTime * 3 {
				self.all[svc.GetKey()] = &svc
			} else {
				go func(svc *Service) {
					self._delete(svc)
				}(&svc)
			}
		}
	}
}

func (self *StoreServiceAgent) _dnsServices(svcName string) []*Service {
	svcs := []*Service{}
	for _, svc := range self.all {
		if svc.Service == svcName {
			svcs = append(svcs, svc)
		}
	}
	return svcs
}

func (self *StoreServiceAgent) _dnsService(svcName string) *Service {
	svcs := self._dnsServices(svcName)
	rand.Seed(time.Now().UnixNano())
	if len(svcs) > 0 {
		return svcs[rand.Intn(len(svcs))]
	}
	return nil
}

func (self *StoreServiceAgent) _update(svc *Service) {
	defer func() {
		if r := recover(); r != nil {
			PrintRecover(r)
			log.Printf("[StoreServiceAgent]_update error:%s", r)
		}
	}()
	if svc.UpdateFunc != nil {
		svc.LoadCount = svc.UpdateFunc()
	}
	svc.UpdateTime = time.Now().Unix()
	s, err := json.Marshal(svc)
	if err != nil {
		log.Printf("[StoreServiceAgent]_update error:%s", err)
		return
	}
	self.store.StCache.SetStField(ServiceTable, "", svc.GetKey(), string(s), true)
}

func (self *StoreServiceAgent) _loop() {
	for {
		if len(self.names) > 0 {
			//log.Println("[StoreServiceAgent]update services")
			self.Lock()
			svcs := make([]*Service, 0, len(self.names))
			for _,v := range self.names {
				svcs = append(svcs, v)
			}
			self.Unlock()
			for _,v := range svcs {
				self._update(v)
			}
		}
		time.Sleep(LoopTime * time.Second)
	}
}
