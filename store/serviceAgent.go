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
	all map[string]*_Service
	store *Store
	allUpdateTime int64
}

func NewStoreServiceAgent(store *Store) *StoreServiceAgent {
	sss := &StoreServiceAgent{
		store: store,
		all: make(map[string]*_Service),
		names: make(map[string]*_Service),
	}
	return sss
}

func (self *StoreServiceAgent) Start() {
	go self._loop()
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
	defer self.Unlock()
	delete(self.names, name)
}

func (self *StoreServiceAgent) Dns(service string) (ip string, port int) {
	self._getAll()
	svc := self._dnsService(service)
	return svc.Ip, svc.Port
}

func (self *StoreServiceAgent) _getAll() {
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
		var svc _Service
		err = json.Unmarshal(v, &svc)
		if err == nil && &svc != nil {
			self.all[svc.Name] = &svc
			//log.Println("~~~", k, &svc)
		}
	}
}

func (self *StoreServiceAgent) _dnsServices(svcName string) []*_Service {
	svcs := []*_Service{}
	for _, svc := range self.all {
		if svc.Service == svcName {
			svcs = append(svcs, svc)
		}
	}
	return svcs
}

func (self *StoreServiceAgent) _dnsService(svcName string) *_Service {
	svcs := self._dnsServices(svcName)
	rand.Seed(time.Now().UnixNano())
	return svcs[rand.Intn(len(svcs))]
}

func (self *StoreServiceAgent) _getField(svc *_Service) string {
	return svc.Service + "-" + svc.Name
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
	s, err := json.Marshal(svc)
	if err != nil {
		log.Printf("[StoreServiceAgent]_update error:%s", err)
		return
	}
	self.store.StCache.SetStField(ServiceTable, "", self._getField(svc), string(s), true)
}

func (self *StoreServiceAgent) _loop() {
	for {
		if len(self.names) > 0 {
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
		time.Sleep(LoopTime * time.Second)
	}
}
