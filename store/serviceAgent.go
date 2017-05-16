package store

import (
	"encoding/json"
	. "github.com/seewindcn/GoStore"
	"log"
	"math/rand"
	"sync"
	"time"
)

const (
	ServiceTable = "service"
	LoopTime     = 10
)

type StoreServiceAgent struct {
	sync.Mutex
	names         map[string]*Service //{serviceName:Service}
	all           map[string]*Service //{key:Service}
	store         *Store
	allUpdateTime int64
}

func NewStoreServiceAgent(store *Store) *StoreServiceAgent {
	sss := &StoreServiceAgent{
		store:         store,
		all:           make(map[string]*Service),
		names:         make(map[string]*Service),
		allUpdateTime: 0,
	}
	return sss
}

func (self *StoreServiceAgent) Start() {
	go self._loop()
}

func (self *StoreServiceAgent) Register(name, service, addr string, stateUpdate ServiceStateUpdate) {
	log.Println("[!]StoreServiceAgent.Register:", name, service)
	svc := &Service{
		Name:       name,
		Service:    service,
		Addr:       addr,
		LoadCount:  0,
		UpdateFunc: stateUpdate,
	}
	self.Lock()
	self.names[service] = svc
	self.Unlock()
	self._update(svc)
}

func (self *StoreServiceAgent) _delete(svc *Service) {
	self.store.StCache.DelStFields(ServiceTable, "", svc.GetKey())
}

func (self *StoreServiceAgent) UnRegister(service string) {
	log.Println("[!]StoreServiceAgent.UnRegister:", service)
	self.Lock()
	svc, ok := self.names[service]
	if !ok {
		self.Unlock()
		return
	}
	delete(self.names, service)
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

func (self *StoreServiceAgent) DnsAll(service string) []*Service {
	self.refresh()
	svcs := self._dnsServices(service)
	return svcs
}

func (self *StoreServiceAgent) refresh() {
	ctime := time.Now().Unix()
	if ctime-self.allUpdateTime < LoopTime {
		return
	}
	self.allUpdateTime = ctime
	for k := range self.all {
		delete(self.all, k)
	}
	fields, err := self.store.StCache.GetStAllFields(ServiceTable, "")
	if err != nil {
		log.Println("[!]StoreServiceAgent.refresh error:", err)
		panic(err)
		return
	}
	for _, v := range fields {
		var svc Service
		err = json.Unmarshal(v, &svc)
		if err == nil && &svc != nil {
			//log.Println("~~~", k, &svc)
			if ctime-svc.UpdateTime < LoopTime*3 {
				self.all[svc.GetKey()] = &svc
			} else {
				go func(svc *Service) {
					self._delete(svc)
				}(&svc)
			}
		}
	}
	log.Println("StoreServiceAgent.refresh", len(self.all), len(fields))
}

func (self *StoreServiceAgent) _dnsServices(svcName string) []*Service {
	svcs := []*Service{}
	for _, svc := range self.all {
		//log.Println("_dnsServices", svc.Service, svcName)
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
	//log.Println("[StoreServiceAgent]update service:", svc.GetKey())
	self.store.StCache.SetStField(ServiceTable, "", svc.GetKey(), string(s), true)
}

func (self *StoreServiceAgent) _loop() {
	for {
		if len(self.names) > 0 {
			//log.Println("[StoreServiceAgent]update services", len(self.names))
			self.Lock()
			svcs := make([]*Service, 0, len(self.names))
			for _, v := range self.names {
				svcs = append(svcs, v)
			}
			self.Unlock()
			for _, v := range svcs {
				self._update(v)
			}
		}
		time.Sleep((LoopTime - 2) * time.Second)
	}
}
