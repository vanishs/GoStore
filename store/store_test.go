package store

import (
	"testing"
	. "github.com/seewindcn/GoStore"
	_ "github.com/seewindcn/GoStore/lock/redis"
	"reflect"
	"log"
	"strconv"
)

type Obj1 struct {
	Id int `bson:"_id"`
	Name string
	Sex int
}

type Obj2 struct {
	Id string `json:"id" bson:"_id,omitempty"`
	Name string
	Sex int
}


func TestStore(t *testing.T) {
	store := New()
	println("new store", store)
	if err := store.NewDB("mongodb"); err != nil {
		t.Error("NewDB error:", err)
	}
	if err := store.NewCache("redis"); err != nil {
		t.Error("NewCache error:", err)
	}
	store.RegTable("Obj1", reflect.TypeOf((*Obj1)(nil)).Elem(), true)
	store.RegTable("Obj2", reflect.TypeOf((*Obj2)(nil)).Elem(), true)
	//store.RegTable("Obj1", reflect.TypeOf((*Obj1)(nil)), true)
	if err := store.Start(MongodbTestConfig, RedisTestConfig); err != nil {
		t.Error("store Start error:", err)
	}

	//
	o1 := &Obj1{Id:999, Name:"abc2233", Sex:2}
	if err := store.Save(o1); err != nil {
		t.Error("store save error:", err)
	}
	o2 := &Obj1{Id:o1.Id}
	if err := store.Load(o2, true); err != nil {
		t.Error("store load error:", err)
	}
	log.Printf("store laod:%s", o2)
	if o2.Name != o1.Name || o2.Sex != o1.Sex {
		t.Fatalf("store load error:%s", o2)
	}

	objs := []Obj1{}
	store.Loads(M{"sex":2}, &objs)
	log.Println("*****", len(objs), objs[0])

	o3 := &Obj1{Id:o1.Id, Name:"cacheName"}
	if err := store.CacheObj(o3); err != nil {
		t.Error("store cache error", err)
	}

	// lockMgr
	store.NewLockMgr("redis", 0, 0, 0)
	testIRegistry(store)

	testServiceAgent(store)
}

// test IRegistry
func testIRegistry(reg IRegistry) {
	addr := "127.0.0.1"
	rs, my := reg.CheckAndRegister("players", "uid1", addr)
	if !my && rs != addr {
		log.Printf("CheckAndRegister other:%s\n", rs)
	}
	reg.CheckAndRegister("players", "uid2", addr+"2")
	reg.CheckAndRegister("players", "uid3", addr+"3")
	if rs, ok := reg.CheckAndRegister("players", "uid4", ""); true {
		log.Println("CheckAndRegister only check", ok, rs)
	}
	ok := reg.UnRegister("players", "uid1", addr)
	if !ok {
		log.Println("CheckAndRegister unregister error")
	}
}

func testServiceAgent(store *Store) {
	name := "test"
	service := "test"
	addr := "127.0.0.1:8001"
	c1 := 0
	for i := 0; i < 5; i++ {
		store.ServiceAgent.Register(name + strconv.Itoa(i), service, addr, func() int {
			c1 += 1; return c1
		})
	}
	for i := 0; i < 10; i++ {
		svc := store.ServiceAgent.Dns(service)
		log.Println("~~~", svc)
	}
	store.ServiceAgent.UnRegister(name)
}

