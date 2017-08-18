package store

import (
	"fmt"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/vanishs/GoStore"
)

const (
	redisDriver = "goredis"
)

var (
	redisConfig = GoStore.ClusterRedisTestConfig
)

type Obj1 struct {
	Id   int `bson:"_id"`
	Name string
	Sex  int
}

type Obj2 struct {
	Id   string `json:"id" bson:"_id,omitempty"`
	Name string
	Sex  int
}

func TestStore(t *testing.T) {
	store := New()
	println("new store", store)
	if err := store.NewDB("mongodb"); err != nil {
		t.Error("NewDB error:", err)
	}
	if err := store.NewCache(redisDriver); err != nil {
		t.Error("NewCache error:", err)
	}
	store.RegTable("Obj1", reflect.TypeOf((*Obj1)(nil)).Elem(), true,
		&GoStore.DbIndex{Key: []string{"name"}, Unique: true},
	)
	store.RegTable("Obj2", reflect.TypeOf((*Obj2)(nil)).Elem(), true, nil)
	//store.RegTable("Obj1", reflect.TypeOf((*Obj1)(nil)), true)
	if err := store.Start(GoStore.MongodbTestConfig, redisConfig); err != nil {
		t.Error("store Start error:", err)
	}

	//
	o1 := &Obj1{Id: 999, Name: "abc2233", Sex: 2}
	if err := store.Save(o1); err != nil {
		t.Error("store save error:", err)
	}
	o2 := &Obj1{Id: o1.Id}
	if err := store.Load(o2, true); err != nil {
		t.Error("store load error:", err)
	}
	fmt.Printf("store laod:%s\n", o2)
	if o2.Name != o1.Name || o2.Sex != o1.Sex {
		t.Fatalf("store load error:%s\n", o2)
	}

	var objs []Obj1
	store.Loads(GoStore.M{"sex": 2}, &objs, nil)
	fmt.Println("*****", len(objs), objs[0])

	o3 := &Obj1{Id: o1.Id, Name: "cacheName"}
	if err := store.CacheObj(o3); err != nil {
		t.Error("store cache error", err)
	}

	// lockMgr
	store.NewLockMgr(redisDriver, 4*time.Second, 0, 0)
	testIRegistry(store)

	testServiceAgent(store)
	testServiceSingleton(store)
}

// test IRegistry
func testIRegistry(reg GoStore.IRegistry) {
	addr := "127.0.0.1"
	rs, my := reg.CheckAndRegister("players", "uid1", addr)
	if !my && rs != addr {
		fmt.Printf("CheckAndRegister other:%s\n", rs)
	}
	reg.CheckAndRegister("players", "uid2", addr+"2")
	reg.CheckAndRegister("players", "uid3", addr+"3")
	if rs, ok := reg.CheckAndRegister("players", "uid4", ""); true {
		fmt.Println("CheckAndRegister only check", ok, rs)
	}
	ok := reg.UnRegister("players", "uid1", addr)
	if !ok {
		fmt.Println("CheckAndRegister unregister error")
	}
}

func testServiceAgent(store *Store) {
	name := "test"
	service := "test"
	addr := "127.0.0.1:8001"
	c1 := 0
	for i := 0; i < 5; i++ {
		svc := &GoStore.Service{
			Name:    name + strconv.Itoa(i),
			Service: service,
			InAddr:  addr,
			OutAddr: addr,
			UpdateFunc: func() int {
				c1 += 1
				return c1
			},
		}
		store.ServiceAgent.Register(svc)
	}
	for i := 0; i < 10; i++ {
		svc := store.ServiceAgent.Dns(service)
		fmt.Println("~~~", svc)
	}
	store.ServiceAgent.UnRegister(name)
}

func testServiceSingleton(store *Store) {
	name := "singletonTest"

	f1 := func(my string) {
		svcFunc := func(looped *bool) {
			for i := 0; i < 2; i++ {
				if *looped {
					fmt.Printf("[%s]%s.\n", my, i)
				} else {
					fmt.Printf("[%s]CheckSingleton error\n", my)
					break
				}
				time.Sleep(time.Second * time.Duration(1))
			}
			fmt.Printf("[%s]end\n", my)
		}
		ss := NewServiceSingleton(store, name, 4*time.Second, svcFunc)
		for {
			if ss.Start() {
				//fmt.Printf("[%s]stop\n", my)
				break
			}
			time.Sleep(time.Second / 4)
		}
	}
	n := 10
	for i := 0; i < n; i++ {
		go f1("test_" + strconv.Itoa(i))
	}
	time.Sleep(time.Second * time.Duration(n*2+1))
}
