package store

import (
	"testing"
	. "github.com/seewindcn/GoStore"
	"reflect"
	"fmt"
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
	if err := store.Load(o2); err != nil {
		t.Error("store load error:", err)
	}
	fmt.Printf("store laod:%s", o2)
	if o2.Name != o1.Name || o2.Sex != o1.Sex {
		t.Fatalf("store load error:%s", o2)
	}

	objs := []Obj1{}
	store.Loads(M{"sex":2}, &objs)
	fmt.Println("*****", len(objs), objs[0])
}
