package store

import (
	"testing"
	. "github.com/seewindcn/GoStore"
	"reflect"
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


func TestNew(t *testing.T) {
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
	o1 := &Obj1{Name:"abc", Sex:2}
	if err := store.Save(o1); err != nil {
		t.Error("store save error:", err)
	}
}
