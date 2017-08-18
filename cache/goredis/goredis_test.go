package goredis

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/vanishs/GoStore"
	"github.com/vanishs/GoStore/cache"
)

func preRedis(t *testing.T) cache.Cache {
	var bm cache.Cache
	var err error
	if bm, err = cache.NewCache("goredis"); err != nil {
		t.Error("init err", err)
		return nil
	}
	if err := bm.Start(GoStore.ClusterRedisTestConfig); err != nil {
		t.Error("Start err", err)
	}
	return bm
}

func TestRedisCache(t *testing.T) {
	var err error
	bm := preRedis(t)
	if err = bm.Put("keyxxx", 1, 10); err != nil {
		t.Error("set Error", err)
	}
	if !bm.IsExist("keyxxx") {
		t.Error("check err")
	}
	testSet(bm, t)

}

func testSet(bm cache.Cache, t *testing.T) {
	key := "set1"
	si := bm.(cache.SetCache)
	si.SetAdd(key, 1, 2, 3, 4, "a", "b", "c", "ddd")
	si.SetRemove(key, "ddd", 4)
	keys, err := si.SetRandom(key, 2)
	fmt.Println("tSet:", keys, err)
	s1, err := si.SetRandomPop(key)
	fmt.Println("tSet:", s1, err)
	ls := bm.Keys("*")
	fmt.Println("1**********Keys:", ls)

}

type Obj struct {
	Name   string
	Sex    int
	Level  int
	IsBool bool
}

func TestStructRedisCache(t *testing.T) {
	var err error
	key := "obj1"
	key2 := "obj2"
	bm := preRedis(t).(cache.StructCache)
	o1 := &Obj{
		Name:   "abc",
		Sex:    1,
		Level:  10,
		IsBool: true,
	}
	table := "{obj}"
	if err = bm.PutStruct(table, key, o1, 1); err != nil {
		t.Error("PutStruct", err)
	}
	//time.Sleep(11*time.Second)

	// *********GetStruct
	o2 := Obj{}
	ok, err := bm.GetStruct(table, key, &o2)
	if err != nil {
		t.Error("GetStruct", err)
	} else if !ok {
		t.Error("GetStruct NoFound")
	} else if o2.Name != "abc" || o2.Sex != 1 || o2.Level != 10 {
		t.Fatalf("GetStruct values(%x) no vaild", o2)
	}

	// ******SetStField
	//key = ""
	dest := "ddd"
	exist, err := bm.SetStField(table, key, "Name", dest, true)
	if err != nil {
		t.Error("SetStField", err)
	} else if !exist {
		t.Fatal("SetStField:no exist")
	}

	// *****GetStField
	val, err := bm.GetStField(table, key, "Name", reflect.String)
	if err != nil {
		t.Error("GetStField", err)
	} else if val.(string) != dest {
		t.Fatalf("GetStField:val(%s) != %s", val, dest)
	}

	val, err = bm.GetStField(table, key, "Sex", reflect.Int)
	if err != nil {
		t.Error("GetStField", err)
	} else if val.(int) != 1 {
		t.Fatalf("GetStField:val(%s) != %s", val, dest)
	}

	val, err = bm.GetStField(table, key, "IsBool", reflect.Bool)
	if err != nil {
		t.Error("GetStField", err)
	} else if val.(bool) != true {
		t.Fatalf("GetStField:val(%s) != %s", val, dest)
	}

	// *****GetStFieldNames
	keys := bm.GetStFieldNames(table, key)
	fmt.Printf("GetStFieldNames:%s, %s\n", keys, val)
	//if len(keys) != 3 {
	//	t.Fatalf("GetStFieldNames error:%s", keys)
	//}

	vals, err := bm.GetStFields(table, key, GoStore.Params("Name", "Sex", "Level"),
		[]reflect.Kind{reflect.String, reflect.Int, reflect.Int},
	)
	fmt.Println("*******", vals)

	cc := bm.(cache.Cache)

	rv := cc.Rename(table+"-"+key, table+"-"+key2)
	if false == rv {
		t.Error("rename fail")
	}

	c, err := bm.DelStFields(table, key2, "Name", "Sex")
	if err != nil {
		t.Error("DelStField", err)
	}
	if c != 2 {
		t.Fatal("DelStField no delete")
	}

	c, err = bm.DelStFields(table, key, "Name", "Sex")
	if err != nil {
		t.Error("DelStField", err)
	}
	if c == 2 {
		//rename key->ke2!!!!!!!!!
		t.Fatal("DelStField wth!")
	}

	if err := bm.SetStFields(table, key2, GoStore.Params("Name", "Sex"), GoStore.Params("newName", 2), false); err != nil {
		t.Error("SetStFields", err)
	}
	vals, err = bm.GetStFields(table, key2, GoStore.Params("Name", "Sex", "Level"),
		[]reflect.Kind{reflect.String, reflect.Int, reflect.Int},
	)
	fmt.Println("*******", vals)

}
