// Copyright 2014 beego Author. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package redis

import (
	"testing"
	"github.com/seewindcn/GoStore/cache"
	"reflect"
	. "github.com/seewindcn/GoStore"
	"log"
)

func preRedis(t *testing.T) cache.Cache {
	var bm cache.Cache
	var err error
	if bm, err = cache.NewCache("redis"); err != nil {
		t.Error("init err", err)
		return nil
	}
	if err := bm.Start(RedisTestConfig); err != nil {
		t.Error("Start err", err)
	}
	return bm
}

func TestRedisCache(t *testing.T) {
	var err error
	bm := preRedis(t)
	if err = bm.Put("redis", 1, 10); err != nil {
		t.Error("set Error", err)
	}
	if !bm.IsExist("redis") {
		t.Error("check err")
	}
	testSet(bm, t)
}

type Obj struct {
	Name string
	Sex int
	Level int
}

func TestStructRedisCache(t *testing.T) {
	var err error
	key := "obj1"
	bm := preRedis(t).(cache.StructCache)
	o1 := &Obj{
		Name:"abc",
		Sex: 1,
		Level: 10,
	}
	table := "obj"
	if err = bm.PutStruct(table, key, o1, 0); err != nil {
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
		t.Fatalf("GetStruct values(%s) no vaild", o2)
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
	val, err := bm.GetStField(table, key, "Name", reflect.String);
	if err != nil {
		t.Error("GetStField", err)
	} else if val.(string) != dest {
		t.Fatalf("GetStField:val(%s) != %s", val, dest)
	}

	// *****GetStFieldNames
	keys := bm.GetStFieldNames(table, key)
	log.Printf("GetStFieldNames:%s, %s", keys, val)
	//if len(keys) != 3 {
	//	t.Fatalf("GetStFieldNames error:%s", keys)
	//}

	vals, err := bm.GetStFields(table, key, Params("Name", "Sex", "Level"),
		[]reflect.Kind{reflect.String, reflect.Int, reflect.Int},
	)
	log.Println("*******", vals)

	c, err := bm.DelStFields(table, key, "Name", "Sex")
	if err != nil {
		t.Error("DelStField", err)
	}
	if c != 2 {
		t.Fatal("DelStField no delete")
	}
	if err := bm.SetStFields(table, key, Params("Name", "Sex"), Params("newName", 2), false); err != nil {
		t.Error("SetStFields", err)
	}
}

func testSet(bm cache.Cache, t *testing.T) {
	key := "set1"
	si := bm.(cache.SetCache)
	si.SetAdd(key, 1,2,3, 4, "a", "b", "c", "ddd")
	si.SetRemove(key, "ddd", 4)
	s1, err := si.SetRandom(key)
	log.Println("tSet:", s1, err)
	s1, err = si.SetRandomPop(key)
	log.Println("tSet:", s1, err)
}
