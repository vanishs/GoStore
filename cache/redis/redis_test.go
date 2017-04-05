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
	if err = bm.PutStruct(table, key, o1, 10); err != nil {
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
	dest := "ddd"
	reflect.TypeOf(dest)
	exist, err := bm.SetStField(table, key, "Name", dest)
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
	log.Printf("GetStFieldNames:%s", keys)
	if len(keys) != 3 {
		t.Fatalf("GetStFieldNames error:%s", keys)
	}
}
