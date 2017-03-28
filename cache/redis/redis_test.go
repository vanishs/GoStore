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
)

func preRedis(t *testing.T) cache.Cache {
	config := map[string] interface{} {
		"addr": "127.0.0.1:6379",
	}
	bm, err := cache.NewCache("redis", config)
	if err != nil {
		t.Error("init err")
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
	if err = bm.PutStruct(key, o1, 10); err != nil {
		t.Error("PutStruct", err)
	}

	o2 := Obj{}
	if err = bm.GetStruct(key, &o2); err != nil {
		t.Error("GetStruct", err)
	}
	if o2.Name != "abc" || o2.Sex != 1 || o2.Level != 10 {
		t.Fatalf("GetStruct values(%s) no vaild", o2)
	}
}
