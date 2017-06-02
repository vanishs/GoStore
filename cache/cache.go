package cache

import (
	"fmt"
	"reflect"
	. "github.com/seewindcn/GoStore"
)

type Cache interface {
	// start cache
	Start(config M) error
	// get cached value by key.
	Get(key string) interface{}
	// GetMulti is a batch version of Get.
	GetMulti(keys []string) []interface{}
	// set cached value with key and expire time.
	Put(key string, val interface{}, timeout int) error
	// delete cached value by key.
	Delete(key string) error
	// delete caches by keys
	Deletes(keys []interface{}) (int, error)
	// increase cached int value by key, as a counter.
	Incr(key string) (int64, error)
	// decrease cached int value by key, as a counter.
	Decr(key string) (int64, error)
	// check if cached value exists or not.
	IsExist(key string) bool
	// EXPIRE
	Expire(key string, timeout int) bool
}

type StructCache interface {
	// **********Struct support********** //
	//table suggest use short string if use redis cache,
	// get cache struct by key,
	GetStruct(table, key string, dest interface{}) (exist bool, err error)
	// put struct in cache by key
	PutStruct(table, key string, val interface{}, timeout int) error
	// get struct field, error if no exist
	GetStField(table, key, field string, t reflect.Kind) (val interface{}, err error)
	// set struct field
	SetStField(table, key, field string, val interface{}, forced bool) (exist bool, err error)
	//DelStField(table, key, field string) (bool, error)
	// get all field's names
	GetStFieldNames(table, key string) []string
	// get all fields
	GetStAllFields(table, key string) (fields map[string][]byte, err error)
	// get struct fields, error if no exist
	GetStFields(table, key string, fields []interface{}, types []reflect.Kind) (vals []interface{}, err error)
	// set struct fields
	SetStFields(table, key string, fields []interface{}, vals []interface{}, forced bool) (err error)
	DelStFields(table, key string, fields ...interface{}) (int, error)
}

type SetCache interface {
	SetAdd(key string, members ...interface{}) (int, error)
	SetRemove(key string, members ...interface{}) (int, error)
	SetLen(key string) (int, error)
	SetRandom(key string, count int) ([]string, error)
	SetRandomPop(key string) (string, error)
}

// Instance is a function create a new Cache Instance
type Instance func() Cache

var adapters = make(map[string]Instance)


// Register makes a cache adapter available by the adapter name.
// If Register is called twice with the same name or if driver is nil,
// it panics.
func Register(name string, adapter Instance) {
	if adapter == nil {
		panic("cache: Register adapter is nil")
	}
	if _, ok := adapters[name]; ok {
		panic("cache: Register called twice for adapter " + name)
	}
	adapters[name] = adapter
}

func NewCache(name string) (adapter Cache, err error){
	instFunc, ok := adapters[name]
	if !ok {
		err = fmt.Errorf("cache: unknown adapter name %q", name)
		return
	}
	adapter = instFunc()
	return
}
