package cache

import (
	"fmt"
)

type Cache interface {
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
	// start cache
	Start(config map[string]interface{}) error

}

type StructCache interface {
	// **********Struct support********** //
	// get cache struct by key
	GetStruct(key string, dest interface{}) error
	PutStruct(key string, val interface{}, timeout int) error
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

func NewCache(name string, config map[string]interface{}) (adapter Cache, err error){
	instFunc, ok := adapters[name]
	if !ok {
		err = fmt.Errorf("cache: unknown adapter name %q", name)
		return
	}
	adapter = instFunc()
	err = adapter.Start(config)
	if err != nil {
		adapter = nil
	}
	return
}
