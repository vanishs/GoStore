package db

import (
	. "github.com/seewindcn/GoStore"
	"fmt"
)


type DB interface {
	Start(config M) error
	// insert or modify to db
	Save(table string, obj interface{}) error
	//Load(table, key string, t reflect.Type) (obj interface{}, err error)
	//LoadAll(table string, t reflect.Type) (objs []interface{}, err error)
	//Update(table, key string, fields M) error
}

type TableInfo struct {
	Name string
	IdName string  //main id in struct's field name
	IsCache bool
}

// Instance is a function create a new DB Instance
type Instance func() DB

var adapters = make(map[string]Instance)

// Register makes a cache adapter available by the adapter name.
// If Register is called twice with the same name or if driver is nil,
// it panics.
func Register(name string, adapter Instance) {
	if adapter == nil {
		panic("db: Register adapter is nil")
	}
	if _, ok := adapters[name]; ok {
		panic("db: Register called twice for adapter " + name)
	}
	adapters[name] = adapter
}

func NewDB(name string, config M) (adapter DB, err error){
	instFunc, ok := adapters[name]
	if !ok {
		err = fmt.Errorf("db: unknown adapter name %q", name)
		return
	}
	adapter = instFunc()
	err = adapter.Start(config)
	if err != nil {
		adapter = nil
	}
	return
}
