package db

import (
	. "github.com/seewindcn/GoStore"
	"fmt"
)


type DB interface {
	Start(infos TableInfos, config M) error
	Stop() error
	// register table
	RegTable(info *TableInfo) error
	// insert or modify to db
	Save(table string, id, obj interface{}) error
	SaveByInfo(info *TableInfo, obj interface{}) error
	Load(table, key string, obj interface{}) error
	LoadByInfo(info *TableInfo, obj interface{}) error
	Loads(table string, query M, obj interface{}) error
	RandomLoad(table string, obj interface{}) error
	Delete(table string, id interface{}) error
	Deletes(table string, query M) (count int, err error)
	//Update(table, key string, fields M) error
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

func NewDB(name string) (adapter DB, err error){
	instFunc, ok := adapters[name]
	if !ok {
		err = fmt.Errorf("db: unknown adapter name %q", name)
		return
	}
	adapter = instFunc()
	return
}
