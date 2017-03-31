package store

import (
	. "github.com/seewindcn/GoStore"
	"github.com/seewindcn/GoStore/cache"
	"reflect"
)


type Store struct {
	Cache cache.Cache
	StCache cache.StructCache
}


func New() *Store {
	return &Store{}
}

func (self *Store) NewCache(name string, config M) error {
	c, err := cache.NewCache(name, config)
	if err != nil {
		return err
	}
	self.Cache = c
	self.StCache = c.(cache.StructCache)
	return nil
}

// register table's struct
func (self *Store) RegTable(table string, st reflect.Type, isCache bool) {

}

