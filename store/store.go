package store

import (
	"reflect"
	. "github.com/seewindcn/GoStore"
	"github.com/seewindcn/GoStore/db"
	_ "github.com/seewindcn/GoStore/db/mongo"
	"github.com/seewindcn/GoStore/cache"
	_ "github.com/seewindcn/GoStore/cache/redis"
	"fmt"
)


type Store struct {
	Cache cache.Cache
	StCache cache.StructCache
	Db db.DB
	Infos TableInfos
}


func New() *Store {
	return &Store{Infos:make(map[reflect.Type]*TableInfo)}
}

func (self *Store) NewCache(name string) error {
	c, err := cache.NewCache(name)
	if err != nil {
		return err
	}
	self.Cache = c
	self.StCache = c.(cache.StructCache)
	return nil
}

func (self *Store) NewDB(name string) error {
	_db, err := db.NewDB(name)
	if err != nil {
		return err
	}
	self.Db = _db
	return nil
}

func (self *Store) Start(dbCfg M, cacheCfg M) error {
	if dbCfg != nil {
		if err := self.Db.Start(self.Infos, dbCfg); err != nil {
			return err
		}
	}
	if cacheCfg != nil {
		if err := self.Cache.Start(cacheCfg); err != nil {
			return err
		}
	}
	return nil
}

// register table's struct
func (self *Store) RegTable(table string, st reflect.Type, isCache bool) {
	if st == nil {
		panic("store: RegTable st is nil")
	}
	if _, ok := self.Infos[st]; ok {
		panic("store: RegTable call twice for table " + table)
	}
	info := NewTableInfo()
	info.Name = table
	info.IsCache = isCache
	info.SType = st
	self.Infos[st] = info
	self.Db.RegTable(info)
}

func (self *Store) Save(obj interface{}) error {
	info := self.Infos.GetTableInfo(obj)
	if info == nil {
		panic(fmt.Sprintf("store save: info no found for obj:%s", obj))
	}
	if err := self.Db.SaveByInfo(info, obj); err != nil {
		return err
	}
	if info.IsCache {
		self.StCache.PutStruct(info.Name, info.GetStrKey(obj), obj, 0)
	}
	return nil
}

func (self *Store) Load(obj interface{}) error {
	info := self.Infos.GetTableInfo(obj)
	if info == nil {
		panic(fmt.Sprintf("store save: info no found for obj:%s", obj))
	}
	if reflect.TypeOf(obj).Kind() != reflect.Ptr {
		panic("store load obj much be struct's pointer")
	}
	return self.Db.LoadByInfo(info, obj)
}

func (self *Store) Loads(query M, obj interface{}) error {
	t := reflect.TypeOf(obj)
	v := t.Elem().Elem()
	if !(t.Kind() == reflect.Ptr && t.Elem().Kind() == reflect.Slice && v.Kind() == reflect.Struct) {
		panic("store loads objs much be []struct pointer")
	}

	info := self.Infos.GetTableInfo(v)
	self.Db.Loads(info.Name, query, obj)
	return nil
}
