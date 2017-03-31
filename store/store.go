package store

import (
	"reflect"
	. "github.com/seewindcn/GoStore"
	"github.com/seewindcn/GoStore/db"
	_ "github.com/seewindcn/GoStore/db/mongo"
	"github.com/seewindcn/GoStore/cache"
	_ "github.com/seewindcn/GoStore/cache/redis"
)


type Store struct {
	Cache cache.Cache
	StCache cache.StructCache
	Db db.DB
	TableInfos map[reflect.Type]*TableInfo
}


func New() *Store {
	return &Store{TableInfos:make(map[reflect.Type]*TableInfo)}
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

func (self *Store) NewDB(name string, config M) error {
	db, err := db.NewDB(name, config)
	if err != nil {
		return err
	}
	self.Db = db
	//fmt.Println("NewDB:", db)
	return nil
}

// register table's struct
func (self *Store) RegTable(table string, st reflect.Type, isCache bool) {
	if st == nil {
		panic("store: RegTable st is nil")
	}
	if _, ok := self.TableInfos[st]; ok {
		panic("store: RegTable call twice for table " + table)
	}
	info := &TableInfo{Name:table, IsCache:isCache}
	self.TableInfos[st] = info
	self.Db.RegTable(info, st)
}

