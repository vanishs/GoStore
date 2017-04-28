package mongo

import (
	"gopkg.in/mgo.v2"
	. "github.com/seewindcn/GoStore"
	"gopkg.in/mgo.v2/bson"
	"github.com/seewindcn/GoStore/db"
	"strings"
	"reflect"
	"log"
)

var (
	ID_FIELD = "_id"
	IS_AUTO_INC = "isAutoInc"
	AUTO_INC_TABLE = "_auto_inc_"
	AUTO_INC_NAME = ID_FIELD
	AUTO_INC_ID = "id"
)

type MongoDB struct {
	url string  // like: mongodb://user:pass@127.0.0.1:27017,127.0.0.2:27017/dbname?maxPoolSize=100&connect=direct
	s *mgo.Session
	Infos TableInfos
}


func NewMongoDB() db.DB {
	return &MongoDB{}
}

// auto_inc table
func autoInc(db *mgo.Database, table string) int {
	result := M{}
	c := db.C(AUTO_INC_TABLE)
	if _, err := c.Find(M{AUTO_INC_NAME: table}).Apply(mgo.Change{
		Update:    bson.M{"$set": M{AUTO_INC_NAME: table}, "$inc": M{AUTO_INC_ID: 1}},
		Upsert:    true,
		ReturnNew: true,
	}, &result); err != nil {
		log.Println("autoInc error(1):", err.Error())
	}
	sec, _ := result[AUTO_INC_ID].(int)
	return sec
}

func isInt(k reflect.Kind) bool {
	switch k {
	case reflect.Int, reflect.Int64, reflect.Int32, reflect.Uint64, reflect.Uint, reflect.Uint32:
		return true
	}
	return false;
}


// config
func (self *MongoDB) config(config M) error {
	for key, value := range config {
		if key == "url" {
			self.url = value.(string)
		}
	}
	return nil
}

// register table's struct
//func (self *MongoDB) RegTable(table string, st reflect.Type, isCache bool)

func (self *MongoDB) Start(infos TableInfos, config M) error {
	err := self.config(config)
	if err != nil {
		return err
	}
	self.Infos = infos
	self.s, err = mgo.Dial(self.url)
	log.Printf("MongoStart:%s\n", self.url)
	if err != nil {
		return err
	}
	self._ensureIndexs()
	return nil
}

func (self *MongoDB) _ensureIndexs() {
	s, db := self._getSessionAndDb()
	defer s.Close()
	for _, info := range self.Infos {
		if info.Index == nil {
			continue
		}
		index := mgo.Index{
			Key: info.Index.Key,
			Unique: info.Index.Unique,
			Name: info.Index.Name,
		}
		err := db.C(info.Name).EnsureIndex(index)
		if err != nil {
			log.Printf("_ensureIndexs:%s, %s, error:%s", info.Name, index.Key, err)
		}
	}
}

func (self *MongoDB) Stop() error {
	self.s.Close()
	return nil
}

func (self *MongoDB) RegTable(info *TableInfo) error {
	st := info.SType
	c := st.NumField()
	for i := 0; i < c; i++ {
		f := st.Field(i)
		s := f.Tag.Get("bson")
		//log.Println("*********", i, f, "----", s)
		if strings.Contains(s, ID_FIELD) {
			info.KeyIndex = i
			info.Params[IS_AUTO_INC] = isInt(f.Type.Kind())
			break
		}
	}
	return nil
}

func (self *MongoDB) _initAutoInc(db *mgo.Database, info *TableInfo, v reflect.Value) {
	if info == nil {
		return
	}
	//log.Printf("*********%s, %s\n", v, v.Kind())
	fv := v.Field(info.KeyIndex)
	if info.Params[IS_AUTO_INC].(bool) && fv.Int()==0 {
		fv.SetInt(int64(autoInc(db, info.Name)))
	}
}

func (self *MongoDB) _getSessionAndDb() (*mgo.Session, *mgo.Database) {
	s := self.s.Copy()
	_db := s.DB("")
	return s, _db
}

// Save insert or modify to db
func (self *MongoDB) Save(table string, id, obj interface{}) error {
	s, _db := self._getSessionAndDb()
	defer s.Close()
	return self._save(_db, table, id, obj)
}

func (self *MongoDB) SaveByInfo(info *TableInfo, obj interface{}) error {
	s, _db := self._getSessionAndDb()
	defer s.Close()
	v := GetValue(obj)
	self._initAutoInc(_db, info, v)
	return self._save(_db, info.Name, info.GetKey(obj), obj)
}

func (self *MongoDB) _save(db *mgo.Database, table string, id, obj interface{}) error {
	if id == nil {
		return db.C(table).Insert(obj)
	}
	_, err := db.C(table).UpsertId(id, obj)
	if err != nil {
		return err
	}
	return nil
}

func (self *MongoDB) Load(table, key string, obj interface{}) error {
	s, _db := self._getSessionAndDb()
	defer s.Close()
	kv := GetValue(obj).FieldByName(key).Interface()
	return self._load(_db, table, kv, obj)
}

func (self *MongoDB) LoadByInfo(info *TableInfo, obj interface{}) error {
	s, _db := self._getSessionAndDb()
	defer s.Close()
	return self._load(_db, info.Name, info.GetKey(obj), obj)
}

func (self *MongoDB) Loads(table string, query M, obj interface{}) error {
	s, _db := self._getSessionAndDb()
	defer s.Close()
	return _db.C(table).Find(query).All(obj)
}

func (self *MongoDB) _load(db *mgo.Database, table string, key, obj interface{}) error {
	return db.C(table).Find(M{ID_FIELD:key}).One(obj)
}

func (self *MongoDB) Delete(table string, id interface{}) error {
	s, _db := self._getSessionAndDb()
	defer s.Close()
	return _db.C(table).RemoveId(id)
}

func (self *MongoDB) Deletes(table string, query M) (count int, err error) {
	s, _db := self._getSessionAndDb()
	defer s.Close()
	if info, err := _db.C(table).RemoveAll(query); err != nil {
		return 0, err
	} else {
		return info.Removed, err
	}
}

func init() {
	db.Register("mongodb", NewMongoDB)
}