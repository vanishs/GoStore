package mongo

import (
	"gopkg.in/mgo.v2"
	. "github.com/seewindcn/GoStore"
	"fmt"
	"gopkg.in/mgo.v2/bson"
	"github.com/seewindcn/GoStore/db"
	"strings"
	"reflect"
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
		fmt.Println("autoInc error(1):", err.Error())
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
	fmt.Printf("MongoStart:%s\n", self.url)
	if err != nil {
		return err
	}
	return nil
}

func (self *MongoDB) RegTable(info *TableInfo) error {
	st := info.SType
	c := st.NumField()
	for i := 0; i < c; i++ {
		f := st.Field(i)
		s := f.Tag.Get("bson")
		//fmt.Println("*********", i, f, "----", s)
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
	//fmt.Printf("*********%s, %s\n", v, v.Kind())
	fv := v.Field(info.KeyIndex)
	if info.Params[IS_AUTO_INC].(bool) && fv.Int()==0 {
		fv.SetInt(int64(autoInc(db, info.Name)))
	}
}

// Save insert or modify to db
func (self *MongoDB) Save(table string, obj interface{}) error {
	info := self.Infos.GetTableInfo(obj)
	if info != nil {
		return self.SaveByInfo(info, obj)
	}
	s := self.s.Copy()
	defer s.Close()
	return self._save(s.DB(""), table, obj)
}

func (self *MongoDB) SaveByInfo(info *TableInfo, obj interface{}) error {
	s := self.s.Copy()
	defer s.Close()
	_db := s.DB("")
	v := GetValue(obj)
	self._initAutoInc(_db, info, v)
	return self._save(_db, info.Name, obj)
}

func (self *MongoDB) _save(db *mgo.Database, table string, obj interface{}) error {
	err := db.C(table).Insert(obj)
	if err != nil {
		return err
	}
	return nil
}

func (self *MongoDB) Load(table, key string, obj interface{}) error {
	info := self.Infos.GetTableInfo(obj)
	if info != nil {
		return self.LoadByInfo(info, obj)
	}
	s := self.s.Copy()
	defer s.Close()
	_db := s.DB("")
	kv := GetValue(obj).FieldByName(key).Interface()
	return self._load(_db, table, kv, obj)
}

func (self *MongoDB) LoadByInfo(info *TableInfo, obj interface{}) error {
	s := self.s.Copy()
	defer s.Close()
	_db := s.DB("")
	return self._load(_db, info.Name, info.GetKey(obj), obj)
}

func (self *MongoDB) _load(db *mgo.Database, table string, key, obj interface{}) error {
	return db.C(table).Find(M{ID_FIELD:key}).One(obj)
}

func init() {
	db.Register("mongodb", NewMongoDB)
}