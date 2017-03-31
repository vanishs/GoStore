package mongo

import (
	"gopkg.in/mgo.v2"
	. "github.com/seewindcn/GoStore"
	"fmt"
	"gopkg.in/mgo.v2/bson"
	"github.com/seewindcn/GoStore/db"
)

var (
	AUTO_INC_TABLE = "_auto_inc_"
	AUTO_INC_NAME = "name"
	AUTO_INC_ID = "id"
)

type MongoDB struct {
	url string  // like: mongodb://user:pass@127.0.0.1:27017,127.0.0.2:27017/dbname?maxPoolSize=100&connect=direct
	s *mgo.Session
}


func NewMongoDB() db.DB {
	return &MongoDB{}
}

// auto_inc table
func autoInc(db mgo.Database, table string) uint64 {
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
	return uint64(sec)
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

func (self *MongoDB) Start(config M) error {
	err := self.config(config)
	if err != nil {
		return err
	}
	self.s, err = mgo.Dial(self.url)
	fmt.Printf("MongoStart:%s\n", self.url)
	if err != nil {
		return err
	}
	return nil
}

// Save insert or modify to db
func (self *MongoDB) Save(table string, obj interface{}) error {
	s := self.s.Copy()
	defer s.Close()
	err := s.DB("").C(table).Insert(obj)
	if err != nil {
		return err
	}
	return nil
}

func init() {
	db.Register("mongodb", NewMongoDB)
}