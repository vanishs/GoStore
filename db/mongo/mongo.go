package mongo

import (
	"gopkg.in/mgo.v2"
	. "github.com/seewindcn/GoStore"
	"fmt"
)


type MongoDB struct {
	url string  // like: mongodb://user:pass@127.0.0.1:27017,127.0.0.2:27017/dbname?maxPoolSize=100&connect=direct
	s *mgo.Session
}


func New() *MongoDB {
	return &MongoDB{}
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

func (self *MongoDB) Start(config M) error {
	err := self.config(config)
	if err != nil {
		return err
	}
	self.s, err = mgo.Dial(self.url)
	fmt.Printf("MongoStart:%s", self.s)
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
