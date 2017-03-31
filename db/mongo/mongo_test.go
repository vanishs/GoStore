package mongo

import (
	"testing"
	. "github.com/seewindcn/GoStore"
	"fmt"
	"github.com/seewindcn/GoStore/db"
)

type Obj1 struct {
	Id int `"bson:_id"`
	Name string
	Sex int
}

type Obj2 struct {
	Id string `json:"id" bson:"_id,omitempty"`
	Name string
	Sex int
}

func preMongoDB(t *testing.T) (db.DB, error) {
	m := NewMongoDB()
	err := m.Start(M{
		"url": "mongodb://127.0.0.1:27017/tmp?maxPoolSize=100&connect=direct",
	})
	if err != nil {
		t.Error("MongoDB new error:", err)
		return nil, err
	}
	return m, nil
}

func TestMongoDB_Save(t *testing.T) {
	m, err := preMongoDB(t)
	o1 := &Obj1{ Name:"idtest", Sex:1}
	o2 := &Obj2{Name:"idtest2", Sex:1}
	err = m.Save("test1", o1)
	err = m.Save("test1", o2)
	if err != nil {
		t.Error("MongoDB.Save err:", err)
	}
	fmt.Printf("****o1:%s, %d", o1, o1.Id)
}

