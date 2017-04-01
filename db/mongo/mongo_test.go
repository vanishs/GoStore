package mongo

import (
	"testing"
	. "github.com/seewindcn/GoStore"
	"fmt"
	"github.com/seewindcn/GoStore/db"
)

type Obj1 struct {
	Id int `bson:"_id"`
	Name string
	Sex int
}

type Obj2 struct {
	Id string `json:"id" bson:"_id,omitempty"`
	Name string
	Sex int
}

func preMongoDB(t *testing.T) db.DB {
	m := NewMongoDB()
	err := m.Start(nil, MongodbTestConfig)
	if err != nil {
		t.Error("MongoDB new error:", err)
		return nil
	}
	return m
}

func TestMongoDB_Save(t *testing.T) {
	var err error
	m := preMongoDB(t)
	defer m.Stop()
	o1 := &Obj1{Name:"idtest", Sex:1}
	o2 := &Obj2{Name:"idtest2", Sex:1}
	err = m.Save("test1", nil, o1)
	err = m.Save("test1", nil, o2)
	o1.Id = 1
	err = m.Save("test1", o1.Id, o1)
	if err != nil {
		t.Error("MongoDB.Save err:", err)
	}
	fmt.Printf("****o1:%s, %d", o1, o1.Id)
}

func TestMongoDB_Delete(t *testing.T) {
	m := preMongoDB(t)
	defer m.Stop()
	m.Delete("test1", 1)
	c, err := m.Deletes("test1", M{"name":"idtest2"})
	if err != nil {
		t.Error("Deletes error", err)
	}
	fmt.Println("******Deletes:", c)
}


