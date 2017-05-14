package mongo

import (
	"testing"
	. "github.com/seewindcn/GoStore"
	"github.com/seewindcn/GoStore/db"
	"log"
	"strconv"
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

func _saveSomeObj(m db.DB, table string) {
	for i := 0; i < 10; i++ {
		o1 := &Obj1{Id:i, Name:"test_" + strconv.Itoa(i), Sex:i}
		m.Save(table, nil, o1)
		//println("save", i)
	}
}

func TestMongoDB_Load(t *testing.T) {
	var err error
	m := preMongoDB(t)
	defer m.Stop()
	_saveSomeObj(m, "load")
	o1 := &Obj1{}
	for i := 0; i < 6; i++ {
		err = m.RandomLoad("load", o1)
		if err != nil {
			t.Error("MongoDB.RandomLoad err:", err)
		}
		println(">>>RandomLoad:", o1.Name)
	}
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
	log.Printf("****o1:%s, %d", o1, o1.Id)
}

func TestMongoDB_Delete(t *testing.T) {
	m := preMongoDB(t)
	defer m.Stop()
	m.Delete("test1", 1)
	c, err := m.Deletes("test1", M{"name":"idtest2"})
	if err != nil {
		t.Error("Deletes error", err)
	}
	log.Println("******Deletes:", c)
}


