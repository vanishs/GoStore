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
	Matchs []string
	Dict1 map[string]string  // only support string key
	Dict2 map[string]interface{}
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

	o2 := &Obj2{Id:"obj2"}
	m.Load("test1", "Id", o2)
	log.Printf("***o2:%s, matchs:%s, Dict1:%s, Dict2:%s", o2, o2.Matchs, o2.Dict1, o2.Dict2)
}

func TestMongoDB_Save(t *testing.T) {
	var err error
	m := preMongoDB(t)
	defer m.Stop()
	o1 := &Obj1{Name:"idtest", Sex:1}
	o2 := &Obj2{Id:"obj2", Name:"idtest2", Sex:2}
	o2.Matchs = []string{"a", "b", "c"}
	o2.Dict1 = map[string]string{"1":"a", "2":"b", "3":"d"}
	o2.Dict2 = map[string]interface{}{"1":[]string{"a", "b", "c"}, "2":"b", "3":1, "4":o2.Dict1}
	err = m.Save("test1", o1.Id, o1)
	err = m.Save("test1", o2.Id, o2)
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
	o2 := &Obj2{Name:"idtest2del", Sex:1}
	m.Save("test1", nil, o2)
	c, err := m.Deletes("test1", M{"name":"idtest2del"})
	if err != nil {
		t.Error("Deletes error", err)
	}
	log.Println("******Deletes:", c)
}


