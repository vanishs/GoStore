package mongo

import (
	"testing"
	. "github.com/seewindcn/GoStore"
	"fmt"
)

type Obj1 struct {
	Id int "_id"
	Name string
	Sex int
}

func preMongoDB(t *testing.T) (*MongoDB, error) {
	m := New()
	err := m.Start(M{
		"url": "mongodb://127.0.0.1:27018/tmp?maxPoolSize=100&connect=direct",
	})
	if err != nil {
		t.Error("MongoDB new error:", err)
		return nil, err
	}
	return m, nil
}

func TestMongoDB_Save(t *testing.T) {
	m, err := preMongoDB(t)
	o1 := &Obj1{1, "abc", 1}
	err = m.Save("test1", o1)
	if err != nil {
		t.Error("MongoDB.Save err:", err)
	}
	fmt.Printf("****o1:%s, %d", o1, o1.Id)
}

