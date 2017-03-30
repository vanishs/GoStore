package db

import (
	"reflect"
	. "github.com/seewindcn/GoStore"
)


type DB interface {
	Start(config M) error
	// insert or modify to db
	Save(table string, obj interface{}) error
	Load(table, key string, t reflect.Type) (obj interface{}, err error)
	LoadAll(table string, t reflect.Type) (objs []interface{}, err error)
	Update(table, key string, fields M) error
}