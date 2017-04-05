package GoStore

import (
	"encoding/json"
	"reflect"
	"strconv"
	"log"
	"fmt"
)

var (
	MongodbTestConfig = M{
		"url": "mongodb://127.0.0.1:27017/tmp?maxPoolSize=100&connect=direct",
	}
	RedisTestConfig = M{
		"addr": "127.0.0.1:6379",
	}
)

type M map[string]interface{}

type TableInfo struct {
	Name string
	KeyIndex int
	IsCache bool
	SType reflect.Type
	Params M
}

func NewTableInfo() *TableInfo {
	return &TableInfo{Params:make(M)}
}

func (self *TableInfo) GetKey(obj interface{}) interface{} {
	v := GetValue(obj)
	fv := v.Field(self.KeyIndex)
	return fv.Interface()
}

func (self *TableInfo) GetStrKey(obj interface{}) string {
	v := GetValue(obj)
	fv := v.Field(self.KeyIndex)
	switch fv.Kind() {
	case reflect.Int, reflect.Int32, reflect.Int64:
		return strconv.Itoa(int(fv.Int()))
	case reflect.String:
		return fv.String()
	}
	panic(fmt.Sprintf("GetStrKey no support:%s", obj))
}

type TableInfos map[reflect.Type]*TableInfo

func (self TableInfos) GetTableInfo(obj interface{}) *TableInfo {
	st := GetType(obj)
	info, ok := self[st]
	if !ok {
		return nil
	}
	return info
}

func Json2Map(sjson string)  (M, error) {
	var rs interface{}
	err := json.Unmarshal([]byte(sjson), &rs)
	if err != nil {
		return nil, err
	}
	return rs.(M), nil
}

// get obj's Value, no ptrValue
func GetValue(obj interface{}) reflect.Value {
	var v reflect.Value
	if rs, ok := obj.(reflect.Value); ok {
		v = rs
	} else {
		v = reflect.ValueOf(obj)
	}
	if v.Kind() == reflect.Ptr {  // if obj is pointer,
		v = v.Elem()
	}
	return v
}

func GetType(obj interface{}) reflect.Type {
	var t reflect.Type
	switch inst := obj.(type) {
	case reflect.Type:
		t = inst
	case reflect.Value:
		t = GetValue(inst).Type()
	default:
		t = GetValue(inst).Type()
	}
	return t
}


func init() {
	log.SetFlags(log.Lshortfile | log.LstdFlags)
}