package GoStore

import (
	"encoding/json"
	"reflect"
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
	IsCache bool
	SType reflect.Type
	Params M
}

func NewTableInfo() *TableInfo {
	return &TableInfo{Params:make(M)}
}

type TableInfos map[reflect.Type]*TableInfo

func (self TableInfos) GetTableInfo(obj interface{}) *TableInfo {
	st := GetValue(obj).Type()
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
	v := reflect.ValueOf(obj)
	if v.Kind() == reflect.Ptr {  // if obj is pointer,
		v = v.Elem()
	}
	return v
}


