package goredis

import (
	"errors"
	"reflect"
	"strings"
	"time"

	"github.com/go-redis/redis"
	"github.com/mitchellh/mapstructure"
	"github.com/vanishs/GoStore"
	"github.com/vanishs/GoStore/cache"
)

var (
	// ErrNoExist redis no exist error
	ErrNoExist = errors.New("no exist error")
	// ErrTimeout redis timeout error
	ErrTimeout = errors.New("timeout")
	// ErrNoSupporKind redis Kind no support
	ErrNoSupporKind = errors.New("Kind no support")
)

// RedisCache Cache is Redis cache adapter.
type RedisCache struct {
	cc         redis.UniversalClient
	addr       string
	dbNum      int
	pwd        string
	mastername string
}

// New create new redis cache with default collection name.
func New() cache.Cache {
	return &RedisCache{}
}

func _redis2value(t reflect.Kind, val *redis.StringCmd) (interface{}, error) {
	switch t {
	case reflect.String:
		return val.Val(), nil
	case reflect.Bool:
		switch val.Val() {
		case "1", "t", "T", "true", "TRUE", "True":
			return true, nil
		case "0", "f", "F", "false", "FALSE", "False":
			return false, nil
		default:
			return nil, redis.Nil
		}
	case reflect.Int:
		v, e := val.Int64()
		return int(v), e
	case reflect.Uint:
		v, e := val.Uint64()
		return uint(v), e
	case reflect.Int8:
		v, e := val.Int64()
		return int8(v), e
	case reflect.Uint8:
		v, e := val.Uint64()
		return uint8(v), e
	case reflect.Int16:
		v, e := val.Int64()
		return int16(v), e
	case reflect.Uint16:
		v, e := val.Uint64()
		return uint16(v), e
	case reflect.Int32:
		v, e := val.Int64()
		return int32(v), e
	case reflect.Uint32:
		v, e := val.Uint64()
		return uint32(v), e
	case reflect.Int64:
		v, e := val.Int64()
		return int64(v), e
	case reflect.Uint64:
		v, e := val.Uint64()
		return uint64(v), e
	case reflect.Float32:
		v, e := val.Float64()
		return float32(v), e
	case reflect.Float64:
		v, e := val.Float64()
		return float64(v), e
	}
	return nil, ErrNoSupporKind
}

func _redisJoin(key string, values []interface{}) []interface{} {
	var ss []interface{}
	ss = append(ss, key)
	ss = append(ss, values...)
	return ss
}

// config
func (rc *RedisCache) config(config GoStore.M) error {
	for key, value := range config {
		if key == "addr" {
			rc.addr = value.(string)
		} else if key == "dbNum" {
			rc.dbNum = value.(int)
		} else if key == "pwd" {
			rc.pwd = value.(string)
		} else if key == "mastername" {
			rc.mastername = value.(string)
		}
	}
	return nil
}
func (rc *RedisCache) GetCC() redis.UniversalClient {
	return rc.cc
}

//Start start cache
func (rc *RedisCache) Start(config GoStore.M) error {
	err := rc.config(config)
	if err != nil {
		return err
	}

	rc.cc = redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs:      strings.Split(rc.addr, ";"),
		Password:   rc.pwd,
		DB:         rc.dbNum,
		MasterName: rc.mastername,
	})

	return nil
}

// Get cache from redis.
func (rc *RedisCache) Get(key string) interface{} {
	v, err := rc.cc.Get(key).Result()
	if err != nil {
		return nil
	}
	return v
}

// GetMulti get cache from redis.
func (rc *RedisCache) GetMulti(keys []string) []interface{} {
	var rv []interface{}

	for _, key := range keys {
		v, err := rc.cc.Get(key).Result()
		if err != nil {
			rv = append(rv, err)
		} else {
			rv = append(rv, v)
		}
	}

	return rv
}

// Put put cache to redis.
func (rc *RedisCache) Put(key string, val interface{}, timeout int) error {
	return rc.cc.Set(key, val, time.Duration(timeout)*time.Second).Err()
}

// Delete delete cache in redis.
func (rc *RedisCache) Delete(key string) error {
	return rc.cc.Del(key).Err()
}

//Deletes delete caches by keys
func (rc *RedisCache) Deletes(keys []interface{}) (int, error) {

	ss := []string{}
	for _, v := range keys {
		ss = append(ss, v.(string))
	}
	v, e := rc.cc.Del(ss...).Result()
	return int(v), e
}

// Incr increase counter in redis.
func (rc *RedisCache) Incr(key string) (int64, error) {
	return rc.cc.Incr(key).Result()
}

// Decr decrease counter in redis.
func (rc *RedisCache) Decr(key string) (int64, error) {
	return rc.cc.Decr(key).Result()
}

// IsExist check cache's existence in redis.
func (rc *RedisCache) IsExist(key string) bool {
	v, e := rc.cc.Exists(key).Result()
	if e != nil || v == 0 {
		return false
	}
	return true
}

// Expire EXPIRE
func (rc *RedisCache) Expire(key string, timeout int) bool {
	v, e := rc.cc.Expire(key, time.Duration(timeout)*time.Second).Result()
	if e != nil {
		return false
	}
	return v
}

func (rc *RedisCache) fullKey(table, key string) string {
	if key == "" {
		return table
	}
	return table + "-" + key
}

func struct2Map(obj interface{}) map[string]interface{} {
	t := reflect.TypeOf(obj).Elem()
	v := reflect.ValueOf(obj).Elem()

	var data = make(map[string]interface{})
	for i := 0; i < t.NumField(); i++ {
		data[t.Field(i).Name] = v.Field(i).Interface()
	}
	return data
}

// PutStruct PutStruct
func (rc *RedisCache) PutStruct(table, key string, val interface{}, timeout int) error {
	fkey := rc.fullKey(table, key)
	p := rc.cc.Pipeline()

	p.HMSet(fkey, struct2Map(val))
	if 0 != timeout {
		p.Expire(fkey, time.Duration(timeout)*time.Second)
	}

	_, err := p.Exec()

	return err
}

// GetStruct get cache struct by key
func (rc *RedisCache) GetStruct(table, key string, dest interface{}) (bool, error) {
	fkey := rc.fullKey(table, key)
	v, err := rc.cc.HGetAll(fkey).Result()
	if err != nil {
		return false, err
	}
	if len(v) == 0 {
		return false, nil
	}
	err = mapstructure.WeakDecode(v, dest)
	if err != nil {
		return false, err
	}
	return true, nil
}

// GetStField get struct field
func (rc *RedisCache) GetStField(table, key, field string, t reflect.Kind) (val interface{}, err error) {

	fkey := rc.fullKey(table, key)
	v := rc.cc.HGet(fkey, field)

	err = v.Err()

	if err != nil {
		if err == redis.Nil {
			err = cache.ErrNil
		}
		return "", err
	}

	val, err = _redis2value(t, v)

	if err == redis.Nil {
		err = cache.ErrNil
		val = ""
	}

	return val, err
}

// SetStField set struct field
func (rc *RedisCache) SetStField(table, key, field string, val interface{}, forced bool) (exist bool, err error) {
	fkey := rc.fullKey(table, key)
	if !forced && !rc.IsExist(fkey) {
		return false, nil
	}

	return true, rc.cc.HSet(fkey, field, val).Err()
}

// GetStFieldNames GetStFieldNames
func (rc *RedisCache) GetStFieldNames(table, key string) []string {
	fkey := rc.fullKey(table, key)
	keys, err := rc.cc.HKeys(fkey).Result()
	if err != nil {
		return []string{}
	}
	return keys
}

// GetStAllFields get all fields
func (rc *RedisCache) GetStAllFields(table, key string) (fields map[string][]byte, err error) {

	fkey := rc.fullKey(table, key)
	v, err := rc.cc.HGetAll(fkey).Result()
	if err != nil {
		return nil, err
	}
	if len(v) == 0 {
		return nil, nil
	}
	fields = make(map[string][]byte)
	for i, vv := range v {
		fields[i] = []byte(vv)
	}

	return

}

// del struct field
//func (rc *RedisCache) DelStField(table, key, field string) (bool, error) {
//	fkey := rc.fullKey(table, key)
//	//exist, err := redis.Bool(rc.do("HEXISTS", fkey, field))
//	rs, err := redis.Int(rc.do("HDEL", fkey, field))
//	//log.Printf("****%s, %s", rs, err)
//	if err != nil  {
//		return false, err
//	}
//	return rs == 1, nil
//}

// GetStFields get struct fields, error if no exist
func (rc *RedisCache) GetStFields(table, key string, fields []interface{}, types []reflect.Kind) (vals []interface{}, err error) {

	if len(fields) != len(types) {
		return nil, errors.New("GetStFields len(fields) != len(types)")
	}

	fkey := rc.fullKey(table, key)
	ss := []string{}
	for _, v := range fields {
		ss = append(ss, v.(string))
	}
	return rc.cc.HMGet(fkey, ss...).Result()

}

// SetStFields set struct fields
func (rc *RedisCache) SetStFields(table, key string, fields []interface{}, vals []interface{}, forced bool) (err error) {
	fkey := rc.fullKey(table, key)
	if !forced && !rc.IsExist(fkey) {
		return ErrNoExist
	}

	m := make(map[string]interface{})

	for index, f := range fields {
		m[f.(string)] = vals[index]
	}

	return rc.cc.HMSet(fkey, m).Err()

}

// DelStFields DelStFields
func (rc *RedisCache) DelStFields(table, key string, fields ...interface{}) (int, error) {
	fkey := rc.fullKey(table, key)
	ss := []string{}
	for _, v := range fields {
		ss = append(ss, v.(string))
	}
	v, e := rc.cc.HDel(fkey, ss...).Result()
	return int(v), e
}

// SetAdd SetAdd
func (rc *RedisCache) SetAdd(key string, members ...interface{}) (int, error) {
	v, e := rc.cc.SAdd(key, members...).Result()
	return int(v), e
}

// SetRemove SetRemove
func (rc *RedisCache) SetRemove(key string, members ...interface{}) (int, error) {
	v, e := rc.cc.SRem(key, members...).Result()
	return int(v), e
}

// SetLen SetLen
func (rc *RedisCache) SetLen(key string) (int, error) {
	v, e := rc.cc.SCard(key).Result()
	return int(v), e
}

// SetRandom SetRandom
func (rc *RedisCache) SetRandom(key string, count int) ([]string, error) {
	v, e := rc.cc.SRandMemberN(key, int64(count)).Result()
	return v, e
}

// SetRandomPop SetRandomPop
func (rc *RedisCache) SetRandomPop(key string) (string, error) {
	v, e := rc.cc.SPop(key).Result()
	return v, e
}

// ListLen ListLen
func (rc *RedisCache) ListLen(key string) (int, error) {
	v, e := rc.cc.LLen(key).Result()
	return int(v), e
}

// ListLPush ListLPush
func (rc *RedisCache) ListLPush(key string, values ...interface{}) (int, error) {
	v, e := rc.cc.LPush(key, values...).Result()
	return int(v), e
}

// ListRPush ListRPush
func (rc *RedisCache) ListRPush(key string, values ...interface{}) (int, error) {
	v, e := rc.cc.RPush(key, values...).Result()
	return int(v), e
}

// ListLPop ListLPop
func (rc *RedisCache) ListLPop(key string) (string, error) {
	v, e := rc.cc.LPop(key).Result()
	return v, e
}

//ListRPop ListRPop
func (rc *RedisCache) ListRPop(key string) (string, error) {
	v, e := rc.cc.RPop(key).Result()
	return v, e
}

//ListIndex ListIndex
func (rc *RedisCache) ListIndex(key string, index int) (string, error) {
	v, e := rc.cc.LIndex(key, int64(index)).Result()
	return v, e
}

// // ListInsert ListInsert
// func (rc *RedisCache) ListInsert(key string, index int, value string) error {

// 	return nil

// }

// Keys : Find all keys matching the given pattern
func (rc *RedisCache) Keys(match string) []string {
	v, e := rc.cc.Keys(match).Result()
	if e != nil {
		return []string{}
	}

	return v
}

// Rename : Rename a key
func (rc *RedisCache) Rename(oldkey, newkey string) bool {
	v, e := rc.cc.Rename(oldkey, newkey).Result()
	if e != nil {
		return false
	}
	if v != "OK" {
		return false
	}

	return true
}

func init() {

	cache.Register("goredis", New)
}
