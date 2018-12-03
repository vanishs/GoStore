package redis

import (
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/vanishs/GoStore"
	"github.com/vanishs/GoStore/cache"
)

var (
	NoExistError = errors.New("no exist error")
	TimeoutError = errors.New("timeout")
)

// Cache is Redis cache adapter.
type RedisCache struct {
	pool  *redis.Pool
	addr  string
	dbNum int
	pwd   string
}

// New create new redis cache with default collection name.
func New() cache.Cache {
	return &RedisCache{
		dbNum: 0,
	}
}

func _redis2value(t reflect.Kind, val interface{}) (interface{}, error) {
	switch t {
	case reflect.String:
		return redis.String(val, nil)
	case reflect.Bool:
		return redis.Bool(val, nil)
	case reflect.Int:
		return redis.Int(val, nil)
	case reflect.Int64:
		return redis.Int64(val, nil)
	case reflect.Uint64:
		return redis.Uint64(val, nil)
	case reflect.Float64:
		return redis.Float64(val, nil)
	}
	return nil, errors.New(fmt.Sprintf("Kind(%s) no support", t))
}

func _redisJoin(key string, values []interface{}) []interface{} {
	var ss []interface{}
	ss = append(ss, key)
	ss = append(ss, values...)
	return ss
}

// config
func (self *RedisCache) config(config GoStore.M) error {
	for key, value := range config {
		if key == "addr" {
			self.addr = value.(string)
		} else if key == "dbNum" {
			self.dbNum = value.(int)
		} else if key == "pwd" {
			self.pwd = value.(string)
		}
	}
	return nil
}

//Start start cache
func (self *RedisCache) Start(config GoStore.M) error {
	err := self.config(config)
	if err != nil {
		return err
	}

	dialFunc := func() (c redis.Conn, err error) {
		c, err = redis.Dial("tcp", self.addr)
		if err != nil {
			return nil, err
		}

		if self.pwd != "" {
			if _, err := c.Do("AUTH", self.pwd); err != nil {
				c.Close()
				return nil, err
			}
		}
		if self.dbNum > 0 {
			_, err := c.Do("SELECT", self.dbNum)
			if err != nil {
				c.Close()
				return nil, err
			}
		}
		return
	}
	testFunc := func(c redis.Conn, t time.Time) error {
		if time.Since(t) < time.Minute {
			return nil
		}
		_, err := c.Do("PING")
		return err
	}
	// initialize a new pool
	self.pool = &redis.Pool{
		MaxIdle:      3,
		IdleTimeout:  500 * time.Second,
		Dial:         dialFunc,
		TestOnBorrow: testFunc,
	}
	return nil
}

// actually do the redis cmds
func (self *RedisCache) do(cmd string, args ...interface{}) (reply interface{}, err error) {
	c := self.pool.Get()
	defer c.Close()

	return c.Do(cmd, args...)
}

//Get Pools
func (self *RedisCache) GetPools() []*redis.Pool {
	return []*redis.Pool{self.pool}
}

// Get cache from redis.
func (self *RedisCache) Get(key string) interface{} {
	if v, err := self.do("GET", key); err == nil {
		return v
	}
	return nil
}

// GetMulti get cache from redis.
func (self *RedisCache) GetMulti(keys []string) []interface{} {
	size := len(keys)
	var rv []interface{}
	c := self.pool.Get()
	defer c.Close()
	var err error
	for _, key := range keys {
		err = c.Send("GET", key)
		if err != nil {
			goto ERROR
		}
	}
	if err = c.Flush(); err != nil {
		goto ERROR
	}
	for i := 0; i < size; i++ {
		if v, err := c.Receive(); err == nil {
			rv = append(rv, v.([]byte))
		} else {
			rv = append(rv, err)
		}
	}
	return rv
ERROR:
	rv = rv[0:0]
	for i := 0; i < size; i++ {
		rv = append(rv, nil)
	}

	return rv
}

// Put put cache to redis.
func (self *RedisCache) Put(key string, val interface{}, timeout int) error {
	var err error
	if _, err = self.do("SETEX", key, int64(timeout), val); err != nil {
		return err
	}
	return err
}

// Delete delete cache in redis.
func (self *RedisCache) Delete(key string) error {
	var err error
	if _, err = self.do("DEL", key); err != nil {
		return err
	}
	return err
}

//Deletes delete caches by keys
func (self *RedisCache) Deletes(keys []interface{}) (int, error) {
	rs, err := redis.Int(self.do("DEL", keys...))
	return rs, err
}

// Incr increase counter in redis.
func (self *RedisCache) Incr(key string) (int64, error) {
	rs, err := redis.Int64(self.do("INCRBY", key, 1))
	return rs, err
}

// Decr decrease counter in redis.
func (self *RedisCache) Decr(key string) (int64, error) {
	rs, err := redis.Int64(self.do("INCRBY", key, -1))
	return rs, err
}

// IsExist check cache's existence in redis.
func (self *RedisCache) IsExist(key string) bool {
	v, err := redis.Bool(self.do("EXISTS", key))
	if err != nil {
		return false
	}
	return v
}

// Expire EXPIRE
func (self *RedisCache) Expire(key string, timeout int) bool {
	v, err := redis.Bool(self.do("EXPIRE", key, int64(timeout)))
	if err != nil {
		return false
	}
	return v
}

func (self *RedisCache) fullKey(table, key string) string {
	if key == "" {
		return table
	}
	return table + "-" + key
}

// PutStruct
func (self *RedisCache) PutStruct(table, key string, val interface{}, timeout int) error {
	fkey := self.fullKey(table, key)
	c := self.pool.Get()
	defer c.Close()
	args := redis.Args{}.Add(fkey).AddFlat(val)
	c.Send("HMSET", args...)
	if timeout > 0 {
		c.Send("EXPIRE", fkey, int64(timeout))
	}
	c.Flush()
	_, err := redis.String(c.Receive())
	if err != nil {
		return err
	}
	if timeout > 0 {
		rs, err := redis.Bool(c.Receive())
		if err != nil {
			return err
		}
		if !rs {
			return TimeoutError
		}
	}
	return nil
}

// GetStruct get cache struct by key
func (self *RedisCache) GetStruct(table, key string, dest interface{}) (bool, error) {
	fkey := self.fullKey(table, key)
	rs, err := redis.Values(self.do("HGETALL", fkey))
	//log.Printf("*****%s, %s", rs, err)
	if err != nil {
		return false, err
	}
	if len(rs) == 0 {
		return false, nil
	}
	err = redis.ScanStruct(rs, dest)
	if err != nil {
		return false, err
	}
	return true, nil
}

// get struct field
func (self *RedisCache) GetStField(table, key, field string, t reflect.Kind) (val interface{}, err error) {

	fkey := self.fullKey(table, key)
	//exist, err := redis.Bool(self.do("HEXISTS", fkey, field))
	val, err = self.do("HGET", fkey, field)
	if err != nil {
		if err == redis.ErrNil {
			err = cache.ErrNil
		}
		return nil, err
	}

	val, err = _redis2value(t, val)
	if err == redis.ErrNil {
		err = cache.ErrNil
	}
	return val, err
}

// set struct field
func (self *RedisCache) SetStField(table, key, field string, val interface{}, forced bool) (exist bool, err error) {
	fkey := self.fullKey(table, key)
	if !forced && !self.IsExist(fkey) {
		return false, nil
	}
	_, err = self.do("HSET", fkey, field, val)
	return true, err
}

func (self *RedisCache) GetStFieldNames(table, key string) []string {
	fkey := self.fullKey(table, key)
	keys, err := redis.Strings(self.do("HKEYS", fkey))
	if err != nil {
		return []string{}
	}
	return keys
}

// get all fields
func (self *RedisCache) GetStAllFields(table, key string) (fields map[string][]byte, err error) {
	fkey := self.fullKey(table, key)
	rs, err := redis.Values(self.do("HGETALL", fkey))
	//log.Printf("*****%s, %s", rs, err)
	if err != nil {
		return nil, err
	}
	if len(rs) == 0 {
		return nil, nil
	}
	fields = make(map[string][]byte)
	l := len(rs)
	for i := 0; i < l; i = i + 2 {
		k := string(rs[i].([]byte))
		v := rs[i+1].([]byte)
		//log.Println("~~~~", k, v)
		fields[k] = v
	}
	//err = redis.ScanStruct(rs, dest)
	//if err != nil {
	//	return false, err
	//}
	return
}

// del struct field
//func (self *RedisCache) DelStField(table, key, field string) (bool, error) {
//	fkey := self.fullKey(table, key)
//	//exist, err := redis.Bool(self.do("HEXISTS", fkey, field))
//	rs, err := redis.Int(self.do("HDEL", fkey, field))
//	//log.Printf("****%s, %s", rs, err)
//	if err != nil  {
//		return false, err
//	}
//	return rs == 1, nil
//}

// get struct fields, error if no exist
func (self *RedisCache) GetStFields(table, key string, fields []interface{}, types []reflect.Kind) (vals []interface{}, err error) {
	if len(fields) != len(types) {
		return nil, errors.New("GetStFields len(fields) != len(types)")
	}
	fkey := self.fullKey(table, key)
	//exist, err := redis.Bool(self.do("HEXISTS", fkey, field))
	ss := _redisJoin(fkey, fields)
	rs, err := redis.Values(self.do("HMGET", ss...))
	//fmt.Println("~~~~~~~", ss, rs)
	if err != nil {
		return nil, err
	}

	for index, t := range types {
		//fmt.Println("***", index, t, rs[index])
		v, _ := _redis2value(t, rs[index])
		vals = append(vals, v)
	}
	return
}

// set struct fields
func (self *RedisCache) SetStFields(table, key string, fields []interface{}, vals []interface{}, forced bool) (err error) {
	fkey := self.fullKey(table, key)
	if !forced && !self.IsExist(fkey) {
		return NoExistError
	}
	var ss []interface{}
	ss = append(ss, fkey)
	for index, f := range fields {
		ss = append(ss, f, vals[index])
	}
	_, err = self.do("HMSET", ss...)
	return err
}

func (self *RedisCache) DelStFields(table, key string, fields ...interface{}) (int, error) {
	fkey := self.fullKey(table, key)
	ss := _redisJoin(fkey, fields)
	//exist, err := redis.Bool(self.do("HEXISTS", fkey, field))
	rs, err := redis.Int(self.do("HDEL", ss...))
	//log.Printf("****%s, %s", rs, err)
	if err != nil {
		return 0, err
	}
	return rs, nil
}

func (self *RedisCache) SetAdd(key string, members ...interface{}) (int, error) {
	ss := _redisJoin(key, members)
	rs, err := redis.Int(self.do("SADD", ss...))
	if err != nil {
		return 0, err
	}
	return rs, nil
}

func (self *RedisCache) SetRemove(key string, members ...interface{}) (int, error) {
	ss := _redisJoin(key, members)
	rs, err := redis.Int(self.do("SREM", ss...))
	if err != nil {
		return 0, err
	}
	return rs, nil
}

func (self *RedisCache) SetLen(key string) (int, error) {
	rs, err := redis.Int(self.do("SCARD", key))
	if err != nil {
		return 0, err
	}
	return rs, nil
}

func (self *RedisCache) SetRandom(key string, count int) ([]string, error) {
	rs, err := redis.Strings(self.do("SRANDMEMBER", key, count))
	if err != nil {
		return nil, err
	}
	return rs, nil
}

func (self *RedisCache) SetRandomPop(key string) (string, error) {
	rs, err := redis.String(self.do("SPOP", key))
	if err != nil {
		return "", err
	}
	return rs, nil

}

func (self *RedisCache) ListLen(key string) (int, error) {
	rs, err := redis.Int(self.do("LLEN", key))
	if err != nil {
		return 0, err
	}
	return rs, nil
}

func (self *RedisCache) ListLPush(key string, values ...interface{}) (int, error) {
	ss := _redisJoin(key, values)
	if rs, err := redis.Int(self.do("LPUSH", ss...)); err != nil {
		return 0, err
	} else {
		return rs, nil
	}
}

func (self *RedisCache) ListRPush(key string, values ...interface{}) (int, error) {
	ss := _redisJoin(key, values)
	if rs, err := redis.Int(self.do("RPUSH", ss...)); err != nil {
		return 0, err
	} else {
		return rs, nil
	}
}

func (self *RedisCache) ListLPop(key string) (string, error) {
	if rs, err := redis.String(self.do("LPOP", key)); err != nil {
		return "", err
	} else {
		return rs, nil
	}
}

func (self *RedisCache) ListRPop(key string) (string, error) {
	if rs, err := redis.String(self.do("RPOP", key)); err != nil {
		return "", err
	} else {
		return rs, nil
	}
}

func (self *RedisCache) ListIndex(key string, index int) (string, error) {
	if rs, err := redis.String(self.do("LINDEX", key, index)); err != nil {
		return "", err
	} else {
		return rs, nil
	}
}

//func (self *RedisCache) ListInsert(key string, index int, value string) error {
//	if rs, err := redis.String(self.do("LINSERT", key, index)); err != nil {
//		return "", err
//	} else {
//		return rs, nil
//	}
//}

// Keys : Find all keys matching the given pattern
func (rc *RedisCache) Keys(match string) []string {
	keys, err := redis.Strings(rc.do("KEYS", match))
	if err != nil {
		return []string{}
	}

	return keys
}

// Rename : Rename a key
func (rc *RedisCache) Rename(oldkey, newkey string) bool {

	val, err := redis.String(rc.do("RENAME", oldkey, newkey))
	if err != nil {
		return false
	}

	if val != "OK" {
		return false
	}

	return true
}

func init() {
	cache.Register("redis", New)
}
