
package redis

import (
	"time"
	"github.com/garyburd/redigo/redis"
	"github.com/seewindcn/GoStore/cache"
	"errors"
	"reflect"
	"fmt"
)

var (
)

// Cache is Redis cache adapter.
type RedisCache struct {
	pool	*redis.Pool
	addr	string
	dbNum	int
	pwd	string
}

// New create new redis cache with default collection name.
func New() cache.Cache {
	return &RedisCache{
		dbNum:0,
	}
}

func _redis2value(t reflect.Kind, val interface{}, err error) (interface{}, error) {
	switch t {
	case reflect.String:
		return redis.String(val, err)
	case reflect.Bool:
		return redis.Bool(val, err)
	case reflect.Int:
		return redis.Int(val, err)
	case reflect.Int64:
		return redis.Int64(val, err)
	case reflect.Uint64:
		return redis.Uint64(val, err)
	case reflect.Float64:
		return redis.Float64(val, err)
	}
	return nil, errors.New(fmt.Sprintf("Kind(%s) no support", t))
}

// config
func (self *RedisCache) config(config map[string] interface{}) error {
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
func (self *RedisCache) Start(config map[string]interface{}) error {
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
		MaxIdle:     3,
		IdleTimeout: 500 * time.Second,
		Dial:        dialFunc,
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
			return errors.New("PutStruct set timeout fail")
		}
	}
	return nil
}

// GetStruct get cache struct by key
func (self *RedisCache) GetStruct(table, key string, dest interface{}) (bool, error) {
	fkey := self.fullKey(table, key)
	rs, err := redis.Values(self.do("HGETALL", fkey))
	//fmt.Printf("*****%s, %s", rs, err)
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
		return nil, err
	}
	return _redis2value(t, val, err)
}

// set struct field
func (self *RedisCache) SetStField(table, key, field string, val interface{}) (exist bool, err error) {
	fkey := self.fullKey(table, key)
	if !self.IsExist(fkey) {
		return false, nil
	}
	_, err = self.do("HSET", fkey, field, val)
	return true, err
}


func init() {
	cache.Register("redis", New)
}



