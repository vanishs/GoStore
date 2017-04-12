package redis

import (
	"gopkg.in/redsync.v1"
	"github.com/seewindcn/GoStore/lock"
	"github.com/seewindcn/GoStore/cache/redis"
	"github.com/seewindcn/GoStore/store"
)

const (
	LOCK_PRE = "_lk_"
)

type RedisDriver struct {
	mgr *lock.LockMgr
	rs *redsync.Redsync
	options []redsync.Option
}

func New(mgr *lock.LockMgr, st interface{}) lock.Driver {
	s := st.(*store.Store)
	c := s.Cache.(*redis.RedisCache)
	pools := []redsync.Pool{}
	for _, p := range c.GetPools() {
		pools = append(pools, p)
	}

	d := &RedisDriver{
		mgr: mgr,
		rs: redsync.New(pools),
		options: []redsync.Option{},
	}
	d.Init()
	return d
}

func (self *RedisDriver) Init() {
	self.options = self.options[:0:0] // new a slice, clean options,
	self.options = append(self.options, redsync.SetExpiry(self.mgr.Expiry))
	self.options = append(self.options, redsync.SetTries(self.mgr.Tries))
	self.options = append(self.options, redsync.SetRetryDelay(self.mgr.Delay))
}

func (self *RedisDriver) NewLock(name string) lock.Lock {
	mx := self.rs.NewMutex(LOCK_PRE + name, self.options...)
	return mx
}

func init() {
	lock.Register("redis", New)
}

