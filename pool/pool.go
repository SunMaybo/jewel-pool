package pool

import (
	"github.com/streadway/amqp"
	"sync"
	"sync/atomic"
	"time"
	"github.com/pkg/errors"
	"log"
)

type Config struct {
	amqp.Config
	Url               string
	Max_Open_Conns    int
	Max_Idle_Conns    int
	Max_Idle_Channel  int
	Channel_Idle_Time int
	Conns_Idle_Time   int
}
type RabbitConnections struct {
	Config         Config
	connects       []*Connection
	poolSync       *sync.Mutex
	count          *int32
	idleTime       *uint64
	idleConnsCount *int32
	confirm        *bool
	confirmFunc    func(channel *Channel, confirm chan amqp.Confirmation)
}
type ChannelConfirm struct {
	Confirm   chan amqp.Confirmation
	ChannelId int32
}

func NewAmqp(config Config) *RabbitConnections {
	config = defaultConfig(config)
	return &RabbitConnections{
		Config:         config,
		poolSync:       &sync.Mutex{},
		count:          new(int32),
		idleConnsCount: new(int32),
	}
}
func defaultConfig(config Config) Config {
	if config.Conns_Idle_Time <= 0 {
		config.Conns_Idle_Time = 9 * 60
	}
	if config.Max_Open_Conns <= 0 {
		config.Max_Open_Conns = 2
	}
	if config.Max_Idle_Conns <= 0 {
		config.Max_Idle_Conns = 1
	}
	if config.Channel_Idle_Time <= 0 {
		config.Channel_Idle_Time = 9 * 60
	}
	if config.ChannelMax <= 0 {
		config.ChannelMax = 2
	}
	if config.Max_Idle_Channel <= 0 {
		config.Max_Idle_Channel = 1
	}
	return config
}
func (pool *RabbitConnections) Confirm(noWait bool) {
	pool.confirm = new(bool)
	pool.confirm = &noWait
}
func (pool *RabbitConnections) Channel(timeOut time.Duration) (*Channel, error) {
	pool.poolSync.Lock()
	ch := make(chan *Channel)
	var cha *Channel
	var err error
	go func() {
		pool.acquire(ch)
	}()
	select {
	case <-time.After(timeOut):
		err = errors.New("timeout on open channel")
	case cha = <-ch:
	}
	pool.poolSync.Unlock()
	return cha, err
}
func (pool *RabbitConnections) acquire(ch chan *Channel) {
	var cha *Channel
	var err error
	if atomic.LoadInt32(pool.count) <= 0 {
		cha, err = pool.createConnect()
	} else {
		var badIndex []int
		for i, c := range pool.connects {
			if c.Bad() {
				badIndex = append(badIndex, i)
				continue
			}
			cha, err = c.Acquire()
			if err != nil {
				badIndex = append(badIndex, i)
			} else if cha != nil {
				break
			}
		}
		if badIndex != nil {
			for _, index := range badIndex {
				pool.connects[index].Close()
				if len(pool.connects) > index+1 {
					pool.connects = append(pool.connects[:index], pool.connects[index+1:]...)
				}else {
					pool.connects = append(pool.connects[:index])
				}

				count := atomic.LoadInt32(pool.count)
				atomic.CompareAndSwapInt32(pool.count, count, count-1)
			}
		}
		if cha == nil && atomic.LoadInt32(pool.count) < int32(pool.Config.Max_Open_Conns) {
			cha, err = pool.createConnect()
		}
	}
	if err != nil {
		log.Println(err)
	}
	if cha == nil {
		pool.acquire(ch)
	}
	pool.clearIdleTimeConnections()
	ch <- cha
}
func (pool *RabbitConnections) NotifyPublish(confirmFunc func(channel *Channel, confirm chan amqp.Confirmation)) {
	pool.confirmFunc = confirmFunc
}
func (pool *RabbitConnections) acquireConnectionCount() {
	for _, conn := range pool.connects {
		if conn.IsIdle() {
			idleConnsCount := atomic.LoadInt32(pool.idleConnsCount)
			atomic.CompareAndSwapInt32(pool.idleConnsCount, idleConnsCount, idleConnsCount+1)
		}
	}
}
func (pool *RabbitConnections) clearIdleTimeConnections() {
	pool.acquireConnectionCount()
	for {
		if atomic.LoadInt32(pool.idleConnsCount) > int32(pool.Config.Max_Idle_Conns) {
			pool.clearIdleConnectionsWithTimeOut()
		}
		break
	}
}
func (pool *RabbitConnections) clearIdleConnectionsWithTimeOut() {
	index := -1
	for i, conn := range pool.connects {
		if conn.IsIdle() && time.Now().Unix()-int64(atomic.LoadUint64(conn.idleTime)) >= int64(pool.Config.Conns_Idle_Time) {
			index = i
			break
		}
	}
	if index != -1 {
		pool.clearIdleConnectionsWithIndex(index)
	}
}
func (pool *RabbitConnections) clearIdleConnectionsWithIndex(index int) {
	pool.connects[index].Close()
	pool.connects = append(pool.connects[:index], pool.connects[index+1:]...)
	count := atomic.LoadInt32(pool.count)
	atomic.CompareAndSwapInt32(pool.count, count, count-1)
	idleConnsCount := atomic.LoadInt32(pool.count)
	atomic.CompareAndSwapInt32(pool.idleConnsCount, idleConnsCount, idleConnsCount-1)
}
func (pool *RabbitConnections) createConnect() (*Channel, error) {
	connect, err := NewConnection(pool.Config, pool.confirmFunc, pool.confirm)
	if err != nil {
		return nil, err
	}
	ch, err := connect.Acquire()
	if err != nil {
		return nil, err
	}
	pool.connects = append(pool.connects, connect)
	atomic.AddInt32(pool.count, 1)
	return ch, nil
}
