package pool

import (
	"github.com/streadway/amqp"
	"sync"
	"log"
	"time"
	"github.com/pkg/errors"
	"sync/atomic"
	"math/rand"
)

type JewelChannel struct {
	Id         int
	Connection int
	Channel    *amqp.Channel
}

var GenId = new(int32)

func produceId() int {
	return int(atomic.AddInt32(GenId, 1))
}

func (channel *JewelChannel) Close() {
	defer func() {
		if e := recover(); e != nil {
			log.Println(e)
		}
	}()
	if channel.Channel != nil {
		channel.Channel.Close()
	}
}
func (channel *JewelChannel) Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	err := channel.Channel.Publish(exchange, key, mandatory, immediate, msg)
	return err
}
func (channel *JewelChannel) Confirm(noWait bool) error {
	return channel.Channel.Confirm(noWait)
}
func (channel *JewelChannel) Ack(tag uint64, multiple bool) error {
	return channel.Channel.Ack(tag, multiple)
}
func (channel *JewelChannel) Nack(tag uint64, multiple bool, requeue bool) error {
	return channel.Channel.Nack(tag, multiple, requeue)
}
func (channel *JewelChannel) NotifyClose(c chan *amqp.Error) chan *amqp.Error {
	return channel.Channel.NotifyClose(c)
}
func (channel *JewelChannel) NotifyFlow(c chan bool) chan bool {
	return channel.Channel.NotifyFlow(c)
}
func (channel *JewelChannel) NotifyReturn(c chan amqp.Return) chan amqp.Return {
	return channel.NotifyReturn(c)
}
func (channel *JewelChannel) NotifyCancel(c chan string) chan string {
	return channel.Channel.NotifyCancel(c)
}
func (channel *JewelChannel) NotifyConfirm(ack, nack chan uint64) (chan uint64, chan uint64) {
	return channel.Channel.NotifyConfirm(ack, nack)
}
func (channel *JewelChannel) NotifyPublish(confirm chan amqp.Confirmation) chan amqp.Confirmation {
	return channel.Channel.NotifyPublish(confirm)
}
func (channel *JewelChannel) Qos(prefetchCount, prefetchSize int, global bool) error {
	return channel.Channel.Qos(prefetchCount, prefetchCount, global)
}
func (channel *JewelChannel) Cancel(consumer string, noWait bool) error {
	return channel.Channel.Cancel(consumer, noWait)
}
func (channel *JewelChannel) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	return channel.Channel.QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
}
func (channel *JewelChannel) QueueDeclarePassive(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	return channel.Channel.QueueDeclarePassive(name, durable, autoDelete, exclusive, noWait, args)
}
func (channel *JewelChannel) QueueInspect(name string) (amqp.Queue, error) {
	return channel.Channel.QueueInspect(name)
}

func (channel *JewelChannel) QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error {
	return channel.Channel.QueueBind(name, key, exchange, noWait, args)
}
func (channel *JewelChannel) QueueUnbind(name, key, exchange string, args amqp.Table) error {
	return channel.Channel.QueueUnbind(name, key, exchange, args)
}
func (channel *JewelChannel) QueuePurge(name string, noWait bool) (int, error) {
	return channel.Channel.QueuePurge(name, noWait)
}
func (channel *JewelChannel) QueueDelete(name string, ifUnused, ifEmpty, noWait bool) (int, error) {
	return channel.Channel.QueueDelete(name, ifUnused, ifEmpty, noWait)
}
func (channel *JewelChannel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	return channel.Channel.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
}
func (channel *JewelChannel) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
	return channel.Channel.ExchangeDeclare(name, kind, durable, autoDelete, internal, noWait, args)
}
func (channel *JewelChannel) ExchangeDeclarePassive(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
	return channel.Channel.ExchangeDeclarePassive(name, kind, durable, autoDelete, internal, noWait, args)
}
func (channel *JewelChannel) ExchangeDelete(name string, ifUnused, noWait bool) error {
	return channel.Channel.ExchangeDelete(name, ifUnused, noWait)
}
func (channel *JewelChannel) ExchangeBind(destination, key, source string, noWait bool, args amqp.Table) error {
	return channel.Channel.ExchangeBind(destination, key, source, noWait, args)
}
func (channel *JewelChannel) ExchangeUnbind(destination, key, source string, noWait bool, args amqp.Table) error {
	return channel.Channel.ExchangeUnbind(destination, key, source, noWait, args)
}
func (channel *JewelChannel) Get(queue string, autoAck bool) (msg amqp.Delivery, ok bool, err error) {
	return channel.Channel.Get(queue, autoAck)
}
func (channel *JewelChannel) Tx() error {
	return channel.Channel.Tx()
}
func (channel *JewelChannel) TxCommit() error {
	return channel.Channel.TxCommit()
}
func (channel *JewelChannel) TxRollback() error {
	return channel.Channel.TxRollback()
}
func (channel *JewelChannel) Flow(active bool) error {
	return channel.Channel.Flow(active)
}
func (channel *JewelChannel) Recover(requeue bool) error {
	return channel.Channel.Recover(requeue)
}
func (channel *JewelChannel) Reject(tag uint64, requeue bool) error {
	return channel.Reject(tag, requeue)
}

type Config struct {
	amqp.Config
	Url                   string
	Max_Open_Conns        int
	Max_Open_Channels     int
	Channel_Idle_Time int
	Channel_TimeOut       time.Duration
}

type AmqpPool struct {
	jchannel       chan JewelChannel
	Locker         *sync.RWMutex
	Config         Config
	connects       []*JewelConnection
	Count          int
	confirm        *bool
	PublishTimeOut time.Duration
	NotifyPublish  func(channelId int, confirm chan amqp.Confirmation)
}

func NewAmqpPool(config Config) *AmqpPool {
	config = defaultConfig(config)
	pool := &AmqpPool{
		Config:   config,
		jchannel: make(chan JewelChannel, config.Max_Open_Channels),
		Locker:   &sync.RWMutex{},
	}
	return pool
}
func defaultConfig(config Config) Config {
	if config.Max_Open_Conns <= 0 {
		config.Max_Open_Conns = 2
	}
	if config.ChannelMax <= 0 {
		config.ChannelMax = 3
	}
	if config.Max_Open_Channels <= 0 {
		config.Max_Open_Channels = 5
	}
	if config.Channel_Idle_Time <= 0 {
		config.Channel_Idle_Time = 5
	}
	if config.Channel_TimeOut <= 0 {
		config.Channel_TimeOut = 3 * time.Second
	}
	return config
}

func (pool *AmqpPool) Confirm(noWait bool) {
	pool.confirm = new(bool)
	pool.confirm = &noWait
}
func (pool *AmqpPool) Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	ch, err := pool.Acquire()
	if err != nil {
		return err
	}
	err = ch.Channel.Publish(exchange, key, mandatory, immediate, msg)
	if err != nil {
		pool.clearConnection(ch.Connection)
	} else {
		pool.addChannel(*ch)
	}
	return err
}
func (pool *AmqpPool) addChannel(ch JewelChannel) {
	go func() {
	loop:
		for {
			select {
			case pool.jchannel <- ch:
				break loop
			case <-time.After(time.Second * time.Duration(pool.Config.Channel_Idle_Time)):
				for _, c := range pool.connects {
					if c.Id == ch.Connection {
						ch.Close()
						c.SubChannelCount()
						break loop
					}
				}
				break loop
			}

		}
	}()

}
func (pool *AmqpPool) Acquire() (*JewelChannel, error) {
	isMoreLong := false
	for {
		if isMoreLong {
			select {
			case ch := <-pool.jchannel:
				return &ch, nil
			case <-time.After(pool.PublishTimeOut):
				return nil, errors.New("acquire channel timeout")
			}
		}
		select {
		case ch := <-pool.jchannel:
			return &ch, nil
		case <-time.After(1 * time.Second):
			ch, err := pool.createChannel()
			if err == nil && ch == nil {
				isMoreLong = true
			} else {
				return ch, err
			}
			break
		}
	}
}
func (pool *AmqpPool) createChannel() (*JewelChannel, error) {
	pool.Locker.Lock()
	defer pool.Locker.Unlock()
	if pool.Count < pool.Config.Max_Open_Conns {
		conn, err := NewJewelConnection(pool.Config)
		if err != nil {
			return nil, err
		}
		conn.confirm = pool.confirm
		conn.NotifyPublish = pool.NotifyPublish
		ch, err := conn.CreateChannel(pool.Config)
		if err != nil {
			return nil, err
		}
		if ch != nil {
			pool.connects = append(pool.connects, conn)
			pool.Count++
			return ch, nil
		}
		return nil, nil
	}
	randomIndex := rand.Intn(pool.Count)
	ch, err := pool.connects[randomIndex].CreateChannel(pool.Config)
	return ch, err
}
func (pool *AmqpPool) clearConnection(id int) {
	pool.Locker.Lock()
	defer pool.Locker.Unlock()
	var index int
	for i, c := range pool.connects {
		if c.Id == id {
			c.SubChannelCount()
		}
		if c.Id == id && c.ChannelCount() <= 0 {
			index = i
			c.Close()
			break
		}
	}
	if index >= pool.Count {
		pool.connects = pool.connects[:index]
	} else {
		pool.connects = append(pool.connects[:index], pool.connects[index+1:]...)
	}
	pool.Count--
}

type JewelConnection struct {
	Id            int
	Connection    *amqp.Connection
	channelCount  int
	Locker        *sync.Mutex
	confirm       *bool
	NotifyPublish func(channelId int, confirm chan amqp.Confirmation)
}

func NewJewelConnection(config Config) (*JewelConnection, error) {
	amqpConfig := amqp.Config{
		SASL:            config.SASL,
		Vhost:           config.Vhost,
		ChannelMax:      config.ChannelMax,
		FrameSize:       config.FrameSize,
		Heartbeat:       config.Heartbeat,
		TLSClientConfig: config.TLSClientConfig,
		Properties:      config.Properties,
		Locale:          config.Locale,
		Dial:            config.Dial,
	}
	conns, err := amqp.DialConfig(config.Url, amqpConfig)
	if err != nil {
		return nil, err
	}
	connect := JewelConnection{
		Id:         produceId(),
		Locker:     &sync.Mutex{},
		Connection: conns,
	}
	return &connect, nil
}

func (conn *JewelConnection) AddChannelCount() {
	conn.Locker.Lock()
	defer conn.Locker.Unlock()
	conn.channelCount++
}
func (conn *JewelConnection) Close() {
	defer func() {
		if e := recover(); e != nil {
			log.Println(e)
		}
	}()
	conn.Connection.Close()
}
func (conn *JewelConnection) CreateChannel(config Config) (*JewelChannel, error) {
	if conn.ChannelCount() >= config.ChannelMax {
		return nil, nil
	}
	ch, err := conn.Connection.Channel()
	if err != nil {
		return nil, err
	}
	jCh := JewelChannel{
		Id:         produceId(),
		Connection: conn.Id,
		Channel:    ch,
	}
	if conn.confirm != nil {
		ch.Confirm(*conn.confirm)
		confirmChan := make(chan amqp.Confirmation)
		ch.NotifyPublish(confirmChan)
		go func() {
			conn.NotifyPublish(jCh.Id, confirmChan)
		}()
	}
	conn.AddChannelCount()
	return &jCh, nil
}
func (conn *JewelConnection) SubChannelCount() {
	conn.Locker.Lock()
	defer conn.Locker.Unlock()
	conn.channelCount--
}
func (conn *JewelConnection) ChannelCount() int {
	conn.Locker.Lock()
	defer conn.Locker.Unlock()
	return conn.channelCount
}
