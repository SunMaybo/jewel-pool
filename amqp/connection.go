package amqp

import (
	"github.com/streadway/amqp"
	"sync/atomic"
	"time"
)

var channelKey = new(int32)

type Channel struct {
	Id         int32
	Connection *amqp.Connection
	Channel    *amqp.Channel
	idle       *uint32
	idleTime   *uint64
}

func generateChannelKey() int32 {
	return atomic.AddInt32(channelKey, 1)
}

func (channel *Channel) acquire() bool {

	return atomic.CompareAndSwapUint32(channel.idle, 0, 1)
}

func (channel *Channel) close() {
	channel.Channel.Close()
}
func (channel *Channel) Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	err := channel.Channel.Publish(exchange, key, mandatory, immediate, msg)
	if err != nil {
		atomic.StoreUint64(channel.idleTime, uint64(time.Now().Unix()))
		atomic.CompareAndSwapUint32(channel.idle, 1, 2)
	} else {
		atomic.CompareAndSwapUint32(channel.idle, 1, 0)
	}
	return err
}
func (channel *Channel) Confirm(noWait bool) error {
	return channel.Channel.Confirm(noWait)
}
func (channel *Channel) Ack(tag uint64, multiple bool) error {
	return channel.Channel.Ack(tag, multiple)
}
func (channel *Channel) Nack(tag uint64, multiple bool, requeue bool) error {
	return channel.Channel.Nack(tag, multiple, requeue)
}
func (channel *Channel) NotifyClose(c chan *amqp.Error) chan *amqp.Error {
	return channel.Channel.NotifyClose(c)
}
func (channel *Channel) NotifyFlow(c chan bool) chan bool {
	return channel.Channel.NotifyFlow(c)
}
func (channel *Channel) NotifyReturn(c chan amqp.Return) chan amqp.Return {
	return channel.NotifyReturn(c)
}
func (channel *Channel) NotifyCancel(c chan string) chan string {
	return channel.Channel.NotifyCancel(c)
}
func (channel *Channel) NotifyConfirm(ack, nack chan uint64) (chan uint64, chan uint64) {
	return channel.Channel.NotifyConfirm(ack, nack)
}
func (channel *Channel) NotifyPublish(confirm chan amqp.Confirmation) chan amqp.Confirmation {
	return channel.Channel.NotifyPublish(confirm)
}
func (channel *Channel) Qos(prefetchCount, prefetchSize int, global bool) error {
	return channel.Channel.Qos(prefetchCount, prefetchCount, global)
}
func (channel *Channel) Cancel(consumer string, noWait bool) error {
	return channel.Channel.Cancel(consumer, noWait)
}
func (channel *Channel) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	return channel.Channel.QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
}
func (channel *Channel) QueueDeclarePassive(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	return channel.Channel.QueueDeclarePassive(name, durable, autoDelete, exclusive, noWait, args)
}
func (channel *Channel) QueueInspect(name string) (amqp.Queue, error) {
	return channel.Channel.QueueInspect(name)
}

func (channel *Channel) QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error {
	return channel.Channel.QueueBind(name, key, exchange, noWait, args)
}
func (channel *Channel) QueueUnbind(name, key, exchange string, args amqp.Table) error {
	return channel.Channel.QueueUnbind(name, key, exchange, args)
}
func (channel *Channel) QueuePurge(name string, noWait bool) (int, error) {
	return channel.Channel.QueuePurge(name, noWait)
}
func (channel *Channel) QueueDelete(name string, ifUnused, ifEmpty, noWait bool) (int, error) {
	return channel.Channel.QueueDelete(name, ifUnused, ifEmpty, noWait)
}
func (channel *Channel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	return channel.Channel.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
}
func (channel *Channel) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
	return channel.Channel.ExchangeDeclare(name, kind, durable, autoDelete, internal, noWait, args)
}
func (channel *Channel) ExchangeDeclarePassive(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
	return channel.Channel.ExchangeDeclarePassive(name, kind, durable, autoDelete, internal, noWait, args)
}
func (channel *Channel) ExchangeDelete(name string, ifUnused, noWait bool) error {
	return channel.Channel.ExchangeDelete(name, ifUnused, noWait)
}
func (channel *Channel) ExchangeBind(destination, key, source string, noWait bool, args amqp.Table) error {
	return channel.Channel.ExchangeBind(destination, key, source, noWait, args)
}
func (channel *Channel) ExchangeUnbind(destination, key, source string, noWait bool, args amqp.Table) error {
	return channel.Channel.ExchangeUnbind(destination, key, source, noWait, args)
}
func (channel *Channel) Get(queue string, autoAck bool) (msg amqp.Delivery, ok bool, err error) {
	return channel.Channel.Get(queue, autoAck)
}
func (channel *Channel) Tx() error {
	return channel.Channel.Tx()
}
func (channel *Channel) TxCommit() error {
	return channel.Channel.TxCommit()
}
func (channel *Channel) TxRollback() error {
	return channel.Channel.TxRollback()
}
func (channel *Channel) Flow(active bool) error {
	return channel.Channel.Flow(active)
}
func (channel *Channel) Recover(requeue bool) error {
	return channel.Channel.Recover(requeue)
}
func (channel *Channel) Reject(tag uint64, requeue bool) error {
	return channel.Reject(tag, requeue)
}

type Connection struct {
	Id          int32
	Conns       *amqp.Connection
	Channels    []*Channel
	config      Config
	count       *int32
	idleTime    *uint64
	confirm     *bool
	confirmFunc func(channel *Channel, confirm chan amqp.Confirmation)
}

func NewConnection(config Config, confirmFunc func(channel *Channel, confirm chan amqp.Confirmation), confirm *bool) (*Connection, error) {
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
	connect := Connection{
		Id:          generateChannelKey(),
		Conns:       conns,
		config:      config,
		count:       new(int32),
		idleTime:    new(uint64),
		confirmFunc: confirmFunc,
		confirm:     confirm,
	}
	idT := uint64(time.Now().Unix())
	connect.idleTime = &idT
	return &connect, nil
}
func (connect *Connection) createChannel() (*Channel, error) {
	amqpCh, err := connect.Conns.Channel()
	if err != nil {
		return nil, err
	}
	ch := &Channel{
		Id:         generateChannelKey(),
		Connection: connect.Conns,
		Channel:    amqpCh,
		idle:       new(uint32),
		idleTime:   new(uint64),
	}
	idleT := uint64(time.Now().Unix())
	ch.idleTime = &idleT
	ch.acquire()
	connect.Channels = append(connect.Channels, ch)
	atomic.AddInt32(connect.count, 1)
	if connect.confirmFunc != nil && connect.confirm != nil {
		ch.Confirm(*connect.confirm)
		channelConfirm := make(chan amqp.Confirmation)
		channelConfirm = ch.NotifyPublish(channelConfirm)
		connect.confirmFunc(ch, channelConfirm)

	}
	return ch, nil
}
func (connect *Connection) acquireIdleCount() int {
	count := 0
	for _, ch := range connect.Channels {
		if atomic.CompareAndSwapUint32(ch.idle, 0, 0) {
			count++
		}
	}
	return count
}
func (connect *Connection) clearIdleTimeChannel() {

	for {
		idleCount := connect.acquireIdleCount()
		if idleCount > connect.config.Max_Idle_Channel {
			connect.clearIdleChannelWithTimeOut()
		}
		break
	}
}
func (connect *Connection) clearIdleChannelWithTimeOut() {
	index := -1
	for i, ch := range connect.Channels {
		if atomic.CompareAndSwapUint32(ch.idle, 0, 0) && time.Now().Unix()-int64(atomic.LoadUint64(ch.idleTime)) >= int64(connect.config.Channel_Idle_Time) {
			index = i
			break
		}
	}
	if index != -1 {
		connect.clearIdleChannelWithIndex(index)
	}

}
func (connect *Connection) clearIdleChannelWithIndex(index int) {
	connect.Channels[index].close()
	if len(connect.Channels) < index+1 {
		connect.Channels = append(connect.Channels[:index], connect.Channels[index+1:]...)
	} else {
		connect.Channels = append(connect.Channels[:index])
	}

	atomic.AddInt32(connect.count, -1)
}

func (connect *Connection) Acquire() (*Channel, error) {
	if atomic.LoadInt32(connect.count) <= 0 {
		return connect.createChannel()
	}
	var cha *Channel

	var err error

	for _, ch := range connect.Channels {
		if ch.acquire() {
			cha = ch
			break
		}
	}

	if cha == nil && atomic.LoadInt32(connect.count) < int32(connect.config.ChannelMax) {
		cha, err = connect.createChannel()
	}

	connect.clearIdleTimeChannel()
	return cha, err
}
func (connect *Connection) Bad() bool {
	isBad := false
	isUsed := false
	for _, c := range connect.Channels {
		if atomic.CompareAndSwapUint32(c.idle, 2, 2) {
			isBad = true
		}
		if atomic.CompareAndSwapUint32(c.idle, 1, 1) {
			isUsed = true
		}
	}
	if isBad && !isUsed {
		return true
	}
	return false
}
func (connect *Connection) IsIdle() bool {
	for _, c := range connect.Channels {
		if atomic.CompareAndSwapUint32(c.idle, 1, 1) {
			return false
		}
	}
	return true
}

func (connect *Connection) Close() {
	for _, c := range connect.Channels {
		c.close()
	}
	connect.Conns.Close()
}
