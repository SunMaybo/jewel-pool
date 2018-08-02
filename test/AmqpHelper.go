package test

import (
	"github.com/streadway/amqp"
	"sync"
	"log"
	"github.com/cihub/seelog"
	"encoding/json"
	"fmt"
)

type AmqpHelp struct {
	Lock       *sync.RWMutex
	Confirms   chan amqp.Confirmation
	Connection *amqp.Connection
	Channel    *amqp.Channel
	Url        string
}

func NewAmqp(url string) *AmqpHelp {
	source, err := amqp.Dial(url)
	if err != nil {
		log.Fatalf("connection.open source: %s", err)
	}
	ch, err := source.Channel()
	if err != nil {
		log.Fatalf("connection.open channel: %s", err)
	}
	helper := AmqpHelp{
		Lock:     &sync.RWMutex{},
		Confirms: make(chan amqp.Confirmation),
		Url:      url,
	}
	//ch.Confirm(false)
	//ch.NotifyPublish(helper.Confirms)
	helper.Channel = ch
	helper.Connection = source
	return &helper
}

func (helper *AmqpHelp) Close() {
	defer func() {
		if e := recover(); e != nil {
			fmt.Println(e)
		}
	}()
	close(helper.Confirms)
	helper.Channel.Close()
	helper.Connection.Close()
}
func (helper *AmqpHelp) Load() error {
	helper.Close()
	source, err := amqp.Dial(helper.Url)
	if err != nil {
		return err
	}
	ch, err := source.Channel()
	if err != nil {
		return err
	}
	//helper.Confirms = make(chan amqp.Confirmation)
	//ch.Confirm(false)
	//ch.NotifyPublish(helper.Confirms)
	helper.Channel = ch
	helper.Connection = source
	return nil
}
func (helper *AmqpHelp) Send(msg interface{}, queue string) error {
	helper.Lock.Lock()
	var err error
	err = helper.SendMsg(msg, queue)
	if err != nil {
		err = helper.Load()
		if err != nil {
			err = helper.SendMsg(msg, queue)
		}
	}
	helper.Lock.Unlock()
	return err
}
func (helper *AmqpHelp) SendMsg(msg interface{}, queue string) error {
	buff, err := json.Marshal(msg)
	if err != nil {
		seelog.Error(err)
		return err
	}
	err = helper.Channel.Publish(
		"",    // exchange
		queue, // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        buff,
		})
	if err != nil {
		seelog.Error(err)
	}
	return err
}
