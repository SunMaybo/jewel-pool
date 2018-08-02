package test

import (
	"testing"
	"github.com/streadway/amqp"
	"time"
	"fmt"
	"jewel-pool/pool"
	"log"
)

func TestPool(t *testing.T) {
	pool := pool.NewAmqpPool(pool.Config{

		Config:            amqp.Config{ChannelMax: 5,},
		Url:               "amqp://guest:guest@127.0.0.1:5672",
		Max_Open_Conns:    5,
		Max_Open_Channels: 20,
	})
	/*pool.Confirm(false)
	pool.NotifyPublish = func(channelId int, confirm chan amqp.Confirmation) {
		for {
			if c := <-confirm; c.Ack {
				log.Printf("channel_id:%d,sure", channelId)
			} else {
				log.Printf("channel_id:%d,false", channelId)
			}
		}
	}
*/
	for i := 0; i < 30; i++ {
		go func() {
			pool.Publish("",       // exchange
				"queue_test_name", // routing key
				false,             // mandatory
				false,             // immediate
				amqp.Publishing{
					ContentType: "application/json",
					Body:        []byte(size1),
				})
		}()
	}
	time.Sleep(2 * time.Second)
	start := time.Now().UnixNano() / 1e6
	size := []byte(size3)
	fmt.Printf("push:%db\n", len(size))
	forever := make(chan bool)
	for i := 0; i < 10; i++ {
		go func() {
			for i := 0; i < 10000; i++ {
				err := pool.Publish(
					"",                // exchange
					"queue_test_name", // routing key
					false,             // mandatory
					false,             // immediate
					amqp.Publishing{
						ContentType: "application/json",
						Body:        []byte(size3),
					})
				if err != nil {
					log.Println(err)
				} else {
					log.Println("ok")
				}
			}
			forever <- true
		}()
	}

	for i := 0; i < 10; i++ {
		<-forever
	}
	end := time.Now().UnixNano() / 1e6
	fmt.Printf("time:%dms\n", end-start)
}
