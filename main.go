package main

import (
	"jewel-pool/pool"
	amqp2 "github.com/streadway/amqp"
	"log"
	"time"
	"sync/atomic"
)

func main() {
	pool := pool.NewAmqp(pool.Config{
		Config:         amqp2.Config{ChannelMax: 20},
		Url:            "amqp://guest:guest@127.0.0.1:5672",
		Max_Open_Conns: 5,
	})
	confirmCount := new(int32)
	pool.Confirm(false)
	pool.NotifyPublish(func(channel *pool.Channel, confirm chan amqp2.Confirmation) {
		for {
			if st := <-confirm; st.Ack {
				atomic.AddInt32(confirmCount, 1)
				log.Println(channel.Id, st, atomic.LoadInt32(confirmCount))
			}

		}

	})
	count := new(int32)
	for i := 0; i < 20; i++ {
		go func() {
			for i := 0; i < 5000; i++ {
				channel, err := pool.Channel(2 * time.Second)
				if err != nil {
					log.Println(err)
				} else {
					err = channel.Publish("", // exchange
						"queue_test_name",    // routing key
						false,                // mandatory
						false,                // immediate
						amqp2.Publishing{
							ContentType: "text/plain",
							Body:        []byte(`{"event":"USER_RECHARGE_REQUEST","nonce":"7208740289275779230","received":{"amount":"0.25","app_id":"cf9a6bff34289f7722ce59720710d351b7491d05","extras":"tokenup-recharge-test","nonce":"6583854047336883625","notify_url":"http://127.0.0.1:8597/test/notify","order_id":"5b2cfaa9-396b-4976-a0cb-c9e9fd11a830","signature":"94213c03c81e76910b67d67ad32f509eda8b54755272d428f19b48da09fdc5a037f09cde1ab58841a333d7ef5f80c5419e2025281d058d07579769ecc311c496866fe3653de5175a86916865d916d6c37751f517b54c947fb171418606100d32d3390a255c68b3ec885b20916f186666c5042919a7fff6837ae115107e96f7ffb1ce693f1d5d5ca23960fa587a838f07353f372405347bff0d3a02654a94c42c32241cf702af19b5812a2ae9a81165be599d0ce8ada65dc1b16e67c8c8f2f82760a54eeddfdbb3e7fdaced784df57f0b4eb6729a11564ac7db8525ba263580063aad3c8624f76db4357920863190a774125895d8add7b265cac940c250621a0c","timestamp":1531987527,"user_id":"90fdc761-9633-41a4-80dd-c48ed3ea0ed0"},"result":{"message":"api invoke success","status":"SUCCESS"},"signature":"15dc083ca56db55c24f30f2d8b4d46184136dbd9f26058edf735980c41a60fb7b03c352e58481211e06546582050a0b463b0c3d98aaa492dbc18809b6159f5721c4a99eee081501d4dccb7ea5d5804e5292708a4734876d9d0a4eb1cf0ba5ad8b2fa50cc9c48fd39f222db43a987383934fa1eff982cd88c68b3cd81a06faaa9"}`),
						})
					if err != nil {
						log.Println("push", err, channel.Id)
					} else {
						atomic.AddInt32(count, 1)
						log.Println(channel.Id, "ok", atomic.LoadInt32(count))
					}
				}
			}
		}()
	}
	select {}
}
func rest() {

}
