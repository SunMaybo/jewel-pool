# jewel-pool
## amqp 连接池
​### 解决问题
1. 对channel进行管理，减少由于open，close所带来的不必要的开销
2. 对connection进行管理，减少由于open，close所带来的不必要的开销
3. 确认模式的回调机制实现。
4. [streadway](https://github.com/streadway/amqp)，用于底层连接。
###使用
#### 创建
```golang
pool := pool.NewAmqpPool(pool.Config{

        Config: amqp.Config{ChannelMax: 5,},
        Url: "amqp://guest:guest@127.0.0.1:5672",
        Max_Open_Conns: 5,
        Max_Open_Channels: 20,
    })
```
#### 配置说明
*Max_Open_Conns*打开最大连接数，*Max_Open_Channels*最大使用通道数量，*Channel_Idle_Time*通道空闲时间，当ChannelMax*Max_Open_Conns>Max_Open_Channels时候,会回收超时的空闲管道*ChannelMax*最大channel数量，*Channel_TimeOut*发送的超时时间。
#### 例子
**发布**
```golang
pool.Publish("", // exchange
                "queue_test_name", // routing key
                false, // mandatory
                false, // immediate
                amqp.Publishing{
                    ContentType: "application/json",
                    Body: []byte(size1),
                })
```
**确认模式**
```golang
pool.Confirm(false)
    pool.NotifyPublish = func(channelId int, confirm chan amqp.Confirmation) {
        for {
            if c := <-confirm; c.Ack {
                log.Printf("channel_id:%d,sure", channelId)
            } else {
                log.Printf("channel_id:%d,false", channelId)
            }
        }
    }
```
**消费**
```golang
ch, _ := pool.Acquire()
q, _ := ch.QueueDeclare(
        "queue_test_name", // name
        true, // durable
        false, // delete when unused
        false, // exclusive
        false, // no-wait
        nil, // arguments
    )
     ch.Qos(
        1, // prefetch count
        0, // prefetch size
        false, // global
    )
    msgs, _ := ch.Consume(
        q.Name, // queue
        "", // consumer
        false, // auto-ack
        false, // exclusive
        false, // no-local
        false, // no-wait
        nil, // args
    )
    go func() {
        for d := range msgs {
            log.Printf("Received a message: %s", d.Body)
            log.Printf("Done")
            d.Ack(false)
        }
    }()
```
### 性能测试
#### 基础环境

*机器环境**

```
型号标识符：   MacBookPro14,2
处理器名称：    Intel Core i5
处理器速度：    3.1 GHz
处理器数目：    1
核总数：  2
L2 缓存(每个核): 256 KB
L3 缓存：    4 MB
内存：    8 GB
```

*docker版本**

```
Client:
Version: 18.03.1-ce
API version: 1.37
Go version: go1.9.5
Git commit: 9ee9f40
Built: Thu Apr 26 07:13:02 2018
OS/Arch: darwin/amd64
Experimental: false
Orchestrator: swarm

Server:
Engine:
Version: 18.03.1-ce
API version: 1.37 (minimum version 1.12)
Go version: go1.9.5
Git commit: 9ee9f40
Built: Thu Apr 26 07:22:38 2018
OS/Arch: linux/amd64
Experimental: true

```

*rabbitmq版本**

```
daocloud.io/rabbitmq:3-management
```

**测试结果**

| size | threads | counts | channel(QPS) | pool(QPS) |
| ----- | ------- | ------ | ------------ | --------- |
| 11b | 10 | 100000 | 14745 | 40933 |
| 1237b | 10 | 100000 | 4929 | 6066 |
| 4953b | 10 | 100000 | 2508 | 3447 |

### 数据丢失问题
#### 数据发送

> 1. 连接池对连接和管道进行管理，数发送采用异步发送,程序异常情况下，多管道会造成更多数据丢失。
> 2. 网络抖动情况下，造成数据丢失。

*解决方案**

1. 确认模式发送，通过缓存发送数据，超时时间发送未确认数据，并且采幂等发送解决数据多次发送问题。
#### 数据消费
1. 消费数通过ack确认，保证数据不会丢失。

