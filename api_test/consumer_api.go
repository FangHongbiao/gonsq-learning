package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/nsqio/go-nsq"
)

type myMessageHandler struct{}

// 真正的消息处理
func processMessage(msg []byte) error {
	time.Sleep(2 * time.Second)
	fmt.Println(string(msg))
	return nil
}

// HandleMessage 实现 Handler 接口.
func (h *myMessageHandler) HandleMessage(m *nsq.Message) error {

	if len(m.Body) == 0 {
		return nil
	}
	// 处理消息
	err := processMessage(m.Body)
	return err
}

func main() {
	// 从命令行接收topic和channel
	topic := flag.String("topic", "default_topic", "set topic name")
	channel := flag.String("channel", "default_channel", "set channel name")
	flag.Parse()

	// 初始化一个consumer
	config := nsq.NewConfig()

	// NewConsumer
	consumer, err := nsq.NewConsumer(*topic, *channel, config)
	if err != nil {
		log.Fatal(err)
	}

	// AddHandler
	// consumer.AddHandler(&myMessageHandler{})

	// AddConcurrentHandlers
	consumer.AddConcurrentHandlers(&myMessageHandler{}, 8)

	// ChangeMaxInFlight
	consumer.ChangeMaxInFlight(2)

	// ConnectToNSQLookupd
	err = consumer.ConnectToNSQLookupd("39.106.208.186:4161")
	if err != nil {
		log.Fatal(err)
	}

	// consumer.ConnectToNSQLookupds("39.106.208.186:4161")
	// consumer.ConnectToNSQD("39.106.208.186:4161")
	// consumer.ConnectToNSQDs("39.106.208.186:4161")

	fmt.Println(consumer.IsStarved())

	consumer.DisconnectFromNSQD("39.106.208.186:4161")
	consumer.DisconnectFromNSQLookupd("39.106.208.186:4161")

	// consumer.SetBehaviorDelegate()
	// consumer.SetLogger()
	// consumer.SetLoggerLevel()

	consumerStats := consumer.Stats()
	fmt.Printf("%#v", consumerStats)

	consumer.Stop()
}
