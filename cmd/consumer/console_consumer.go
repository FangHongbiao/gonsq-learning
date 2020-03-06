package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/nsqio/go-nsq"
)

type myMessageHandler struct{}

// 真正的消息处理
func processMessage(msg []byte) error {
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
	consumer, err := nsq.NewConsumer(*topic, *channel, config)
	if err != nil {
		log.Fatal(err)
	}

	// 设置处理器
	consumer.AddHandler(&myMessageHandler{})

	// 连接oNSQLookupd， 接收消息
	err = consumer.ConnectToNSQLookupd("39.106.208.186:4161")
	if err != nil {
		log.Fatal(err)
	}

	select {}
}
