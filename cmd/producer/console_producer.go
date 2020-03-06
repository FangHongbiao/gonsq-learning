package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/nsqio/go-nsq"
)

var (
	producer *nsq.Producer
)

func main() {
	defer producer.Stop()

	// 从命令行接收topic和channel
	topic := flag.String("topic", "default_topic", "set topic name")
	flag.Parse()

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		msg := scanner.Text()
		fmt.Printf("send msg to %s: %s\n", *topic, msg)
		sendMsg(producer, *topic, msg)
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(os.Stderr, "reading standard input:", err)
	}
}

func init() {
	// 初始化 producer.
	config := nsq.NewConfig()
	var err error
	producer, err = nsq.NewProducer("39.106.208.186:4150", config)
	if err != nil {
		log.Fatal(err)
	}
}

func sendMsg(producer *nsq.Producer, topicName string, msg string) {
	messageBody := []byte(msg)

	// 同步发送消息
	err := producer.Publish(topicName, messageBody)
	if err != nil {
		log.Fatal(err)
	}
}
