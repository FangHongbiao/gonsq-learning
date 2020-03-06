package main

import (
	"fmt"
	"log"
	"time"

	"github.com/nsqio/go-nsq"
)

type myMessageHandler struct{}

// HandleMessage implements the Handler interface.
func (h *myMessageHandler) HandleMessage(m *nsq.Message) error {

	fmt.Printf("%#v \n", m)

	if len(m.Body) == 0 {
		// Returning nil will automatically send a FIN command to NSQ to mark the message as processed.
		return nil
	}

	processMessage := func(msg []byte) error {
		fmt.Println(string(msg))
		return nil
	}
	err := processMessage(m.Body)

	// Returning a non-nil error will automatically send a REQ command to NSQ to re-queue the message.
	return err
}

func main() {

	// Instantiate a producer.
	config := nsq.NewConfig()
	producer, err := nsq.NewProducer("39.106.208.186:4150", config)
	if err != nil {
		log.Fatal(err)
	}

	messageBody := []byte("hello")
	topicName := "topic"

	// Synchronously publish a single message to the specified topic.
	// Messages can also be sent asynchronously and/or in batches.
	err = producer.Publish(topicName, messageBody)
	if err != nil {
		log.Fatal(err)
	}

	// Gracefully stop the producer.
	producer.Stop()

	// Instantiate a consumer that will subscribe to the provided channel.
	config = nsq.NewConfig()
	consumer, err := nsq.NewConsumer("topic", "channel", config)
	if err != nil {
		log.Fatal(err)
	}

	// Set the Handler for messages received by this Consumer. Can be called multiple times.
	// See also AddConcurrentHandlers.
	consumer.AddHandler(&myMessageHandler{})

	// Use nsqlookupd to discover nsqd instances.
	// See also ConnectToNSQD, ConnectToNSQDs, ConnectToNSQLookupds.
	err = consumer.ConnectToNSQLookupd("39.106.208.186:4161")
	if err != nil {
		log.Fatal(err)
	}

	time.Sleep(2 * time.Second)
	consumer.Stop()
}
