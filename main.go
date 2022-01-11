package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	consumer "github.com/mistsys/sarama-consumer"
	"github.com/mistsys/sarama-consumer/offsets"
	"github.com/mistsys/sarama-consumer/stable"
	"log"
	"sync"
	"time"
)

func main() {
	// create a suitable sarama.Client
	sconfig := sarama.NewConfig()
	sconfig.Version = consumer.MinVersion // consumer requires at least 0.9
	sconfig.Consumer.Return.Errors = true // needed if asynchronous ErrOffsetOutOfRange handling is desired (it's a good idea)
	sclient, _ := sarama.NewClient([]string{"localhost:9092"}, nil)

	// from that, create a consumer.Config with some fancy options
	config := consumer.NewConfig()
	config.Partitioner = stable.New(false)                                                 // use a stable (but inconsistent) partitioner
	config.StartingOffset, config.OffsetOutOfRange = offsets.NoOlderThan(time.Second * 30) // always start and restart no more than 30 seconds in the past (NOTE: requires kafka 0.10 brokers to work properly)

	// and finally a consumer Client
	client, _ := consumer.NewClient("forwarder", config, sclient)
	defer client.Close() // not strictly necessary, since we don't exit, but this is example code and someone might C&V it and exit

	// consume and print errors
	go func() {
		for err := range client.Errors() {
			fmt.Println(err)
		}
	}()
	MainConsumer(client)

}

func HandleNew(message []byte) {
	println("Handle test func")
	println(string(message))
}
func HandleTest(message []byte) {
	println("Handle USSD request")
	println(string(message))
}
func MainConsumer(client consumer.Client) {
	var topicsWithFuns = map[string]func([]byte){
		"newTopic":  HandleNew,
		"testTopic": HandleTest,
	}
	var wg sync.WaitGroup
	// consume a topic
	topics := []string{"newTopic", "ussd-request"}
	topicConsumers, _ := client.ConsumeMany(topics)
	wg.Add(len(topicConsumers))
	for i, topicConsumer := range topicConsumers {
		println("Index: ", i)
		go func(topicConsumer consumer.Consumer) {
			for msg := range topicConsumer.Messages() {
				topicConsumer.Done(msg) // required
				topicsWithFuns[msg.Topic](msg.Value)
			}
			wg.Done()
		}(topicConsumer)
	}
	wg.Wait()
}
func MainProducer() {
	//addresses of available kafka brokers
	brokers := []string{"localhost:9092"}
	//setup relevant config info
	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true                   // Optional
	producer, err := sarama.NewAsyncProducer(brokers, config) // There is an option of SyncProducer
	defer producer.Close()
	if err != nil {
		println("Producer failed")
		log.Fatal(err)
	}
	topic := "testTopic" //e.g create-user-topic
	msg := "actual information to save on kafka"
	message := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(msg),
	}

	producer.Input() <- message
	if err != nil {
		println("Producing failed")
		log.Fatal(err)
	}
	println("Done producing")
}
