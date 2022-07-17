package main

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "go-kafka_kafka_1:9092",
		"client.id": "goapp-consumer",
		"group.id": "goapp-group",
	}

	consumer, err := kafka.NewConsumer(configMap)
	if err != nil {
		fmt.Println("error consumer ", err.Error())
	}

	topics := []string{"teste"}
	err = consumer.SubscribeTopics(topics, nil)

	for {
		msg, err := consumer.ReadMessage(-1)
		if err == nil {
			fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
		} 
	}
}