package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {

	deliveryChan := make(chan kafka.Event)

	producer := NewKafkaProducer()
	defer producer.Close()

	Publish("Transferiu", "teste", producer, []byte("transferencia"), deliveryChan)

	go DeliveryReport(deliveryChan)
	

	// e:= <-deliveryChan
	// msg := e.(*kafka.Message)
	// if msg.TopicPartition.Error != nil {
	// 	fmt.Println("Delivery failed:", msg.TopicPartition.Error)
	// } else {
	// 	fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
	// 		msg.TopicPartition.Topic, msg.TopicPartition.Partition, msg.TopicPartition.Offset)
	// }

	producer.Flush(2000)

}

func NewKafkaProducer() *kafka.Producer {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "go-kafka_kafka_1:9092",
		"delivery.timeout.ms": "0",
		"acks": "all",
		"enable.idempotence": "true",
	}

	producer, err := kafka.NewProducer(configMap)
	if err != nil {
		log.Println(err.Error())
	}

	return producer
}

func Publish(msg string, topic string, producer *kafka.Producer, key []byte, deliveryChan chan kafka.Event) error {
	message := kafka.Message{
		Value: []byte(msg),
		Key: key,
		TopicPartition: kafka.TopicPartition{
			Topic: &topic,
			Partition: kafka.PartitionAny,
		},
	}

	err := producer.Produce(&message, deliveryChan)
	if err != nil {
		return err
	}
	return nil
}

func DeliveryReport(deliveryChan chan kafka.Event) {
	for e := range deliveryChan {
		switch event := e.(type) {
		case *kafka.Message:
			if event.TopicPartition.Error != nil {
				fmt.Printf("Delivery failed: %v\n", event.TopicPartition.Error)
			} else {
				fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
					event.TopicPartition.Topic, event.TopicPartition.Partition, event.TopicPartition.Offset)
			}
		}
	}
}