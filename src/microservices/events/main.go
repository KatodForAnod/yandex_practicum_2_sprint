package main

import (
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"log"
	"net/http"
	"os"
	"time"
)

func ReadEvent(consumer *kafka.Consumer) {
	consumer.SubscribeTopics([]string{"user-events", "payment-events", "movie-events"}, nil)

	for {
		msg, err := consumer.ReadMessage(-1) // -1 for indefinite timeout
		if err == nil {
			fmt.Println(string(msg.Value))
		} else if !err.(kafka.Error).IsFatal() {
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
			return
		} else {
			fmt.Printf("Fatal consumer error: %v\n", err)
			return
		}
	}
}

type UserEvent struct {
	UserID    int64     `json:"user_id"`
	Username  string    `json:"username"`
	Action    string    `json:"action"`
	Timestamp time.Time `json:"timestamp"`
}

type PaymentEvent struct {
	PaymentID  int64     `json:"payment_id"`
	UserID     int64     `json:"user_id"`
	Amount     float64   `json:"amount"`
	Status     string    `json:"status"`
	Timestamp  time.Time `json:"timestamp"`
	MethodType string    `json:"method_type"`
}

type MovieEvent struct {
	MovieID int64  `json:"movie_id"`
	Title   string `json:"title"`
	Action  string `json:"action"`
	UserID  int64  `json:"user_id"`
}

type HttpBroker struct {
	producer *kafka.Producer
}

func (receiver HttpBroker) CreateUserEvent(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	var t UserEvent
	err := decoder.Decode(&t)
	if err != nil {
		panic(err)
	}
	log.Println(t)

	w.WriteHeader(http.StatusCreated)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "success"})

	topic := "user-events"
	value, err := json.Marshal(&t)
	if err != nil {
		panic(err)
	}
	err = receiver.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          value,
	}, nil)
	if err != nil {
		fmt.Printf("Failed to produce message: %v\n", err)
	} else {
		fmt.Printf("Produced message to %s: %s\n", topic, value)
	}
}

func (receiver HttpBroker) CreateMovieEvent(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	var t MovieEvent
	err := decoder.Decode(&t)
	if err != nil {
		panic(err)
	}
	log.Println(t)

	w.WriteHeader(http.StatusCreated)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "success"})

	topic := "movie-events"
	value, err := json.Marshal(&t)
	if err != nil {
		panic(err)
	}
	err = receiver.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          value,
	}, nil)
	if err != nil {
		fmt.Printf("Failed to produce message: %v\n", err)
	} else {
		fmt.Printf("Produced message to %s: %s\n", topic, value)
	}
}

func (receiver HttpBroker) CreatePaymentEvent(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	var t PaymentEvent
	err := decoder.Decode(&t)
	if err != nil {
		panic(err)
	}
	log.Println(t)

	w.WriteHeader(http.StatusCreated)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "success"})

	topic := "payment-events"
	value, err := json.Marshal(&t)
	if err != nil {
		panic(err)
	}
	err = receiver.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          value,
	}, nil)
	if err != nil {
		fmt.Printf("Failed to produce message: %v\n", err)
	} else {
		fmt.Printf("Produced message to %s: %s\n", topic, value)
	}
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"status": true})
}

func main() {
	kafkaServer := os.Getenv("KAFKA_BROKERS")

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": kafkaServer})
	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		panic(err)
	}
	defer p.Close()

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaServer,
		"auto.offset.reset": "earliest",
		"group.id":          "events_consumer_group",
	})
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	go ReadEvent(consumer)

	httpBroker := HttpBroker{producer: p}

	port := os.Getenv("PORT")

	http.HandleFunc("/api/events/payment", httpBroker.CreatePaymentEvent)
	http.HandleFunc("/api/events/user", httpBroker.CreateUserEvent)
	http.HandleFunc("/api/events/movie", httpBroker.CreateMovieEvent)
	http.HandleFunc("/api/events/health", handleHealth)

	http.ListenAndServe(":"+port, nil)
}
