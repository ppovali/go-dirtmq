package main

import (
	"log"
	"os"

	"github.com/ppovali/go-dirtmq/pkg/client"
)

func main() {
	brokerAddr := os.Getenv("DIRTMQ_BROKER_ADDR")
	if brokerAddr == "" {
		brokerAddr = "localhost:8080"
	}

	client, err := client.Connect(brokerAddr)
	if err != nil {
		log.Fatalf("Failed to connect to broker %s: %v", brokerAddr, err)
	}

	topic := "orders"
	messages, err := client.Subscribe(topic)
	if err != nil {
		log.Fatalf("Failed to subscribe topic [%s]: %v", topic, err)
	}

	for packet := range messages {
		payload := string(packet.Payload)
		log.Printf("Recieved a new payload: %s", payload)
	}
}
