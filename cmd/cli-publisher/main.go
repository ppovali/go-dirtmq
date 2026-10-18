package main

import (
	"fmt"
	"log"
	"net"
	"os"

	"github.com/ppovali/go-dirtmq/internal/protocol"
)

func main() {
	brokerAddr := os.Getenv("DIRTMQ_BROKER_ADDR")
	if brokerAddr == "" {
		brokerAddr = "127.0.0.1:8080"
	}

	conn, err := net.Dial("tcp", brokerAddr)
	if err != nil {
		log.Fatalf("[ERROR] Failed to connect to broker: %v", err)
	}
	defer conn.Close()

	topic := "orders"
	payload := "test-payload-data"
	payloadBytes := []byte(payload)
	packet := &protocol.Packet{
		Header: protocol.Header{
			Version:   1,
			Operation: protocol.OpPublish,
		},
		Topic:   topic,
		Payload: payloadBytes,
	}

	binaryFrame, err := protocol.SerializePacket(packet)
	if err != nil {
		log.Fatalf("[ERROR] Failed to serialize packet: %v", err)
	}

	_, err = conn.Write(binaryFrame)
	if err != nil {
		log.Fatalf("[ERROR] Failed to stream binary frame to socket: %v", err)
	}

	fmt.Printf("[SENT] Successfully fired packed binary message to topic [%s]", topic)
}
