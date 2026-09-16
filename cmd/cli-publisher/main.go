package main

import (
	"fmt"
	"log"
	"net"

	"github.com/ppovali/go-dirtmq/internal/protocol"
)

func main() {
	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		log.Fatalf("Failed to connect to broker: %v", err)
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
		log.Fatalf("Failed to serialize packet: %v", err)
	}

	_, err = conn.Write(binaryFrame)
	if err != nil {
		log.Fatalf("Failed to stream binary frame to socket: %v", err)
	}

	fmt.Printf("[SENT] Successfully fired packed binary message to topic [%s]", topic)
}
