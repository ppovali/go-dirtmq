package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"

	"github.com/ppovali/go-dirtmq/internal/protocol"
)

func main() {
	brokerAddr := os.Getenv("DIRTMQ_BROKER_ADDR")
	if brokerAddr == "" {
		brokerAddr = "localhost:8080"
	}

	log.Printf("Connecting to go-DirtMQ cluster endpoint at [%s]", brokerAddr)

	conn, err := net.Dial("tcp", brokerAddr)
	if err != nil {
		log.Fatalf("Failed to connect to broker: %v", err)
	}
	defer conn.Close()

	topic := "orders"
	packet := &protocol.Packet{
		Header: protocol.Header{
			Version:   1,
			Operation: 2,
		},
		Topic: topic,
	}

	binaryFrame, err := protocol.SerializePacket(packet)
	if err != nil {
		log.Fatalf("Failed to serialize packet: %v", err)
	}

	_, err = conn.Write(binaryFrame)
	if err != nil {
		log.Fatalf("Failed to stream binary frame to socket: %v", err)
	}

	log.Printf("Subscriber CLI emulator activated on topic [%s]. Standing by for live events...", topic)

	reader := bufio.NewReader(conn)
	for {
		packet, err := protocol.DecodePacket(reader)
		if err != nil {
			log.Fatalf("Disconnected from broker stream: %v", err)
		}

		if packet.Header.Operation == protocol.OpSend {
			fmt.Printf("[BROADCAST RECV] Topic: %s | Payload Size: %d bytes", packet.Topic, len(packet.Payload))
			fmt.Printf("\nPayload: %s\n", string(packet.Payload))
		} else {
			log.Printf("Received unexpected packet operation: %d", packet.Header.Operation)
		}
	}
}
