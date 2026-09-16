package main

import (
	"bufio"
	"encoding/binary"
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
	topicBytes := []byte(topic)

	header := make([]byte, 8)
	header[0] = 1
	header[1] = 2 // OpCode (OpSubscribe)
	binary.BigEndian.PutUint16(header[2:4], uint16(len(topicBytes)))
	binary.BigEndian.PutUint32(header[4:8], 0)

	_, _ = conn.Write(header)
	_, _ = conn.Write(topicBytes)

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
