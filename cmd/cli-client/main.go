package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"time"
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
	buf := make([]byte, 1024)
	for {
		_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		n, err := conn.Read(buf)
		if err != nil {
			log.Fatalf("Disconnected from broker stream: %v", err)
		}
		fmt.Printf("[BROADCAST RECV]: %s\n", string(buf[:n]))
	}
}
