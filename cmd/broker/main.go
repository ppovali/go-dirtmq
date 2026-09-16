package main

import (
	"bufio"
	"io"
	"log"
	"net"
	"time"

	"github.com/ppovali/go-dirtmq/internal/protocol"
	"github.com/ppovali/go-dirtmq/internal/storage"
)

func main() {
	listener, err := net.Listen("tcp", ":8080")

	if err != nil {
		log.Fatal("Error listening:", err)
	}

	defer listener.Close()

	brokerEngine := storage.NewEngine()

	for {
		conn, err := listener.Accept()

		if err != nil {
			log.Println("Error accepting connection:", err)
			continue
		}

		go handleConnection(conn, brokerEngine)
	}
}

func handleConnection(conn net.Conn, engine *storage.Engine) {

	log.Println("Service active:", conn.RemoteAddr())

	var subscribedTopics []string

	defer func() {
		log.Println("Service disconnect: ", conn.RemoteAddr())
		conn.Close()

		for _, topic := range subscribedTopics {
			log.Printf("Removing subscriber %s from topic [%s]", conn.RemoteAddr(), topic)
			engine.RemoveSubscriber(topic, conn)
		}
	}()

	reader := bufio.NewReader(conn)

	for {
		err := conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		if err != nil {
			log.Println("Failed to set deadline:", err)
			return
		}

		packet, err := protocol.DecodePacket(reader)

		if err != nil {

			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				log.Printf("No data received for 5 seconds, client: %s", conn.RemoteAddr())
				continue
			}

			// Check if the client has closed the connection
			if err != io.EOF {
				log.Println("Error reading message:", err)
			}

			return
		}

		log.Printf("Received packet: OpCode=%d | Topic=%s | Bytes=%d\n",
			packet.Header.Operation, packet.Topic, len(packet.Payload))

		switch packet.Header.Operation {
		case protocol.OpPublish:
			err := engine.Publish(packet.Topic, packet.Payload)
			if err != nil {
				log.Println("Storage engine save error:", err)
				return
			}
			log.Printf("[PUBLISH] saving message to topic: %s", packet.Topic)
			engine.Broadcast(packet.Topic, packet.Payload)

		case protocol.OpSubscribe:
			err := engine.RegisterSubscriber(packet.Topic, conn)
			if err != nil {
				log.Println("Storage subscribe error: ", err)
				return
			}
			subscribedTopics = append(subscribedTopics, packet.Topic)
			log.Printf("[SUBSCRIBE] microservice '%s' subscribed to topic: %s", conn.RemoteAddr(), packet.Topic)
		}
	}
}
