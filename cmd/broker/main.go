package main

import (
	"bufio"
	"io"
	"log"
	"net"
	"time"
)

func main() {
	listener, err := net.Listen("tcp", ":8080")

	if err != nil {
		log.Fatal("Error listening:", err)
	}

	defer listener.Close()

	for {
		conn, err := listener.Accept()

		if err != nil {
			log.Println("Error accepting connection:", err)
			continue
		}

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {

	log.Println("User active")

	defer log.Println("User disconnect")
	defer conn.Close()

	reader := bufio.NewReader(conn)
	for {
		err := conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		if err != nil {
			log.Println("Failed to set deadline:", err)
			return
		}

		message, err := reader.ReadBytes('\n')
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				log.Println("No data received for 5 seconds")
				break
			}
			if err != io.EOF {
				log.Println("Error reading message:", err)
			}
			return
		}
		log.Printf("Received message: %s", message)
	}
}
