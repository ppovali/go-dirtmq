package main

import (
	"log"
	"net"
	"sync"
	"time"

	"github.com/ppovali/go-dirtmq/internal/protocol"
)

func main() {
	concurrency := 1000
	messagesPerWorker := 500

	var wg sync.WaitGroup
	startTime := time.Now()
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			conn, err := net.Dial("tcp", "localhost:8080")
			if err != nil {
				return
			}

			packet := &protocol.Packet{
				Header: protocol.Header{
					Version:   1,
					Operation: protocol.OpPublish,
				},
				Topic:   "stress-topic",
				Payload: []byte{0x01, 0x4B, 0xFF, 0x2A},
			}
			binaryFrame, err := protocol.SerializePacket(packet)
			if err != nil {
				return
			}
			for k := 0; k < messagesPerWorker; k++ {
				conn.Write(binaryFrame)
			}
		}(i)
	}

	wg.Wait()
	testTime := time.Since(startTime).Seconds()
	log.Printf("QPS: %f", float64(500000)/testTime)
}
