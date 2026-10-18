package main

import (
	"crypto/rand"
	"log"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ppovali/go-dirtmq/internal/protocol"
)

func main() {
	concurrency := 40
	messagesPerWorker := 2000
	payloadSize := 4096
	brokerAddr := os.Getenv("DIRTMQ_BROKER_ADDR")
	if brokerAddr == "" {
		brokerAddr = "127.0.0.1:8080"
	}

	payload := make([]byte, payloadSize)
	if payloadSize > 10 {
		_, _ = rand.Read(payload)
	} else {
		payload = []byte{0x01, 0x4B, 0xFF, 0x2A}
	}

	packet := &protocol.Packet{
		Header: protocol.Header{
			Version:   1,
			Operation: protocol.OpPublish,
		},
		Topic:   "stress-topic",
		Payload: payload,
	}
	binaryFrame, err := protocol.SerializePacket(packet)
	if err != nil {
		return
	}

	runBenchmark(brokerAddr, 20, 1000, binaryFrame)
	time.Sleep(500 * time.Millisecond)

	totalSent, duration := runBenchmark(brokerAddr, concurrency, messagesPerWorker, binaryFrame)
	totalBytes := totalSent * uint64(len(binaryFrame))
	log.Printf("QPS: %f", float64(totalSent)/duration)
	log.Printf("I/O speed: %f MB/s", (float64(totalBytes)/1024/1024)/(duration))
}

func runBenchmark(addr string, concurrency, messagesPerWorker int, binaryFrame []byte) (uint64, float64) {
	var wg sync.WaitGroup
	var successCount uint64

	startTime := time.Now()
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				log.Println("[ERROR] Failed to connect to broker:", err)
				return
			}
			defer conn.Close()

			for k := 0; k < messagesPerWorker; k++ {
				_, err := conn.Write(binaryFrame)
				if err != nil {
					log.Printf("[ERROR] Worker %d failed to send message %d: %v", workerID, k, err)
					break
				}
				atomic.AddUint64(&successCount, 1)

				time.Sleep(200 * time.Microsecond)
				// runtime.Gosched()
			}
		}(i)
	}

	wg.Wait()

	return atomic.LoadUint64(&successCount), time.Since(startTime).Seconds()
}
