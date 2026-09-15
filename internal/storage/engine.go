package storage

import (
	"errors"
	"log"
	"net"
	"sync"
	"time"
)

type Engine struct {
	mu          sync.RWMutex
	topics      map[string][][]byte
	subscribers map[string][]net.Conn
}

func NewEngine() *Engine {
	return &Engine{
		topics: make(map[string][][]byte),

		subscribers: make(map[string][]net.Conn),
	}
}

func (e *Engine) Publish(topic string, payload []byte) error {
	if topic == "" {
		return errors.New("topic name cannot be empty")
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	e.topics[topic] = append(e.topics[topic], payload)

	return nil
}

func (e *Engine) GetMessage(topic string) ([][]byte, error) {
	if topic == "" {
		return nil, errors.New("topic name cannot be empty")
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	message := e.topics[topic]

	return message, nil
}

func (e *Engine) RegisterSubscriber(topic string, conn net.Conn) error {
	if topic == "" || conn == nil {
		return errors.New("Invalid topic or connection instance")
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	e.subscribers[topic] = append(e.subscribers[topic], conn)

	return nil
}

// Broadcast streams raw bytes to all live socket pipes
func (e *Engine) Broadcast(topic string, payload []byte) {
	if topic == "" || len(payload) == 0 {
		return
	}

	e.mu.RLock()
	conns, exists := e.subscribers[topic]
	e.mu.RUnlock()

	if !exists || len(conns) == 0 {
		return //no one is listening
	}

	log.Printf("Broadcasting message to %d subscribers on topic [%s]", len(conns), topic)

	for _, conn := range conns {
		_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))

		_, err := conn.Write(payload)
		if err != nil {
			log.Printf("Failed to write subscriber %s: %v", conn.RemoteAddr(), err)
		}
	}

}
