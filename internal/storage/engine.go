package storage

import (
	"errors"
	"log"
	"net"
	"sync"
	"time"

	"github.com/ppovali/go-dirtmq/internal/protocol"
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

	p := &protocol.Packet{
		Header: protocol.Header{
			Version:   protocol.ProtocolVersion,
			Operation: protocol.OpPublish,
		},
		Topic:   topic,
		Payload: payload,
	}

	e.mu.RLock()
	conns, exists := e.subscribers[topic]
	e.mu.RUnlock()

	if !exists || len(conns) == 0 {
		return // No one is listening
	}

	binaryFrame, err := protocol.SerializePacket(p)
	if err != nil {
		log.Printf("Failed to serialize packet for topic [%s]: %v", topic, err)
		return
	}

	log.Printf("Broadcasting message to %d subscribers on topic [%s]", len(conns), topic)

	for _, conn := range conns {
		_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))

		_, err := conn.Write(binaryFrame)
		if err != nil {
			log.Printf("Failed to write subscriber %s: %v", conn.RemoteAddr(), err)
			e.RemoveSubscriber(topic, conn)
		}
	}

	protocol.BufferPool.Put(binaryFrame[:cap(binaryFrame)])
}

func (e *Engine) RemoveSubscriber(topic string, conn net.Conn) error {
	if topic == "" || conn == nil {
		return errors.New("Invalid topic or connection instance")
	}

	e.mu.RLock()
	subscribers, exists := e.subscribers[topic]
	e.mu.RUnlock()

	if !exists {
		return errors.New("no subscribers found for the topic")
	}

	targetIndex := -1
	for i, subscriber := range subscribers {
		if subscriber == conn {
			targetIndex = i
			break
		}
	}

	if targetIndex == -1 {
		return nil
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if targetIndex < len(e.subscribers[topic]) && e.subscribers[topic][targetIndex] == conn {
		e.subscribers[topic] = append(e.subscribers[topic][:targetIndex], e.subscribers[topic][targetIndex+1:]...)
	}

	return nil
}
