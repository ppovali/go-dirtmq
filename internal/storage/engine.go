package storage

import (
	"errors"
	"log"
	"net"
	"sync"

	"github.com/ppovali/go-dirtmq/internal/protocol"
)

type Engine struct {
	mu          sync.RWMutex
	topics      map[string][][]byte
	subscribers map[string][]*Subscriber
}

type Subscriber struct {
	conn  net.Conn
	queue chan []byte
}

func NewEngine() *Engine {
	return &Engine{
		topics: make(map[string][][]byte),

		subscribers: make(map[string][]*Subscriber),
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

func (e *Engine) GetMessages(topic string) ([][]byte, error) {
	if topic == "" {
		return nil, errors.New("topic name cannot be empty")
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	messages := e.topics[topic]

	return messages, nil
}

func (e *Engine) RegisterSubscriber(topic string, conn net.Conn) error {
	if topic == "" || conn == nil {
		return errors.New("Invalid topic or connection instance")
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	sub := &Subscriber{
		conn:  conn,
		queue: make(chan []byte, 1000),
	}
	e.subscribers[topic] = append(e.subscribers[topic], sub)

	go func(s *Subscriber) {
		for frame := range s.queue {
			_, err := s.conn.Write(frame)
			if err != nil {
				return
			}
		}
	}(sub)
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
			Operation: protocol.OpSend,
		},
		Topic:   topic,
		Payload: payload,
	}

	e.mu.RLock()
	subs, exists := e.subscribers[topic]
	e.mu.RUnlock()

	if !exists || len(subs) == 0 {
		return // No one is listening
	}

	binaryFrame, err := protocol.SerializePacket(p)
	if err != nil {
		log.Printf("Failed to serialize packet for topic [%s]: %v", topic, err)
		return
	}

	log.Printf("Broadcasting message to %d subscribers on topic [%s]", len(subs), topic)

	for _, sub := range subs {
		frameCopy := make([]byte, len(binaryFrame))
		copy(frameCopy, binaryFrame)

		select {
		case sub.queue <- frameCopy:

		default:
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
		if subscriber.conn == conn {
			targetIndex = i
			break
		}
	}

	if targetIndex == -1 {
		return nil
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if targetIndex < len(e.subscribers[topic]) && e.subscribers[topic][targetIndex].conn == conn {
		e.subscribers[topic] = append(e.subscribers[topic][:targetIndex], e.subscribers[topic][targetIndex+1:]...)
	}

	return nil
}
