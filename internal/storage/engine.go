package storage

import (
	"errors"
	"sync"
)

type Engine struct {
	mu     sync.RWMutex
	topics map[string][][]byte
}

func NewEngine() *Engine {
	return &Engine{
		topics: make(map[string][][]byte),
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
