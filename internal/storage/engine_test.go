package storage

import (
	"bytes"
	"sync"
	"testing"
)

func TestEngine_PublishAndGet(t *testing.T) {
	engine := NewEngine()
	topic := "test-topic"
	message := []byte("hello-dirt")

	err := engine.Publish(topic, message)
	if err != nil {
		t.Fatalf("Failed to publish message: %v", err)
	}

	messages, err := engine.GetMessage(topic)
	if err != nil {
		t.Fatalf("Failed to rerieve message: %v", err)
	}

	if len(messages) != 1 {
		t.Errorf("Expecred 1 message, got: %d", len(messages))
	}

	if !bytes.Equal(messages[0], message) {
		t.Errorf("Expected data '%s', got: %s", string(message), string(messages[0]))
	}
}

func TestEngine_ConcurrentPublish(t *testing.T) {
	engine := NewEngine()
	topic := "concurrent-topic"

	var wg sync.WaitGroup
	workers := 100

	for i := 0; i != workers; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			_ = engine.Publish(topic, []byte("spawn"))
		}()
	}

	wg.Wait()

	messages, _ := engine.GetMessage(topic)
	if len(messages) != workers {
		t.Errorf("Expecred %d messages in memory, got: %d", workers, len(messages))
	}
}
