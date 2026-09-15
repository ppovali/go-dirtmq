package protocol

import (
	"bytes"
	"testing"
)

func TestDecodePacket_ValidData(t *testing.T) {
	mockNetworkStream := []byte{
		1,
		1,
		0, 3,
		0, 0, 0, 2,
		'a', 'b', 'c',
		'h', 'i',
	}

	reader := bytes.NewReader(mockNetworkStream)

	packet, err := DecodePacket(reader)

	if err != nil {
		t.Fatalf("DecodePacket returned an error: %v", err)
	}
	if packet.Topic != "abc" {
		t.Errorf("Expected topic 'abc', got '%s'", packet.Topic)
	}
	if string(packet.Payload) != "hi" {
		t.Errorf("Expected payload 'hi', got '%s'", packet.Payload)
	}
}
