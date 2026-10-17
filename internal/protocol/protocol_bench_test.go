package protocol

import (
	"bytes"
	"testing"
)

func BenchmarkSerializePacket(b *testing.B) {
	packet := Packet{
		Header: Header{
			Version:   1,
			Operation: OpPublish,
		},
		Topic:   "orders.v1.processed",
		Payload: []byte("{\"user_id\": 9982, \"amount\": 450.00, \"status\": \"success\"}"),
	}

	b.ResetTimer()

	for b.Loop() {
		BinaryFrame, _ := SerializePacket(&packet)
		ReleaseBuffer(BinaryFrame)
	}
}

func BenchmarkDecodePacket(b *testing.B) {
	dummyPacket := &Packet{
		Header: Header{
			Version:   1,
			Operation: OpPublish,
		},
		Topic:   "orders.v1.processed",
		Payload: []byte("{\"user_id\": 9982, \"amount\": 450.00, \"status\": \"success\"}"),
	}

	binaryFrame, _ := SerializePacket(dummyPacket)

	readerObject := bytes.NewReader(binaryFrame)

	b.ResetTimer()
	for b.Loop() {
		readerObject.Reset(binaryFrame)
		_, _ = DecodePacket(readerObject)
	}
}
