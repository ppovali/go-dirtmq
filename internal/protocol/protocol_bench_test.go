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

func BenchmarkDecodePacker(b *testing.B) {
	dummyPacket := &Packet{
		Header: Header{
			Version:   1,
			Operation: OpPublish,
		},
		Topic:   "orders.v1.processed",
		Payload: []byte("{\"user_id\": 9982, \"amount\": 450.00, \"status\": \"success\"}"),
	}

	binaryFrame, _ := SerializePacket(dummyPacket)

	b.ResetTimer()
	for b.Loop() {
		readerObject := bytes.NewReader(binaryFrame)
		_, _ = DecodePacket(readerObject)
	}
}
