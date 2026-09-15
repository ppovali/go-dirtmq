package protocol

import (
	"testing"
)

func BenchmarkSerializePacket(b *testing.B) {
	packet := &Packet{
		Header: Header{
			Version:   1,
			Operation: OpPublish,
		},
		Topic:   "orders.v1.processed",
		Payload: []byte("{\"user_id\": 9982, \"amount\": 450.00, \"status\": \"success\"}"),
	}

	for b.Loop() {
		BinaryFrame, _ := SerializePacket(packet)

		BufferPool.Put(BinaryFrame[:cap(BinaryFrame)])
	}
}
