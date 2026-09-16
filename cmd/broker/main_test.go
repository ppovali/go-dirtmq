package main

import (
	"net"
	"testing"
	"time"

	"github.com/ppovali/go-dirtmq/internal/protocol"
	"github.com/ppovali/go-dirtmq/internal/storage"
)

func TestBroker_EndToEndPipeline(t *testing.T) {
	engine := storage.NewEngine()
	testAddr := "127.0.0.1:8085"

	listener, err := net.Listen("tcp", testAddr)
	if err != nil {
		t.Fatalf("Failed to bind test port: %v", err)
	}
	defer listener.Close()

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go handleConnection(conn, engine)
		}
	}()

	time.Sleep(50 * time.Millisecond)

	subConn, err := net.Dial("tcp", testAddr)
	if err != nil {
		t.Fatalf("Subscriber failed to connect: %v", err)
	}
	defer subConn.Close()

	subPacket := &protocol.Packet{
		Header: protocol.Header{
			Version:   1,
			Operation: protocol.OpSubscribe,
		},
		Topic: "test-channel",
	}
	subBytes, _ := protocol.SerializePacket(subPacket)
	_, _ = subConn.Write(subBytes)

	time.Sleep(50 * time.Millisecond)

	pubConn, err := net.Dial("tcp", testAddr)
	if err != nil {
		t.Fatalf("Publisher failed to connect: %v", err)
	}
	defer pubConn.Close()

	exceptedData := []byte("test-payload-data")
	pubPacket := &protocol.Packet{
		Header: protocol.Header{
			Version:   1,
			Operation: protocol.OpPublish,
		},
		Topic:   "test-channel",
		Payload: exceptedData,
	}
	pubBytes, _ := protocol.SerializePacket(pubPacket)
	_, _ = pubConn.Write(pubBytes)

	_ = subConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	recivedPacket, err := protocol.DecodePacket(subConn)
	if err != nil {
		t.Fatalf("Failed to decode incoming broadcast packet: %v", err)
	}

	if recivedPacket.Header.Operation != protocol.OpSend {
		t.Errorf("Excepted operation %d, recived: %d", protocol.OpSend, recivedPacket.Header.Operation)
	}

	if string(recivedPacket.Payload) != string(exceptedData) {
		t.Errorf("Excepted payload %s, recived: %s", string(exceptedData), string(recivedPacket.Payload))
	}
}
