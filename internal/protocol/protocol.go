package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
)

// ProtocolVersion is the version of the protocol used for communication between clients and the broker.
const ProtocolVersion byte = 1

type Operation byte

const (
	OpPublish   Operation = 1
	OpSubscribe Operation = 2
	OpSend      Operation = 3
)

const (
	HeaderSize    = 8 // Version (1 byte) + Operation (1 byte) + TopicLength (2 bytes) + PayloadLength (4 bytes)
	MaxPacketSize = 4096
	MaxBodySize   = MaxPacketSize - HeaderSize
	MaxTopicSize  = 1024
)

type Header struct {
	Version       uint8
	Operation     Operation
	TopicLength   uint16
	PayloadLength uint32
}

type Packet struct {
	Header  Header
	Topic   string
	Payload []byte
}

var bufferPool = &sync.Pool{
	New: func() any {
		return make([]byte, MaxPacketSize)
	},
}

func DecodeHeader(r io.Reader) (Header, error) {
	var buf [HeaderSize]byte

	_, err := io.ReadFull(r, buf[:])
	if err != nil {
		return Header{}, err
	}

	var header Header

	header.Version = buf[0]
	header.Operation = Operation(buf[1])
	header.TopicLength = binary.BigEndian.Uint16(buf[2:4])
	header.PayloadLength = binary.BigEndian.Uint32(buf[4:8])

	if header.Version != ProtocolVersion {
		return Header{}, errors.New("unsupported protocol version")
	}

	if !header.Operation.Valid() {
		return Header{}, errors.New("unsupported operation")
	}

	return header, nil
}

func DecodePacket(r io.Reader) (*Packet, error) {
	header, err := DecodeHeader(r)
	if err != nil {
		return nil, err
	}

	if header.TopicLength > MaxTopicSize {
		return nil, errors.New("topic length exceeds maximum allowed size")
	}

	totalPayLoadBytes := int(header.TopicLength) + int(header.PayloadLength)
	if totalPayLoadBytes > MaxBodySize {
		return nil, errors.New("packet data exceeds maximum buffer treshold bounds")
	}

	rawScratchBuf := bufferPool.Get().([]byte)
	defer bufferPool.Put(rawScratchBuf[:cap(rawScratchBuf)])

	if totalPayLoadBytes > 0 {
		_, err = io.ReadFull(r, rawScratchBuf[:totalPayLoadBytes])
		if err != nil {
			return nil, fmt.Errorf("failed to read packet body: %w", err)
		}
	}

	topicBytes := rawScratchBuf[:header.TopicLength]
	payloadBytes := rawScratchBuf[header.TopicLength:totalPayLoadBytes]

	topicBuf := string(topicBytes)
	payloadBuf := make([]byte, len(payloadBytes))
	copy(payloadBuf, payloadBytes)

	return &Packet{
		Header:  header,
		Topic:   topicBuf,
		Payload: payloadBuf,
	}, nil
}

func SerializePacket(packet *Packet) ([]byte, error) {
	if packet == nil {
		return nil, errors.New("packet is nil")
	}

	totalLength := int(HeaderSize) + len(packet.Topic) + len(packet.Payload)

	if totalLength > MaxPacketSize {
		return nil, errors.New("packet size exceeds maximum allowed size")
	}

	if len(packet.Topic) > MaxTopicSize {
		return nil, errors.New("topic is too large")
	}

	rawBuf := bufferPool.Get().([]byte)

	rawBuf[0] = ProtocolVersion
	rawBuf[1] = byte(packet.Header.Operation)
	binary.BigEndian.PutUint16(rawBuf[2:4], uint16(len(packet.Topic)))
	binary.BigEndian.PutUint32(rawBuf[4:8], uint32(len(packet.Payload)))

	copy(rawBuf[8:], packet.Topic)
	copy(rawBuf[8+len(packet.Topic):], packet.Payload)

	return rawBuf[:totalLength], nil
}

func (op Operation) Valid() bool {
	switch op {
	case OpPublish, OpSend, OpSubscribe:
		return true
	default:
		return false
	}
}

func (op Operation) String() string {
	switch op {
	case OpPublish:
		return "PUBLISH"
	case OpSubscribe:
		return "SUBSCRIBE"
	case OpSend:
		return "SEND"
	default:
		return "UNKNOWN"
	}
}

func ReleaseBuffer(buf []byte) {
	bufferPool.Put(buf[:cap(buf)])
}
