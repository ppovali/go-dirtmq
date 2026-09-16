package protocol

import (
	"encoding/binary"
	"errors"
	"io"
	"sync"
)

// ProtocolVersion is the version of the protocol used for communication between clients and the broker.
const ProtocolVersion byte = 1

const (
	OpPublish   byte = 1
	OpSubscribe byte = 2
	OpSend      byte = 3
)

const HeaderSize byte = 1 + 1 + 2 + 4 // Version (1 byte) + Operation (1 byte) + TopicLength (2 bytes) + PayloadLength (4 bytes)

type Header struct {
	// Version is the version of the protocol used for communication between clients and the broker.
	Version uint8

	// Operation is the operation to be performed (e.g., publish, subscribe).
	Operation byte

	// TopicLength is the length of the topic name.
	TopicLength uint16

	// PayloadLength is the length of the payload.
	PayloadLength uint32
}

type Packet struct {
	Header  Header
	Topic   string
	Payload []byte
}

var BufferPool = &sync.Pool{
	New: func() any {
		return make([]byte, 4096)
	},
}

func DecodeHeader(r io.Reader) (Header, error) {
	buf := make([]byte, HeaderSize)

	_, err := io.ReadFull(r, buf)
	if err != nil {
		return Header{}, err
	}

	var header Header

	header.Version = buf[0]
	header.Operation = buf[1]
	header.TopicLength = binary.BigEndian.Uint16(buf[2:4])
	header.PayloadLength = binary.BigEndian.Uint32(buf[4:8])

	if header.Version != ProtocolVersion {
		return Header{}, errors.New("unsupported protocol version")
	}

	return header, nil
}

func DecodePacket(r io.Reader) (*Packet, error) {

	header, err := DecodeHeader(r)
	if err != nil {
		return &Packet{}, err
	}

	if header.TopicLength > 1024 {
		return &Packet{}, errors.New("topic length exceeds maximum allowed size")
	}

	topicBuf := make([]byte, header.TopicLength)
	if header.TopicLength > 0 {
		_, err = io.ReadFull(r, topicBuf)
		if err != nil {
			return &Packet{}, errors.New("failed to read topic")
		}
	}

	payloadBuf := make([]byte, header.PayloadLength)
	if header.PayloadLength > 0 {
		_, err = io.ReadFull(r, payloadBuf)
		if err != nil {
			return &Packet{}, errors.New("failed to read payload")
		}
	}

	return &Packet{
		Header:  header,
		Topic:   string(topicBuf),
		Payload: payloadBuf,
	}, nil
}

func SerializePacket(packet *Packet) ([]byte, error) {
	if packet == nil {
		return nil, errors.New("packet is nil")
	}

	totalLength := int(HeaderSize) + len(packet.Topic) + len(packet.Payload)

	if totalLength > 4096 {
		return nil, errors.New("packet size exceeds maximum allowed size")
	}

	rawBuf := BufferPool.Get().([]byte)

	rawBuf[0] = ProtocolVersion
	rawBuf[1] = packet.Header.Operation
	binary.BigEndian.PutUint16(rawBuf[2:4], uint16(len(packet.Topic)))
	binary.BigEndian.PutUint32(rawBuf[4:8], uint32(len(packet.Payload)))

	copy(rawBuf[8:], []byte(packet.Topic))
	copy(rawBuf[8+len(packet.Topic):], packet.Payload)

	return rawBuf[:totalLength], nil
}
