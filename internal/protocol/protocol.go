package protocol

import (
	"encoding/binary"
	"errors"
	"io"
)

// ProtocolVersion is the version of the protocol used for communication between clients and the broker.
const ProtocolVersion byte = 1

const (
	OpPublish   byte = 1
	OpSubscribe byte = 2
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
