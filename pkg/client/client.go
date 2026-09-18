package client

import (
	"bufio"
	"errors"
	"net"

	"github.com/ppovali/go-dirtmq/internal/protocol"
)

type Client struct {
	conn net.Conn
}

func Connect(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	return &Client{conn: conn}, nil
}

func (c *Client) Publish(topic string, payload []byte) error {
	if topic == "" || payload == nil {
		return errors.New("Topic or payload is nil")
	}

	packet := &protocol.Packet{
		Header: protocol.Header{
			Version:   1,
			Operation: protocol.OpPublish,
		},
		Topic:   topic,
		Payload: payload,
	}

	binaryFrame, err := protocol.SerializePacket(packet)
	if err != nil {
		return err
	}

	_, err = c.conn.Write(binaryFrame)
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) Subscribe(topic string) (<-chan *protocol.Packet, error) {
	if topic == "" {
		return nil, errors.New("Topic can't be empty")
	}

	ch := make(chan *protocol.Packet, 100)

	packet := &protocol.Packet{
		Header: protocol.Header{
			Version:   1,
			Operation: protocol.OpSubscribe,
		},
		Topic: topic,
	}

	binaryFrame, err := protocol.SerializePacket(packet)
	if err != nil {
		return nil, err
	}

	_, err = c.conn.Write(binaryFrame)
	if err != nil {
		return nil, err
	}

	go func() {
		reader := bufio.NewReader(c.conn)
		for {
			packet, err := protocol.DecodePacket(reader)
			if err != nil {
				close(ch)
				return
			}
			ch <- packet
		}
	}()

	return ch, nil
}

func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
