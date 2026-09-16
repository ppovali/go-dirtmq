package client

import (
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

func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
