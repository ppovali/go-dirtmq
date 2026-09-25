package storage

import (
	"bufio"
	"io"
	"os"
	"sync"

	"github.com/ppovali/go-dirtmq/internal/protocol"
)

type WAL struct {
	file *os.File
	mu   sync.Mutex
}

func NewWAl(filepath string) (*WAL, error) {
	file, err := os.OpenFile(filepath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	return &WAL{file: file}, nil
}

func (w *WAL) Append(data []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	n, err := w.file.Write(data)
	if err != nil {
		return err
	}
	if n != len(data) {
		return io.ErrShortWrite
	}

	err = w.file.Sync()
	if err != nil {
		return err
	}

	return nil
}

func (w *WAL) RecoverState() ([]protocol.Packet, error) {
	readFile, err := os.Open(w.file.Name())
	if err != nil {
		return nil, err
	}
	defer readFile.Close()
	var packets []protocol.Packet = nil

	reader := bufio.NewReader(readFile)
	for {
		packet, err := protocol.DecodePacket(reader)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		packets = append(packets, *packet)
	}
	return packets, nil
}

func (w *WAL) Close() error {
	err := w.file.Close()
	if err != nil {
		return err
	}
	return nil
}
