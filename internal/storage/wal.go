package storage

import (
	"bufio"
	"io"
	"os"
	"sync"
	"time"

	"github.com/ppovali/go-dirtmq/internal/protocol"
)

const (
	bufferSize = 65536 // 64KB buffer size for reading from the WAL file
)

type WAL struct {
	file   *os.File
	writer *bufio.Writer
	mu     sync.Mutex
	cancel chan struct{}
}

func NewWAl(filepath string) (*WAL, error) {
	file, err := os.OpenFile(filepath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}

	wal := &WAL{
		file:   file,
		writer: bufio.NewWriterSize(file, bufferSize),
		cancel: make(chan struct{}),
	}

	return wal, nil
}

func (w *WAL) Start() {
	go w.syncLoop()
}

func (w *WAL) Append(data []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	n, err := w.writer.Write(data)
	if err != nil {
		return err
	}
	if n != len(data) {
		return io.ErrShortWrite
	}

	if w.writer.Available() < 8192 {
		if err := w.writer.Flush(); err != nil {
			return err
		}
	}

	return nil
}

func (w *WAL) syncLoop() {
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			w.mu.Lock()
			if w.writer.Buffered() > 0 {
				_ = w.writer.Flush()
				_ = w.file.Sync()
			}
			w.mu.Unlock()
		case <-w.cancel:
			return
		}
	}
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
	close(w.cancel)

	w.mu.Lock()
	defer w.mu.Unlock()

	var err error

	if flushErr := w.writer.Flush(); flushErr != nil {
		err = flushErr
	}

	if syncErr := w.file.Sync(); syncErr != nil && err == nil {
		err = syncErr
	}

	if closeErr := w.file.Close(); closeErr != nil && err == nil {
		err = closeErr
	}

	return err
}
