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
	bufferSize              = 65536  // 64KB buffer size for reading from the WAL file
	queueSize               = 524288 // Size of the channel buffer for queued data
	writeAvailableThreshold = 8192   // Threshold for available buffer space before flushing
)

type WAL struct {
	file   *os.File
	writer *bufio.Writer
	queue  chan []byte
	wg     sync.WaitGroup
}

func NewWAl(filepath string) (*WAL, error) {
	file, err := os.OpenFile(filepath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}

	wal := &WAL{
		file:   file,
		writer: bufio.NewWriterSize(file, bufferSize),
		queue:  make(chan []byte, queueSize),
	}

	return wal, nil
}

func (w *WAL) Start() {
	w.wg.Add(1)
	go w.workerLoop()
}

func (w *WAL) Append(data []byte) error {
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)

	select {
	case w.queue <- dataCopy:
		return nil
	default:
		for i := 0; i < 5; i++ {
			time.Sleep(100 * time.Microsecond)

			select {
			case w.queue <- dataCopy:
				return nil
			default:
			}
		}
		return os.NewSyscallError("WAL queue overflow", nil)
	}
}

func (w *WAL) workerLoop() {
	defer w.wg.Done()

	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case data, ok := <-w.queue:
			if !ok {
				if w.writer.Buffered() > 0 {
					_ = w.writer.Flush()
					_ = w.file.Sync()
				}
				return
			}

			n, err := w.writer.Write(data)
			if err != nil || n != len(data) {
				continue
			}

			if w.writer.Available() < writeAvailableThreshold {
				_ = w.writer.Flush()
			}
		case <-ticker.C:
			if w.writer.Buffered() > 0 {
				_ = w.writer.Flush()
				_ = w.file.Sync()
			}
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
	close(w.queue)

	w.wg.Wait()

	return w.file.Close()
}
