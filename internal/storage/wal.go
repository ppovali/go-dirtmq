package storage

import (
	"os"
	"sync"
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

	_, err := w.file.Write(data)
	if err != nil {
		return err
	}

	err = w.file.Sync()
	if err != nil {
		return err
	}

	return nil
}

func (w *WAL) Close() error {
	err := w.file.Close()
	if err != nil {
		return err
	}
	return nil
}
