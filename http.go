package http

import (
	"io"
	"sync"
)

type writerInPool struct {
	InUse      bool
	Writer     io.WriteCloser
	RefCount   int64
	WaitSignal chan bool
}

var (
	writerLocker sync.Mutex
	httpWriters  map[string]*writerInPool = make(map[string]*writerInPool)
)
