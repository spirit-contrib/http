package http

import (
	"errors"
	"io"
	"sync"
	"sync/atomic"

	"github.com/gogap/spirit"
)

const (
	writerPoolURN = "urn:spirit-contrib:io:pool:writer:http"
)

var (
	ErrNoRelatedResponseWriter = errors.New("no related http response writer")
)

var _ spirit.WriterPool = new(HTTPWriterPool)

type HTTPWriterPoolConfig struct {
}

type HTTPWriterPool struct {
	statusLocker sync.Mutex
	conf         HTTPWriterPoolConfig

	isClosed bool
}

func init() {
	spirit.RegisterWriterPool(writerPoolURN, NewHTTPWriterPool)
}

func NewHTTPWriterPool(config spirit.Config) (pool spirit.WriterPool, err error) {
	conf := HTTPWriterPoolConfig{}
	if err = config.ToObject(&conf); err != nil {
		return
	}

	pool = &HTTPWriterPool{
		conf: conf,
	}

	return
}

func (p *HTTPWriterPool) SetNewWriterFunc(newFunc spirit.NewWriterFunc, config spirit.Config) (err error) {
	return
}

func (p *HTTPWriterPool) Get(delivery spirit.Delivery) (writer io.WriteCloser, err error) {
	if p.isClosed == true {
		err = spirit.ErrWriterPoolAlreadyClosed
		return
	}

	writerLocker.Lock()
	defer writerLocker.Unlock()

	if httpWriter, exist := httpWriters[delivery.Id()]; exist {
		if httpWriter.InUse {
			err = spirit.ErrWriterInUse
			return
		} else {
			writer = httpWriter.Writer
			httpWriter.InUse = true
			newV := atomic.AddInt64(&httpWriter.RefCount, 1)

			spirit.Logger().WithField("actor", "writer pool").
				WithField("urn", writerPoolURN).
				WithField("event", "get writer").
				WithField("delivery_id", delivery.Id()).
				WithField("ref", newV).
				Debugln("get an exist http writer")

			return
		}
	} else {

		err = ErrNoRelatedResponseWriter

		spirit.Logger().WithField("actor", "writer pool").
			WithField("urn", writerPoolURN).
			WithField("event", "get writer").
			WithField("delivery_id", delivery.Id()).
			Errorln(err)

		return
	}
}

func (p *HTTPWriterPool) Put(delivery spirit.Delivery, writer io.WriteCloser) (err error) {
	if p.isClosed == true {
		err = spirit.ErrWriterPoolAlreadyClosed
		return
	}

	writerLocker.Lock()
	defer writerLocker.Unlock()

	if httpWriter, exist := httpWriters[delivery.Id()]; exist {

		if httpWriter.Writer != writer {
			spirit.Logger().WithField("actor", "writer pool").
				WithField("urn", writerPoolURN).
				WithField("event", "put writer").
				WithField("http_id", delivery.Id()).
				Debugln("put http writer")
			httpWriter.Writer = writer
		}

		newV := atomic.AddInt64(&httpWriter.RefCount, -1)

		spirit.Logger().WithField("actor", "writer pool").
			WithField("urn", writerPoolURN).
			WithField("event", "put writer").
			WithField("delivery_id", delivery.Id()).
			WithField("ref", newV).
			Debugln("put http writer back")

		if newV == 0 {
			httpWriter.WaitSignal <- true
			delete(httpWriters, delivery.Id())
			return
		}

	} else {
		spirit.Logger().WithField("actor", "writer pool").
			WithField("urn", writerPoolURN).
			WithField("event", "put writer").
			WithField("http_id", delivery.Id()).
			Debugln("unknown writer")
	}

	return
}

func (p *HTTPWriterPool) Close() {
	writerLocker.Lock()
	defer writerLocker.Unlock()

	p.statusLocker.Lock()
	defer p.statusLocker.Unlock()

	p.isClosed = true

	return
}
