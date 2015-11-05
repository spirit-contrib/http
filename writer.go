package http

import (
	"io"
	gohttp "net/http"
)

type _HTTPWriter struct {
	w gohttp.ResponseWriter
}

func newHTTPWriter(w gohttp.ResponseWriter) io.WriteCloser {
	return &_HTTPWriter{w: w}
}

func (p *_HTTPWriter) Write(data []byte) (n int, err error) {
	return p.w.Write(data)
}

func (p *_HTTPWriter) Close() (err error) {
	return
}
