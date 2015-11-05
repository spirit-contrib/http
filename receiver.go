package http

import (
	"errors"
	"log"
	gohttp "net/http"
	"sync"

	"github.com/go-martini/martini"
	"github.com/gogap/spirit"
)

const (
	httpReceiverURN = "urn:spirit-contrib:receiver:http"
)

var (
	ErrHTTPReceiverAlreadyStarted = errors.New("http receiver already started")
)

var _ spirit.Receiver = new(HTTPReceiver)

type emptyWriter struct {
}

func (p *emptyWriter) Write(v []byte) (n int, err error) {
	return
}

type HTTPReceiverConfig struct {
	Address       string `json:"address"`
	Path          string `json:"path"`
	ContentType   string `json:"content_type"`
	DisableLogger bool   `json:"disable_logger"`
}

type HTTPReceiver struct {
	conf HTTPReceiverConfig

	putter     spirit.DeliveryPutter
	translator spirit.InputTranslator

	status spirit.Status

	statusLocker sync.Mutex
}

func init() {
	spirit.RegisterReceiver(httpReceiverURN, NewHTTPReceiver)
}

func NewHTTPReceiver(options spirit.Options) (receiver spirit.Receiver, err error) {
	conf := HTTPReceiverConfig{}
	if err = options.ToObject(&conf); err != nil {
		return
	}

	if conf.Address == "" {
		conf.Address = ":8080"
	}

	if conf.Path == "" {
		conf.Path = "/"
	}

	if conf.ContentType == "" {
		conf.ContentType = "text/plain"
	}

	receiver = &HTTPReceiver{
		conf: conf,
	}

	return
}

func (p *HTTPReceiver) SetDeliveryPutter(putter spirit.DeliveryPutter) (err error) {
	p.putter = putter
	return
}

func (p *HTTPReceiver) Start() (err error) {
	p.statusLocker.Lock()
	defer p.statusLocker.Unlock()

	if p.status == spirit.StatusRunning {
		err = ErrHTTPReceiverAlreadyStarted
		return
	}

	go p.serve()

	return
}

func (p *HTTPReceiver) Handler(res gohttp.ResponseWriter, req *gohttp.Request) {
	var deliveries []spirit.Delivery
	var err error

	if deliveries, err = p.translator.In(req.Body); err != nil {
		spirit.Logger().WithField("actor", "writer pool").
			WithField("urn", httpReceiverURN).
			WithField("event", "translator request body").
			Errorln(err)

		res.WriteHeader(gohttp.StatusBadRequest)
		return
	}

	writer := newHTTPWriter(res)

	writerLocker.Lock()
	waitSignal := make(chan bool, 1)
	for _, delivery := range deliveries {
		httpWriters[delivery.Id()] = &writerInPool{false, writer, 0, waitSignal}
	}
	writerLocker.Unlock()

	res.Header().Add("Content-Type", p.conf.ContentType)

	if err := p.putter.Put(deliveries); err != nil {
		spirit.Logger().WithField("actor", "writer pool").
			WithField("urn", httpReceiverURN).
			WithField("event", "translator request body").
			Errorln(err)

		for _, delivery := range deliveries {
			delete(httpWriters, delivery.Id())
		}

		res.WriteHeader(gohttp.StatusInternalServerError)
	}

	<-waitSignal

}

func (p *HTTPReceiver) serve() {
	m := martini.Classic()
	m.Post(p.conf.Path, p.Handler)

	var logger *log.Logger
	if p.conf.DisableLogger {
		logger = log.New(new(emptyWriter), "", 0)
	} else {
		logger = log.New(spirit.Logger().Writer(), "["+httpReceiverURN+"] ", 0)
	}

	m.Map(logger)

	m.RunOnAddr(p.conf.Address)
}

func (p *HTTPReceiver) SetTranslator(translator spirit.InputTranslator) (err error) {
	p.translator = translator
	return
}

func (p *HTTPReceiver) Stop() (err error) {
	p.statusLocker.Lock()
	defer p.statusLocker.Unlock()

	if p.status == spirit.StatusStopped {
		return
	}

	return
}

func (p *HTTPReceiver) Status() spirit.Status {
	return p.status
}
