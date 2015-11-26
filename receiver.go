package http

import (
	"errors"
	"log"
	gohttp "net/http"
	"sync"

	"github.com/go-martini/martini"
	"github.com/gogap/spirit"
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
	DisableLogger bool   `json:"disable_logger"`
}

type HTTPReceiver struct {
	conf HTTPReceiverConfig

	putter     spirit.DeliveryPutter
	translator spirit.InputTranslator

	status spirit.Status

	statusLocker sync.Mutex

	marti *martini.ClassicMartini

	requestHandler HTTPRequestHandlerFunc
}

func NewHTTPReceiver(conf HTTPReceiverConfig, requestHandler HTTPRequestHandlerFunc) (receiver *HTTPReceiver, err error) {
	if conf.Address == "" {
		conf.Address = ":8080"
	}

	receiver = &HTTPReceiver{
		conf:           conf,
		marti:          martini.Classic(),
		requestHandler: requestHandler,
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

	spirit.Logger().WithField("actor", spirit.ActorReceiver).
		WithField("urn", "urn:spirit-contrib:receiver:http[base]").
		WithField("event", "start").
		Debugln("enter start")

	if p.status == spirit.StatusRunning {
		err = ErrHTTPReceiverAlreadyStarted
		return
	}

	go p.serve()

	p.status = spirit.StatusRunning

	spirit.Logger().WithField("actor", spirit.ActorReceiver).
		WithField("urn", "urn:spirit-contrib:receiver:http[base]").
		WithField("event", "start").
		Infoln("started")

	return
}

func (p *HTTPReceiver) Handler(res gohttp.ResponseWriter, req *gohttp.Request) {
	var deliveries []spirit.Delivery
	var err error

	deliveryChan := make(chan spirit.Delivery)
	doneChan := make(chan bool)

	defer close(deliveryChan)
	defer close(doneChan)

	if deliveries, err = p.requestHandler(res, req, deliveryChan, doneChan); err != nil {
		spirit.Logger().WithField("actor", spirit.ActorReceiver).
			WithField("event", "call http request handler").
			Errorln(err)

		return
	}

	deliveriesChan.Put(deliveryChan, deliveries...)

	if err := p.putter.Put(deliveries); err != nil {
		spirit.Logger().WithField("actor", spirit.ActorReceiver).
			WithField("event", "put deliveries").
			Errorln(err)

		deliveriesChan.Delete(deliveries...)

		res.WriteHeader(gohttp.StatusInternalServerError)
	} else {

		// wait http request handler to finish process
		<-doneChan
		deliveriesChan.Delete(deliveries...)
	}
}

func (p *HTTPReceiver) Group(path string, router func(martini.Router), middlerWares ...martini.Handler) {
	p.marti.Group(path, router, middlerWares...)
}

func (p *HTTPReceiver) Stop() (err error) {
	p.statusLocker.Lock()
	defer p.statusLocker.Unlock()

	spirit.Logger().WithField("actor", spirit.ActorReceiver).
		WithField("urn", "urn:spirit-contrib:receiver:http[base]").
		WithField("event", "stop").
		Debugln("enter stop")

	if p.status == spirit.StatusStopped {
		return
	}

	spirit.Logger().WithField("actor", spirit.ActorReceiver).
		WithField("urn", "urn:spirit-contrib:receiver:http[base]").
		WithField("event", "stop").
		Infoln("stopped")

	return
}

func (p *HTTPReceiver) Status() spirit.Status {
	return p.status
}

func (p *HTTPReceiver) serve() {
	var logger *log.Logger
	if p.conf.DisableLogger {
		logger = log.New(new(emptyWriter), "", 0)
	} else {
		logger = log.New(spirit.Logger().Writer(), "[http]", 0)
	}

	p.marti.Map(logger)

	p.marti.RunOnAddr(p.conf.Address)
}
