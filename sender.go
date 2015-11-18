package http

import (
	"errors"
	"sync"
	"time"

	"github.com/gogap/spirit"
)

const (
	senderURN = "urn:spirit-contrib:sender:http"
)

var (
	ErrHTTPSenderAlreadyStarted = errors.New("http sender already started")
)

var (
	_ spirit.Sender = new(HTTPSender)
)

type HTTPSenderConfig struct {
	Timeout int `json:"timeout"`
}

type HTTPSender struct {
	status       spirit.Status
	statusLocker sync.Mutex

	getter spirit.DeliveryGetter

	conf HTTPSenderConfig
}

func init() {
	spirit.RegisterSender(senderURN, NewHTTPSender)
}

func NewHTTPSender(config spirit.Config) (sender spirit.Sender, err error) {
	conf := HTTPSenderConfig{}

	if err = config.ToObject(&conf); err != nil {
		return
	}

	sender = &HTTPSender{
		conf: conf,
	}

	return
}

func (p *HTTPSender) SetDeliveryGetter(getter spirit.DeliveryGetter) (err error) {
	p.getter = getter
	return
}

func (p *HTTPSender) send() {
	for {
		if deliveries, err := p.getter.Get(); err != nil {
			return
		} else {
			for _, delivery := range deliveries {
				deliveriesChanLocker.Lock()
				if deChan, exist := deliveriesChan[delivery.Id()]; exist {
					go func(deChan chan spirit.Delivery, delivery spirit.Delivery) {
						select {
						case deChan <- delivery:
							{
							}
						case <-time.After(time.Duration(p.conf.Timeout) * time.Millisecond):
							{
							}
						}
					}(deChan, delivery)
				}
				deliveriesChanLocker.Unlock()
			}
		}
	}
}

func (p *HTTPSender) Start() (err error) {
	p.statusLocker.Lock()
	defer p.statusLocker.Unlock()

	if p.status == spirit.StatusRunning {
		err = ErrHTTPSenderAlreadyStarted
		return
	}

	go p.send()

	return
}

func (p *HTTPSender) Stop() (err error) {
	p.statusLocker.Lock()
	defer p.statusLocker.Unlock()

	if p.status == spirit.StatusStopped {
		return
	}

	return
}

func (p *HTTPSender) Status() spirit.Status {
	return p.status
}
