package http

import (
	"sync"

	"github.com/gogap/spirit"
)

type _DeliveryChans map[string]chan spirit.Delivery

var (
	deliveriesChan _DeliveryChans = make(map[string]chan spirit.Delivery)

	deliveriesChanLocker sync.Mutex
)

func (p _DeliveryChans) Put(deChan chan spirit.Delivery, deliveries ...spirit.Delivery) {
	deliveriesChanLocker.Lock()
	defer deliveriesChanLocker.Unlock()

	if deliveries != nil {
		for _, de := range deliveries {
			p[de.Id()] = deChan
		}
	}
}

func (p _DeliveryChans) Delete(deliveries ...spirit.Delivery) {
	deliveriesChanLocker.Lock()
	defer deliveriesChanLocker.Unlock()

	if deliveries != nil {
		for _, de := range deliveries {
			delete(p, de.Id())
		}
	}
}
