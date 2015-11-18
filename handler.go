package http

import (
	"net/http"

	"github.com/gogap/spirit"
)

type HTTPRequestHandlerFunc func(
	res http.ResponseWriter,
	req *http.Request,
	deliveryChan <-chan spirit.Delivery,
	done chan<- bool,
) (deliveries []spirit.Delivery, err error)
