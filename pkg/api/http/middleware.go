package http

import (
	"log"
	hp "net/http"
)

func loggingMiddleware(next hp.Handler) hp.Handler {
	return hp.HandlerFunc(func(w hp.ResponseWriter, r *hp.Request) {
		log.Println(r.Method, r.RequestURI, r.ContentLength)
		next.ServeHTTP(w, r)
	})
}
