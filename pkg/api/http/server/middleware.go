package server

import (
	"log"
	"net/http"
)

// loggingMiddleware shims in a handler middleware that logs requests.
func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Println(r.Method, r.RequestURI, r.ContentLength)
		next.ServeHTTP(w, r)
	})
}
