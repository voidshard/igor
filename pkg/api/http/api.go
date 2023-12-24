// http API module serves Igor's API over HTTP.
package http

import (
	"context"
	"encoding/json"
	"log"
	hp "net/http"
	"os"
	"os/signal"
	"time"

	"github.com/gorilla/mux"

	"github.com/voidshard/igor/pkg/api"
	"github.com/voidshard/igor/pkg/structs"
)

const (
	wait = 30 * time.Second
)

type Server struct {
	svc        api.API
	exit       chan os.Signal
	httpserver *hp.Server
}

func (s *Server) ServeForever(svc api.API) error {
	s.svc = svc

	go func() {
		if err := s.httpserver.ListenAndServe(); err != nil {
			log.Println(err)
		}
	}()

	signal.Notify(s.exit, os.Interrupt)
	defer s.Close()
	<-s.exit

	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	s.httpserver.Shutdown(ctx)
	os.Exit(0)
	return nil
}

func (s *Server) Jobs(w hp.ResponseWriter, r *hp.Request) {
	switch r.Method {
	case hp.MethodGet:
		s.getJobs(w, r)
	case hp.MethodPost:
		s.createJob(w, r)
	default:
		hp.Error(w, "Method not allowed", hp.StatusMethodNotAllowed)
	}
}

func (s *Server) createJob(w hp.ResponseWriter, r *hp.Request) {
}

func (s *Server) getJobs(w hp.ResponseWriter, r *hp.Request) {
	q := &structs.Query{}
	err := unmarshalJson(w, r, q)
	if err != nil {
		return
	}

	jobs, err := s.svc.Jobs(q)
	if err != nil {
		hp.Error(w, err.Error(), mapError(err))
		return
	}

	json.NewEncoder(w).Encode(jobs)
}

func (s *Server) Close() error {
	s.exit <- os.Interrupt
	return nil
}

func (s *Server) Health(w hp.ResponseWriter, r *hp.Request) {
	json.NewEncoder(w).Encode(map[string]bool{"ok": true})
}

func NewServer(addr string) *Server {
	router := mux.NewRouter()

	me := &Server{
		exit: make(chan os.Signal, 1),
		httpserver: &hp.Server{
			Handler:      router,
			Addr:         addr,
			WriteTimeout: 15 * time.Second,
			ReadTimeout:  15 * time.Second,
		},
	}

	router.HandleFunc("/healthz", me.Health).Methods(hp.MethodGet)
	router.HandleFunc("/api/v1/jobs", me.Jobs).Methods(hp.MethodGet, hp.MethodPost)

	return me
}
