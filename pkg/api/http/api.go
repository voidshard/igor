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

	"github.com/voidshard/igor/internal/utils"
	"github.com/voidshard/igor/pkg/api"
	"github.com/voidshard/igor/pkg/structs"
)

const (
	wait = 30 * time.Second
)

type Server struct {
	addr       string
	static     string
	debug      bool
	svc        api.API
	exit       chan os.Signal
	httpserver *hp.Server
}

func (s *Server) ServeForever(svc api.API) error {
	s.svc = svc

	router := mux.NewRouter()

	router.HandleFunc("/healthz", s.Health).Methods(hp.MethodGet)
	router.HandleFunc("/api/v1/jobs", s.Jobs).Methods(hp.MethodGet, hp.MethodPost)

	router.HandleFunc("/api/v1/layers", s.Layers).Methods(hp.MethodGet)
	router.HandleFunc("/api/v1/layers/pause", s.ToggleOp(structs.KindLayer, s.svc.Pause)).Methods(hp.MethodPatch)
	router.HandleFunc("/api/v1/layers/unpause", s.ToggleOp(structs.KindLayer, s.svc.Unpause)).Methods(hp.MethodPatch)
	router.HandleFunc("/api/v1/layers/skip", s.ToggleOp(structs.KindLayer, s.svc.Skip)).Methods(hp.MethodPatch)

	router.HandleFunc("/api/v1/tasks", s.Tasks).Methods(hp.MethodGet, hp.MethodPost)
	router.HandleFunc("/api/v1/tasks/pause", s.ToggleOp(structs.KindTask, s.svc.Pause)).Methods(hp.MethodPatch)
	router.HandleFunc("/api/v1/tasks/unpause", s.ToggleOp(structs.KindTask, s.svc.Unpause)).Methods(hp.MethodPatch)
	router.HandleFunc("/api/v1/tasks/skip", s.ToggleOp(structs.KindTask, s.svc.Skip)).Methods(hp.MethodPatch)
	router.HandleFunc("/api/v1/tasks/kill", s.ToggleOp(structs.KindTask, s.svc.Kill)).Methods(hp.MethodPatch)
	router.HandleFunc("/api/v1/tasks/retry", s.ToggleOp(structs.KindTask, s.svc.Retry)).Methods(hp.MethodPatch)

	if s.static != "" {
		log.Println("Serving static files from", s.static)
		router.PathPrefix("/").Handler(hp.FileServer(hp.Dir(s.static)))
	}

	if s.debug {
		log.Println("Debug enabled, adding per-request logging middleware")
		router.Use(loggingMiddleware)
	}

	s.httpserver = &hp.Server{
		Handler:      router,
		Addr:         s.addr,
		WriteTimeout: 15 * time.Second,
		ReadTimeout:  15 * time.Second,
	}

	go func() {
		log.Println("Listening on", s.httpserver.Addr)
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
	cjr := &structs.CreateJobRequest{}
	err := unmarshalJson(w, r, cjr)
	if err != nil {
		return
	}

	resp, err := s.svc.CreateJob(cjr)
	if err != nil {
		hp.Error(w, err.Error(), mapError(err))
		return
	}

	err = json.NewEncoder(w).Encode(resp)
	if err != nil {
		hp.Error(w, err.Error(), hp.StatusInternalServerError)
	}
}

func (s *Server) getJobs(w hp.ResponseWriter, r *hp.Request) {
	q := &structs.Query{}
	err := unmarshalQuery(w, r, q)
	if err != nil {
		return
	}

	items, err := s.svc.Jobs(q)
	if err != nil {
		hp.Error(w, err.Error(), mapError(err))
		return
	}
	if s.debug {
		log.Println(r.URL, "returned", len(items), "items")
	}

	err = json.NewEncoder(w).Encode(items)
	if err != nil {
		hp.Error(w, err.Error(), hp.StatusInternalServerError)
	}
}

func (s *Server) Layers(w hp.ResponseWriter, r *hp.Request) {
	// only GET is allowed, so we know what this request is
	q := &structs.Query{}
	err := unmarshalQuery(w, r, q)
	if err != nil {
		return
	}

	items, err := s.svc.Layers(q)
	if err != nil {
		hp.Error(w, err.Error(), mapError(err))
		return
	}
	if s.debug {
		log.Println(r.URL, "returned", len(items), "items")
	}

	err = json.NewEncoder(w).Encode(items)
	if err != nil {
		hp.Error(w, err.Error(), hp.StatusInternalServerError)
	}
}

func (s *Server) Tasks(w hp.ResponseWriter, r *hp.Request) {
	switch r.Method {
	case hp.MethodGet:
		s.getTasks(w, r)
	case hp.MethodPost:
		s.createTasks(w, r)
	default:
		hp.Error(w, "Method not allowed", hp.StatusMethodNotAllowed)
	}
}

func (s *Server) createTasks(w hp.ResponseWriter, r *hp.Request) {
	ctr := []*structs.CreateTaskRequest{}
	err := unmarshalJson(w, r, &ctr)
	if err != nil {
		return
	}

	for _, lr := range ctr {
		// we accept blank or existing layer IDs.
		// Blank IDs tells us to create a new job / layer automatically.
		if lr.LayerID != "" && !utils.IsValidID(lr.LayerID) {
			hp.Error(w, "bad layer id", hp.StatusBadRequest)
			return
		}
	}

	resp, err := s.svc.CreateTasks(ctr)
	if err != nil {
		hp.Error(w, err.Error(), mapError(err))
		return
	}

	err = json.NewEncoder(w).Encode(resp)
	if err != nil {
		hp.Error(w, err.Error(), hp.StatusInternalServerError)
	}
}

func (s *Server) getTasks(w hp.ResponseWriter, r *hp.Request) {
	q := &structs.Query{}
	err := unmarshalQuery(w, r, q)
	if err != nil {
		return
	}

	items, err := s.svc.Tasks(q)
	if err != nil {
		hp.Error(w, err.Error(), mapError(err))
		return
	}
	if s.debug {
		log.Println(r.URL, "returned", len(items), "items")
	}

	err = json.NewEncoder(w).Encode(items)
	if err != nil {
		hp.Error(w, err.Error(), hp.StatusInternalServerError)
	}
}

func (s *Server) ToggleOp(k structs.Kind, fn func([]*structs.ObjectRef) (int64, error)) func(hp.ResponseWriter, *hp.Request) {
	return func(w hp.ResponseWriter, r *hp.Request) {
		tog := []*structs.ObjectRef{}
		err := unmarshalJson(w, r, &tog)
		if err != nil {
			return
		}

		for _, t := range tog { // implied by URL
			t.Kind = k
		}

		updated, err := fn(tog)
		if err != nil {
			hp.Error(w, err.Error(), mapError(err))
			return
		}

		err = json.NewEncoder(w).Encode(&UpdateResponse{Updated: updated})
		if err != nil {
			hp.Error(w, err.Error(), hp.StatusInternalServerError)
		}
	}
}

func (s *Server) Close() error {
	s.exit <- os.Interrupt
	return nil
}

func (s *Server) Health(w hp.ResponseWriter, r *hp.Request) {
	json.NewEncoder(w).Encode(map[string]bool{"ok": true})
}

func NewServer(addr, static string, debug bool) *Server {
	return &Server{
		static: static,
		addr:   addr,
		debug:  debug,
		exit:   make(chan os.Signal, 1),
	}
}
