package server

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/gorilla/mux"

	"github.com/voidshard/igor/internal/utils"
	"github.com/voidshard/igor/pkg/api"
	"github.com/voidshard/igor/pkg/api/http/common"
	"github.com/voidshard/igor/pkg/structs"
)

const (
	wait = 30 * time.Second
)

// Server is the http server for igor
type Server struct {
	addr       string
	static     string
	cert       string
	key        string
	debug      bool
	svc        api.API
	exit       chan os.Signal
	httpserver *http.Server
}

// ServeForever starts the server and blocks until it's closed
func (s *Server) ServeForever(svc api.API) error {
	s.svc = svc

	router := mux.NewRouter()
	router.HandleFunc("/healthz", s.Health).Methods(http.MethodGet)
	router.HandleFunc(common.API_JOBS, s.Jobs).Methods(http.MethodGet, http.MethodPost)
	router.HandleFunc(common.API_LAYERS, s.Layers).Methods(http.MethodGet)
	router.HandleFunc(common.API_TASKS, s.Tasks).Methods(http.MethodGet, http.MethodPost)
	router.HandleFunc(common.API_PAUSE, s.ToggleOp(s.svc.Retry)).Methods(http.MethodPatch)
	router.HandleFunc(common.API_UNPAUSE, s.ToggleOp(s.svc.Unpause)).Methods(http.MethodPatch)
	router.HandleFunc(common.API_SKIP, s.ToggleOp(s.svc.Skip)).Methods(http.MethodPatch)
	router.HandleFunc(common.API_KILL, s.ToggleOp(s.svc.Kill)).Methods(http.MethodPatch)
	router.HandleFunc(common.API_RETRY, s.ToggleOp(s.svc.Retry)).Methods(http.MethodPatch)

	if s.static != "" {
		log.Println("Serving static files from", s.static)
		router.PathPrefix("/").Handler(http.FileServer(http.Dir(s.static)))
	}

	if s.debug {
		log.Println("Debug enabled, adding per-request logging middleware")
		router.Use(loggingMiddleware)
	}

	s.httpserver = &http.Server{
		Handler:      router,
		Addr:         s.addr,
		WriteTimeout: 15 * time.Second,
		ReadTimeout:  15 * time.Second,
	}

	go func() {
		log.Println("Listening on", s.httpserver.Addr)
		var err error
		if s.cert != "" && s.key != "" {
			log.Println("TLS enabled (", s.cert, s.key, ")")
			err = s.httpserver.ListenAndServeTLS(s.cert, s.key)
		} else {
			err = s.httpserver.ListenAndServe()
		}
		log.Println(err)
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

// Jobs handles requests for the Jobs API endpoint
func (s *Server) Jobs(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.getJobs(w, r)
	case http.MethodPost:
		s.createJob(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// createJob handles requests to create a new job
func (s *Server) createJob(w http.ResponseWriter, r *http.Request) {
	cjr := &structs.CreateJobRequest{}
	err := unmarshalJson(w, r, cjr)
	if err != nil {
		return
	}

	resp, err := s.svc.CreateJob(cjr)
	if err != nil {
		http.Error(w, err.Error(), mapError(err))
		return
	}

	err = json.NewEncoder(w).Encode(resp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// getJobs handles requests to get jobs
func (s *Server) getJobs(w http.ResponseWriter, r *http.Request) {
	q := &structs.Query{}
	err := unmarshalQuery(w, r, q)
	if err != nil {
		return
	}

	items, err := s.svc.Jobs(q)
	if err != nil {
		http.Error(w, err.Error(), mapError(err))
		return
	}
	if s.debug {
		log.Println(r.URL, "returned", len(items), "items")
	}

	err = json.NewEncoder(w).Encode(items)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// Layers handles requests for the Layers API endpoint
func (s *Server) Layers(w http.ResponseWriter, r *http.Request) {
	// only GET is allowed, so we know what this request is
	q := &structs.Query{}
	err := unmarshalQuery(w, r, q)
	if err != nil {
		return
	}

	items, err := s.svc.Layers(q)
	if err != nil {
		http.Error(w, err.Error(), mapError(err))
		return
	}
	if s.debug {
		log.Println(r.URL, "returned", len(items), "items")
	}

	err = json.NewEncoder(w).Encode(items)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// Tasks handles requests for the Tasks API endpoint
func (s *Server) Tasks(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.getTasks(w, r)
	case http.MethodPost:
		s.createTasks(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// createTasks handles requests to create new task(s)
func (s *Server) createTasks(w http.ResponseWriter, r *http.Request) {
	ctr := []*structs.CreateTaskRequest{}
	err := unmarshalJson(w, r, &ctr)
	if err != nil {
		return
	}

	for _, lr := range ctr {
		// we accept blank or existing layer IDs.
		// Blank IDs tells us to create a new job / layer automatically.
		if lr.LayerID != "" && !utils.IsValidID(lr.LayerID) {
			http.Error(w, "bad layer id", http.StatusBadRequest)
			return
		}
	}

	resp, err := s.svc.CreateTasks(ctr)
	if err != nil {
		http.Error(w, err.Error(), mapError(err))
		return
	}

	err = json.NewEncoder(w).Encode(resp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// getTasks handles requests to get tasks
func (s *Server) getTasks(w http.ResponseWriter, r *http.Request) {
	q := &structs.Query{}
	err := unmarshalQuery(w, r, q)
	if err != nil {
		return
	}

	items, err := s.svc.Tasks(q)
	if err != nil {
		http.Error(w, err.Error(), mapError(err))
		return
	}
	if s.debug {
		log.Println(r.URL, "returned", len(items), "items")
	}

	err = json.NewEncoder(w).Encode(items)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// ToggleOp is a helper to create a handler for PATCH requests that call some simple operation.
// Ie. pause, unpause, skip, kill, retry
func (s *Server) ToggleOp(fn func([]*structs.ObjectRef) (int64, error)) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		tog := []*structs.ObjectRef{}
		err := unmarshalJson(w, r, &tog)
		if err != nil {
			return
		}

		updated, err := fn(tog)
		if err != nil {
			http.Error(w, err.Error(), mapError(err))
			return
		}

		err = json.NewEncoder(w).Encode(&common.UpdateResponse{Updated: updated})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}

// Close shuts down the server
func (s *Server) Close() error {
	s.exit <- os.Interrupt
	return nil
}

// Health is a simple health check endpoint
func (s *Server) Health(w http.ResponseWriter, r *http.Request) {
	// TODO: improve with actual stats or something
	json.NewEncoder(w).Encode(map[string]bool{"ok": true})
}

// NewServer creates a new server
func NewServer(addr, static, certFile, keyFile string, debug bool) *Server {
	return &Server{
		static: static,
		cert:   certFile,
		key:    keyFile,
		addr:   addr,
		debug:  debug,
		exit:   make(chan os.Signal, 1),
	}
}
