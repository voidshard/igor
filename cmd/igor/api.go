package main

import (
	"github.com/voidshard/igor/internal/utils"
	"github.com/voidshard/igor/pkg/api"
	"github.com/voidshard/igor/pkg/api/http/server"
	"github.com/voidshard/igor/pkg/database"
	"github.com/voidshard/igor/pkg/queue"
)

const (
	docApi = `Run the API server`
)

type optsAPI struct {
	optsGeneral
	optsDatabase
	optsQueue

	Addr    string `long:"addr" env:"ADDR" description:"Address to bind to" default:"localhost:8100"`
	TLSCert string `long:"cert" env:"CERT" description:"Path to TLS certificate"`
	TLSKey  string `long:"key" env:"KEY" description:"Path to TLS key"`

	StaticDir string `long:"static-dir" env:"STATIC_DIR" default:"" description:"Serve static files from this directory"`
}

func (c *optsAPI) Execute(args []string) error {
	// This main runs an API server (in this case, http) so that callers can interact with Igor over HTTP.
	// Since this is configured with OptionsClientDefault it does not run any background routines
	// that Igor needs to function (ie, to process events, queue tasks etc).
	//
	// This is intended purely to serve Igor's service API to clients over the network. Though you could
	// have one server type do both if you wanted.
	//
	// If you wished to interact with Igor via. importing the pkg libraries, then you don't need to run this.
	//
	// Alternatively, you could add more servers under pkg/api/ to serve Igor's API over other protocols like
	// gRPC, thrift or whatever you like and modifiy this to serve them all.
	tlsCfg, err := utils.TLSConfig(c.QueueTLSCaCert, c.QueueTLSCert, c.QueueTLSKey)
	if err != nil {
		panic(err)
	}
	qOpts := &queue.Options{URL: c.QueueURL, TLSConfig: tlsCfg}

	api, err := api.New(
		&database.Options{URL: c.DatabaseURL},
		qOpts,
		api.OptionsClientDefault(),
	)
	if err != nil {
		panic(err)
	}

	s := server.NewServer(c.Addr, c.StaticDir, c.TLSCert, c.TLSKey, c.Debug)
	return s.ServeForever(api)
}
