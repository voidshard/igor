package main

import (
	"os"
	"os/signal"

	"github.com/voidshard/igor/internal/utils"
	"github.com/voidshard/igor/pkg/api"
	"github.com/voidshard/igor/pkg/database"
	"github.com/voidshard/igor/pkg/queue"
)

const (
	docWorker = `Run Igor background worker`
)

type optsWorker struct {
	optsGeneral
	optsDatabase
	optsQueue
}

func (c *optsWorker) Execute(args []string) error {
	// This main runs an Igor internal server. That is, it runs some number of internal worker routines
	// to process igor events, queue tasks, push status updates or whatever else.
	//
	// This is intended to be run internal background processes, not to serve Igor's API to clients.
	// Though you could have one server type do both if you wanted.
	tlsCfg, err := utils.TLSConfig(c.QueueTLSCaCert, c.QueueTLSCert, c.QueueTLSKey)
	if err != nil {
		panic(err)
	}

	api, err := api.New(
		&database.Options{URL: c.DatabaseURL},
		&queue.Options{URL: c.QueueURL, TLSConfig: tlsCfg},
		api.OptionsServerDefault(),
	)
	if err != nil {
		panic(err)
	}

	defer api.Close()

	exit := make(chan os.Signal, 1)
	signal.Notify(exit, os.Interrupt)
	<-exit

	return nil
}
