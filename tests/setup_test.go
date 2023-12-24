package main

import (
	"fmt"
	"os"
	"time"

	"github.com/voidshard/igor/pkg/core"
	"github.com/voidshard/igor/pkg/database"
	"github.com/voidshard/igor/pkg/queue"
)

var (
	setup = &Setup{}
)

type Setup struct {
	db  database.Database
	qdb *core.QueueDB
	que queue.Queue
	svc *core.Service
}

func init() {
	// set in run.sh script for test harness
	pgURL := os.Getenv("IGOR_TEST_PG_URL")
	rdURL := os.Getenv("IGOR_TEST_RD_URL")
	fmt.Println("Test Postgres Location:", pgURL)
	fmt.Println("Test Redis Location:", rdURL)

	// Connect to all the things
	dbconn, err := database.NewPostgres(&database.Options{URL: pgURL})
	if err != nil {
		panic(err)
	}
	setup.db = dbconn

	setup.qdb = core.NewQueueDB(setup.db)

	setup.que, err = queue.NewAsynqQueue(setup.qdb, &queue.Options{URL: rdURL})
	if err != nil {
		panic(err)
	}

	svc, err := core.NewService(setup.db, setup.que, &core.Options{
		EventRoutines:     2,
		TidyRoutines:      2,
		TidyJobFrequency:  1 * time.Minute,
		TidyTaskFrequency: 1 * time.Minute,
		MaxTaskRuntime:    1 * time.Minute,
	})
	if err != nil {
		panic(err)
	}
	setup.svc = svc
}
