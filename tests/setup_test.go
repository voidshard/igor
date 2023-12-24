package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/voidshard/igor/pkg/api"
	"github.com/voidshard/igor/pkg/api/http/client"
	"github.com/voidshard/igor/pkg/database"
	"github.com/voidshard/igor/pkg/queue"
)

var (
	setup = &Setup{
		testDataDir: os.Getenv("IGOR_TEST_DATA"),
		apiURL:      os.Getenv("IGOR_TEST_API"),
	}
)

type Setup struct {
	db  database.Database
	que queue.Queue
	svc api.API

	client *client.Client

	testDataDir string
	apiURL      string
}

func (s *Setup) loadTestData(filename string, obj interface{}) error {
	fpath := filepath.Join(s.testDataDir, filename)
	fmt.Println("Loading test data from", fpath)
	f, err := os.Open(fpath)
	if err != nil {
		return err
	}
	defer f.Close()
	defer fmt.Println("Loaded test data", obj)
	return json.NewDecoder(f).Decode(obj)
}

func init() {
	// set in run.sh script for test harness
	pgURL := os.Getenv("IGOR_TEST_PG_URL")
	rdURL := os.Getenv("IGOR_TEST_RD_URL")
	fmt.Println("Test Postgres Location:", pgURL)
	fmt.Println("Test Redis Location:", rdURL)

	svc, err := api.New(
		&database.Options{URL: pgURL},
		&queue.Options{URL: rdURL},
		&api.Options{
			EventRoutines:       2,
			TidyRoutines:        2,
			TidyLayerFrequency:  1 * time.Minute,
			TidyTaskFrequency:   1 * time.Minute,
			TidyUpdateThreshold: 1 * time.Minute,
			MaxTaskRuntime:      1 * time.Minute,
		})
	if err != nil {
		panic(err)
	}
	setup.svc = svc
	fmt.Println("Test API Location:", setup.apiURL)
	fmt.Println("Test Data Dir:", setup.testDataDir)

	setup.client, err = client.New(setup.apiURL)
	if err != nil {
		panic(err)
	}
}
