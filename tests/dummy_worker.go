package main

import (
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/voidshard/igor/pkg/api"
	"github.com/voidshard/igor/pkg/database"
	"github.com/voidshard/igor/pkg/queue"
)

const (
	taskSleep = "sleep"
	taskError = "die"
)

func main() {
	pgURL := os.Getenv("DATABASE_URL")
	rdURL := os.Getenv("QUEUE_URL")

	svc, err := api.New(
		&database.Options{URL: pgURL},
		&queue.Options{URL: rdURL},
		api.OptionsClientDefault(),
	)
	if err != nil {
		panic(err)
	}

	myErr := fmt.Errorf("boom")

	svc.Register(taskSleep, func(work []*queue.Meta) error {
		for _, w := range work {
			w.SetRunning("hi")
			d := time.Duration(rand.Intn(5)+1) * time.Second
			fmt.Printf("Sleeping for %s (%d 'tasks')\n", d, len(work))
			time.Sleep(d)
			fmt.Printf("Done sleeping\n")
		}
		return nil
	})
	svc.Register(taskError, func(work []*queue.Meta) error {
		for _, w := range work {
			fmt.Printf("Erroring %s\n", w.Task.ID)
			w.SetError(myErr)
		}
		return nil
	})

	svc.Run()
}
