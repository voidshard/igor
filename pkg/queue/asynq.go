package queue

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/hibiken/asynq"

	"github.com/voidshard/igor/pkg/database"
	"github.com/voidshard/igor/pkg/structs"
)

const (
	asyncWorkQueue   = "igor:work"
	asyncAggMaxSize  = 1000
	asyncAggMaxDelay = 2 * time.Second
	asyncAggRune     = "¬"
)

// Asynq is a Queue implementation that uses asynq.
// See github.com/hibiken/asynq
type Asynq struct {
	opts *Options

	// the asynq client & inspector
	ins *asynq.Inspector
	cli *asynq.Client

	// the funcs we're allowed to call inside of igor
	svc database.QueueDB

	// if register is called we're intended to start a server
	lock sync.Mutex
	mux  *asynq.ServeMux
	srv  *asynq.Server

	// errs is a channel of errors that we'll send to
	// Currenty we just log these, probably we should support some output / callback.
	errs chan error
}

// NewAsynqQueue returns a new Asynq queue with the given settings
func NewAsynqQueue(svc database.QueueDB, opts *Options) (*Asynq, error) {
	ins := asynq.NewInspector(asynq.RedisClientOpt{Addr: opts.URL})
	cli := asynq.NewClient(asynq.RedisClientOpt{Addr: opts.URL})
	return &Asynq{
		opts: opts,
		ins:  ins,
		cli:  cli,
		svc:  svc,
	}, nil
}

// Close shuts down the queue
func (a *Asynq) Close() error {
	if a.srv == nil {
		return nil
	}
	a.srv.Stop()
	a.srv.Shutdown()
	close(a.errs)
	return nil
}

// Run starts the queue, this is only required for workers (ie. handlers)
func (a *Asynq) Run() error {
	if a.srv == nil {
		a.buildServer()
	}
	return a.srv.Run(a.mux)
}

// Register a handler for a task type, this allows us to process tasks (indicated by their Type).
//
// Note that we pass here a []*Meta, this is a list of Task wrappers.
// A user may optionally call SetError(), SetComplete() or SetRunning() per Meta (task) in the list
// to have this value reflected in the database.
//
// If a user does *not* call one of these, Igor implicitly calls SetComplete() for them at the conclusion
// of the handler.
//
// Note that returning an error from this handler does not imply SetError() is called, as it is assumed
// that already completed work is still valid.
func (a *Asynq) Register(task string, handler func(work []*Meta) error) error {
	if a.mux == nil {
		a.buildServer()
	}
	a.mux.HandleFunc(aggregatedTask(task), func(ctx context.Context, t *asynq.Task) error {
		meta, err := a.deaggregateTasks(t)
		if err != nil {
			return err
		}
		err = handler(meta)
		if err != nil {
			return err
		}
		for _, m := range meta {
			if m.Task.Status == structs.QUEUED || m.Task.Status == structs.RUNNING {
				m.SetComplete()
			}
		}
		return nil
	})
	return nil
}

// Kill calls the our underlying queue to cancel a task by it's ID (given to us when we Enqueue() it).
func (a *Asynq) Kill(queuedTaskID string) error {
	// Best effort cancel; asynq can't guarantee this will kill it
	return a.ins.CancelProcessing(queuedTaskID)
}

// deaggregateTasks takes a task that has been aggregated and returns a list of tasks.
func (a *Asynq) deaggregateTasks(t *asynq.Task) ([]*Meta, error) {
	// parse into IDs (the payloads)
	taskIDs := []string{}
	for _, load := range bytes.Split(t.Payload(), []byte(asyncAggRune)) {
		id := bytes.TrimSpace(load)
		if len(load) == 0 {
			continue
		}
		taskIDs = append(taskIDs, string(id))
	}

	// read out the tasks
	tasks, err := a.svc.Tasks(taskIDs)
	if err != nil {
		return nil, err
	}

	// build in "metas" (sugar for callers)
	ms := []*Meta{}
	for _, t := range tasks {
		ms = append(ms, &Meta{Task: t, svc: a.svc, errs: a.errs})
	}

	return ms, nil
}

// Enqueue a task to be processed by a worker.
func (a *Asynq) Enqueue(task *structs.Task) (string, error) {
	qtask := asynq.NewTask(task.Type, []byte(task.ID), asynq.MaxRetry(int(task.Retries)))
	info, err := a.cli.Enqueue(qtask, asynq.Queue(asyncWorkQueue), asynq.Group(aggregatedTask(task.Type)))
	if err != nil {
		return "", err
	}
	return info.ID, nil
}

// buildServer creates a new server & mux for us to use.
//
// We set this up with a simple aggregator that just concatenates task IDs together.
// We only need to pass the task ID, since Igor stores the rest of the data in the database (which we can
// look up by ID in batches pretty efficiently).
func (a *Asynq) buildServer() {
	a.lock.Lock()
	defer a.lock.Unlock()
	if a.mux != nil {
		// someone locked and set this first
		return
	}
	srv := asynq.NewServer(
		asynq.RedisClientOpt{Addr: a.opts.URL},
		asynq.Config{
			Queues:          map[string]int{asyncWorkQueue: 1},
			GroupAggregator: asynq.GroupAggregatorFunc(aggregate),
			GroupMaxSize:    asyncAggMaxSize,
			GroupMaxDelay:   asyncAggMaxDelay,
		},
	)
	mux := asynq.NewServeMux()
	a.srv = srv
	a.mux = mux
	a.errs = make(chan error)
	go func() {
		for err := range a.errs {
			if err != nil {
				log.Println("asynq error:", err)
			}
		}
	}()
}

// aggregate takes multiple tasks from Asynq and aggregates them into a single task.
//
// We do this by concatenating the task IDs together, separated by a rune.
// This is what deaggregateTasks() does in reverse.
func aggregate(group string, tasks []*asynq.Task) *asynq.Task {
	var b strings.Builder
	for _, t := range tasks {
		if t == nil || t.Payload() == nil {
			continue
		}
		b.Write(t.Payload())
		b.WriteString(asyncAggRune)
	}
	return asynq.NewTask(group, []byte(b.String()))
}

// aggregatedTask returns the name of the aggregated task for a given group.
//
// Internally we set this in Asynq as the handler for a group so we can do automatic aggregation.
func aggregatedTask(group string) string {
	return fmt.Sprintf("aggregated%s", group)
}
