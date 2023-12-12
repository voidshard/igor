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

	//
	errs chan error
}

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

func (a *Asynq) Close() error {
	if a.srv == nil {
		return nil
	}
	a.srv.Stop()
	a.srv.Shutdown()
	close(a.errs)
	return nil
}

func (a *Asynq) Run() error {
	if a.srv == nil {
		a.buildServer()
	}
	return a.srv.Run(a.mux)
}

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

func (a *Asynq) Kill(queuedTaskID string) error {
	// Best effort cancel; asynq can't guarantee this will kill it
	return a.ins.CancelProcessing(queuedTaskID)
}

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

func (a *Asynq) Enqueue(task *structs.Task) (string, error) {
	qtask := asynq.NewTask(task.Type, []byte(task.ID), asynq.MaxRetry(int(task.Retries)))
	info, err := a.cli.Enqueue(qtask, asynq.Queue(asyncWorkQueue), asynq.Group(aggregatedTask(task.Type)))
	if err != nil {
		return "", err
	}
	return info.ID, nil
}

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

func aggregatedTask(group string) string {
	return fmt.Sprintf("aggregated%s", group)
}
