package queue

import (
	"bytes"
	"context"
	"fmt"
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
	asyncTaskRune    = "|"
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
	return nil
}

func (a *Asynq) Register(task string, handler func(work []*Meta)) error {
	if a.mux == nil {
		a.buildServer()
	}
	a.mux.HandleFunc(aggregatedTask(task), func(ctx context.Context, t *asynq.Task) error {
		meta, err := a.deaggregateTasks(t)
		if err != nil {
			return err
		}
		handler(meta)
		return a.batchUpdates(meta)
	})
	return nil
}

func (a *Asynq) Kill(queuedTaskID string) error {
	// Best effort cancel; asynq can't guarantee this will kill it
	return a.ins.CancelProcessing(queuedTaskID)
}

func (a *Asynq) batchUpdates(meta []*Meta) error {
	// work out all the updates we need to do
	taskUpdates := map[string][]*structs.Task{}
	runUpdates := map[string][]*structs.Run{}
	for _, m := range meta {
		// determine batches of task updates
		tup := string(structs.COMPLETED)
		if m.skip { // skip trumps all
			tup = string(structs.SKIPPED)
		} else if m.err != nil {
			tup = string(structs.ERRORED)
		}

		// run has the same status, but we have messages to deal with too
		rup := fmt.Sprintf("%s:%s", tup, m.msg)

		// record stuff we can update together
		tupdates, ok := taskUpdates[tup]
		if !ok {
			tupdates = []*structs.Task{}
		}
		taskUpdates[tup] = append(tupdates, m.Task)

		rupdates, ok := runUpdates[rup]
		if !ok {
			rupdates = []*structs.Run{}
		}
		runUpdates[rup] = append(rupdates, m.Run)
	}

	// we'll try to apply all updates & return any errors we get
	errs := []error{}
	for _, tasks := range taskUpdates {
		err := a.svc.SetTasksState(tasks, structs.Status(tasks[0].Status))
		errs = append(errs, err)
	}
	for _, runs := range runUpdates {
		err := a.svc.SetRunsState(runs, structs.Status(runs[0].Status), runs[0].Message)
		errs = append(errs, err)
	}

	//TODO there probably is a lib to do this
	if len(errs) == 0 {
		return nil
	} else if len(errs) == 1 {
		return errs[0]
	}
	final := fmt.Errorf("multiple errors: ")
	for _, err := range errs {
		final = fmt.Errorf("%w\n%v", final, err)
	}
	return final
}

func (a *Asynq) deaggregateTasks(t *asynq.Task) ([]*Meta, error) {
	// parse into ID pairs
	taskIDs := []string{}
	runIDs := []string{}
	for _, load := range bytes.Split(t.Payload(), []byte(asyncTaskRune)) {
		load = bytes.TrimSpace(load)
		if len(load) == 0 {
			continue
		}
		parts := bytes.Split(load, []byte(asyncAggRune))
		if len(parts) != 2 {
			continue
		}
		taskIDs = append(taskIDs, string(parts[0]))
		runIDs = append(runIDs, string(parts[1]))
	}

	tasks, err := a.svc.Tasks(taskIDs)
	if err != nil {
		return nil, err
	}
	runs, err := a.svc.Runs(runIDs)
	if err != nil {
		return nil, err
	}

	taskByID := map[string]*structs.Task{}
	for _, t := range tasks {
		taskByID[t.ID] = t
	}

	ms := []*Meta{}
	for _, r := range runs {
		task, ok := taskByID[r.TaskID]
		if ok {
			ms = append(ms, &Meta{Task: task, Run: r})
		}
	}

	return ms, nil
}

func (a *Asynq) Enqueue(task *structs.Task, run *structs.Run) (string, error) {
	args := bytes.Join([][]byte{[]byte(task.ID), []byte(run.ID)}, []byte(asyncTaskRune))
	qtask := asynq.NewTask(task.Type, args)
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
