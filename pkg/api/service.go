package api

import (
	"fmt"
	"log"
	"time"

	"github.com/voidshard/igor/internal/utils"
	"github.com/voidshard/igor/pkg/database"
	"github.com/voidshard/igor/pkg/database/changes"
	"github.com/voidshard/igor/pkg/errors"
	"github.com/voidshard/igor/pkg/queue"
	"github.com/voidshard/igor/pkg/structs"
)

const (
	// max values
	maxNameLength = 500
	maxTypeLength = 500
	maxArgsLength = 10000
)

var (
	incompleteStates = []structs.Status{
		structs.PENDING,
		structs.QUEUED,
		structs.RUNNING,
	}

	defLimit = 2000
)

type Service struct {
	db   database.Database
	qu   queue.Queue
	opts *Options
	errs chan error
}

func New(dbopts *database.Options, queopts *queue.Options, opts *Options) (*Service, error) {
	db, err := database.NewPostgres(dbopts)
	if err != nil {
		return nil, err
	}
	qu, err := queue.NewAsynqQueue(database.NewQueueDB(db), queopts)
	if err != nil {
		return nil, err
	}
	return newService(db, qu, opts)
}

func newService(db database.Database, qu queue.Queue, opts *Options) (*Service, error) {
	me := &Service{db: db, qu: qu, opts: opts, errs: make(chan error)}

	go func() {
		for err := range me.errs {
			if err != nil {
				log.Println("[Service]", err)
			}
		}
	}()

	if opts.TidyRoutines > 0 {
		me.startTidyRoutines()
	}

	if opts.EventRoutines > 0 {
		me.startEventRoutines()
	}

	return me, nil
}

func (c *Service) startTidyRoutines() {
	// Tidy work is internal busy work re-checking data to ensure it's in the correct state if it hasn't been
	// updated in a while. This is guarding against dropped events / errors.
	//
	// The rate of re-processing here is limited to one routine that fetches data in batches and some number
	// of worker routines.
	//
	// This means that hopefully even when processing at full speed we generate constant but a predictable
	// load on the DB (and for the process as a whole).
	//
	// Note that these routines are seperate from dedicated event routines.

	tidyLayerWork := make(chan *structs.Layer)
	tidyTaskWork := make(chan *structs.Task)
	go func() {
		defer close(tidyLayerWork)
		defer close(tidyTaskWork)

		tickLyr := time.NewTicker(c.opts.TidyLayerFrequency)
		tickTsk := time.NewTicker(c.opts.TidyTaskFrequency)
		for {
			select {
			case <-tickLyr.C:
				c.queueTidyLayerWork(c.errs, tidyLayerWork)
			case <-tickTsk.C:
				c.queueTidyTaskWork(c.errs, tidyTaskWork)
			}
		}
	}()

	for i := int64(0); i < c.opts.TidyRoutines; i++ {
		go func() {
			for {
				// Nb. we could batch here, but since we simply re-call the handle event (which takes 1)
				// the batch wouldn't acheive much.
				select {
				case w := <-tidyLayerWork:
					c.errs <- c.handleLayerEvent(&changes.Change{Kind: structs.KindLayer, Old: w, New: w})
				case w := <-tidyTaskWork:
					c.errs <- c.handleTaskEvent(&changes.Change{Kind: structs.KindTask, Old: w, New: w})
				}
			}
		}()
	}
}

func (c *Service) startEventRoutines() {
	// Rather than have each worker routine listen for events and deal with massive
	// numbers of duplicates (and more work for the DB) we have a single routine
	// that fetches events and passes them to a channel.
	//
	// We can still have multiple processes fetching events (ie. multiple igor services)
	// but this greatly cuts down on duplicate work.
	evtWork := make(chan *changes.Change)

	go func() {
		// Listen to changes. Retry on error(s) forever.
		defer close(evtWork)

		var stream changes.Stream
		var err error
		for {
			stream, err = c.db.Changes()
			if err != nil {
				c.errs <- err
				continue
			}

			for {
				evt, err := stream.Next()
				if err != nil {
					c.errs <- err
					break
				}
				if evt == nil {
					return
				}
				evtWork <- evt
			}
		}
	}()

	for i := int64(0); i < c.opts.EventRoutines; i++ {
		go func() {
			c.handleEvents(c.errs, evtWork)
		}()
	}
}

func (c *Service) Close() error {
	c.qu.Close()
	c.db.Close()
	return nil
}

func (c *Service) Register(task string, handler func(work []*queue.Meta) error) error {
	return c.qu.Register(task, handler)
}

func (c *Service) Run() error {
	return c.qu.Run()
}

func (c *Service) Pause(r []*structs.ObjectRef) (int64, error) {
	return c.togglePause(time.Now().Unix(), r)
}

func (c *Service) Unpause(r []*structs.ObjectRef) (int64, error) {
	return c.togglePause(0, r)
}

func (c *Service) togglePause(now int64, r []*structs.ObjectRef) (int64, error) {
	byKind, err := validateToggles(r)
	if err != nil {
		return 0, err
	}

	var count int64
	etag := utils.NewRandomID()
	for k, toggles := range byKind {
		switch k {
		case structs.KindLayer:
			delta, err := c.db.SetLayersPaused(now, etag, toggles)
			count += delta
			if err != nil {
				return count, err
			}
		case structs.KindTask:
			delta, err := c.db.SetTasksPaused(now, etag, toggles)
			count += delta
			if err != nil {
				return count, err
			}
		default:
			log.Println("pause not supported on kind", k)
		}
	}

	return count, nil
}

func (c *Service) Retry(r []*structs.ObjectRef) (int64, error) {
	byKind, err := validateToggles(r)
	if err != nil {
		return 0, err
	}

	var count int64
	for k, toggles := range byKind {
		switch k {
		case structs.KindTask:
			delta, err := c.retryTasks(toggles)
			count += delta
			if err != nil {
				return count, err
			}
		default:
			log.Println("retry not supported on kind", k)
		}
	}

	return count, nil
}

func (c *Service) retryTasks(tt []*structs.ObjectRef) (int64, error) {
	taskIds := []string{}
	taskIdToEtag := map[string]string{}
	for _, t := range tt {
		taskIds = append(taskIds, t.ID)
		taskIdToEtag[t.ID] = t.ETag
	}

	tasks, err := c.db.Tasks(&structs.Query{Limit: len(taskIds), TaskIDs: taskIds})
	if err != nil {
		return 0, err
	}

	enqueue := []*structs.Task{}
	for _, t := range tasks {
		expected, _ := taskIdToEtag[t.ID]
		if t.ETag != expected { // task state has changed, user may no longer want to retry
			continue
		}
		if t.QueueTaskID != "" { // task could be running: at least try to kill it
			c.errs <- c.killTask(t)
		}
		enqueue = append(enqueue, t)
	}

	return int64(len(enqueue)), c.enqueueTasks(enqueue)
}

func (c *Service) Skip(r []*structs.ObjectRef) (int64, error) {
	byKind, err := validateToggles(r)
	if err != nil {
		return 0, err
	}

	var count int64
	etag := utils.NewRandomID()
	for k, toggles := range byKind {
		switch k {
		case structs.KindLayer:
			delta, err := c.db.SetLayersStatus(structs.SKIPPED, etag, toggles)
			count += delta
			if err != nil {
				return count, err
			}
		case structs.KindTask:
			delta, err := c.db.SetTasksStatus(structs.SKIPPED, etag, toggles)
			count += delta
			if err != nil {
				return count, err
			}
		default:
			log.Println("skip not supported on kind", k)
		}
	}

	return count, nil
}

func (c *Service) Kill(r []*structs.ObjectRef) (int64, error) {
	byKind, err := validateToggles(r)
	if err != nil {
		return 0, err
	}

	var count int64
	etag := utils.NewRandomID()
	for k, toggles := range byKind {
		switch k {
		case structs.KindTask:
			delta, err := c.db.SetTasksStatus(structs.KILLED, etag, toggles)
			count += delta
			if err != nil {
				return count, err
			}
		default:
			log.Println("kill not supported on kind", k)
		}
	}

	return count, nil
}

func (c *Service) Jobs(q *structs.Query) ([]*structs.Job, error) {
	q.Sanitize()
	return c.db.Jobs(q)
}

func (c *Service) Layers(q *structs.Query) ([]*structs.Layer, error) {
	q.Sanitize()
	return c.db.Layers(q)
}

func (c *Service) Tasks(q *structs.Query) ([]*structs.Task, error) {
	q.Sanitize()
	return c.db.Tasks(q)
}

func (c *Service) CreateTasks(in []*structs.CreateTaskRequest) ([]*structs.Task, error) {
	// validate; the input must be valid and layers where given must exist
	if in == nil || len(in) == 0 {
		return nil, errors.ErrNoTasks
	}
	needNewJob := false
	layersByID := map[string]*structs.Layer{}
	layerIDs := []string{}
	for _, t := range in {
		err := validateTaskSpec(&t.TaskSpec)
		if err != nil {
			return nil, err
		}
		if t.LayerID == "" {
			needNewJob = true
		} else if utils.IsValidID(t.LayerID) {
			if _, ok := layersByID[t.LayerID]; !ok {
				layersByID[t.LayerID] = nil
				layerIDs = append(layerIDs, t.LayerID)
			}
		} else {
			return nil, fmt.Errorf("%w layer id %s is invalid", errors.ErrInvalidArg, t.LayerID)
		}
	}

	// if we have layers to look up, do so
	var layers []*structs.Layer
	var err error
	if len(layersByID) > 0 {
		layers, err = c.db.Layers(&structs.Query{Limit: len(layerIDs), LayerIDs: layerIDs})
		if err != nil {
			return nil, err
		}
		if len(layers) != len(layerIDs) { // we require all the layers to exist
			return nil, fmt.Errorf("%w expected %d layers, got %d", errors.ErrParentNotFound, len(layerIDs), len(layers))
		}
		for _, l := range layers {
			layersByID[l.ID] = l
		}
	}

	// now we can attempt to build everything
	etag := utils.NewRandomID()
	var newJob *structs.Job
	var newLayer *structs.Layer
	if needNewJob {
		newJob = &structs.Job{
			ID:     utils.NewRandomID(),
			Status: structs.PENDING,
			ETag:   etag,
		}
		newLayer = &structs.Layer{
			ID:     utils.NewRandomID(),
			JobID:  newJob.ID,
			Status: structs.PENDING,
			ETag:   etag,
		}
		layersByID[newLayer.ID] = newLayer
		layersByID[""] = newLayer
	}

	newTasks := []*structs.Task{}
	addTasks := []*structs.Task{}
	for _, t := range in {
		parentLayer, ok := layersByID[t.LayerID]
		if !ok || parentLayer == nil {
			return nil, fmt.Errorf("%w parent layer %s not found for task", errors.ErrParentNotFound, t.LayerID)
		}
		if !layerCanHaveMoreTasks(parentLayer) {
			return nil, fmt.Errorf("%w parent layer %s cannot have more tasks", errors.ErrInvalidState, t.LayerID)
		}
		tsk := &structs.Task{
			TaskSpec: t.TaskSpec,
			ID:       utils.NewRandomID(),
			JobID:    parentLayer.JobID,
			LayerID:  parentLayer.ID,
			Status:   structs.PENDING,
			ETag:     etag,
		}
		if t.LayerID == "" {
			newTasks = append(newTasks, tsk)
		} else {
			addTasks = append(addTasks, tsk)
		}
	}

	// insert into db
	if needNewJob {
		err = c.db.InsertJob(newJob, []*structs.Layer{newLayer}, newTasks)
		if err != nil {
			return nil, err
		}
	}
	return append(newTasks, addTasks...), c.db.InsertTasks(addTasks)
}

// CreateJob creates a new job with the given layers and tasks.
// Tasks may be enqueued into Layers
func (c *Service) CreateJob(cjr *structs.CreateJobRequest) (*structs.CreateJobResponse, error) {
	// validate input
	err := validateCreateJobRequest(cjr)
	if err != nil {
		return nil, err
	}

	// build structs
	job, layers, tasks, tasks_by_layer := buildJob(cjr)

	// insert into db
	err = c.db.InsertJob(job, layers, tasks)
	if err != nil {
		return nil, err
	}

	// build response
	resp_layers := []*structs.JobLayerResponse{}
	for _, l := range layers {
		resp_layers = append(resp_layers, &structs.JobLayerResponse{
			Layer: l,
			Tasks: tasks_by_layer[l.ID],
		})
	}
	return &structs.CreateJobResponse{
		Job:    job,
		Layers: resp_layers,
	}, nil
}

func (c *Service) enqueueTasks(ts []*structs.Task) error {
	etag := utils.NewRandomID()
	for _, t := range ts {
		if t.PausedAt > 0 {
			continue
		}
		queueTaskID, err := c.qu.Enqueue(t)
		if err != nil {
			_, derr := c.db.SetTasksStatus(structs.ERRORED, etag, []*structs.ObjectRef{{ID: t.ID, ETag: t.ETag}}, err.Error())
			if derr != nil {
				c.errs <- fmt.Errorf("failed to set task %s status to ERRORED, database error %v, original queue error %v\n", t.ID, derr, err)
			}
			continue
		}
		err = c.db.SetTaskQueueID(t.ID, t.ETag, etag, queueTaskID, structs.QUEUED)
		if err != nil {
			c.qu.Kill(queueTaskID)
			_, derr := c.db.SetTasksStatus(structs.ERRORED, etag, []*structs.ObjectRef{{ID: t.ID, ETag: t.ETag}}, err.Error())
			if derr != nil {
				c.errs <- fmt.Errorf("failed to set task %s queue id, database error %v, original queue error %v\n", t.ID, derr, err)
			}
		} else {
			t.QueueTaskID = queueTaskID
			t.Status = structs.QUEUED
			t.ETag = etag
		}
	}
	return nil
}

func (c *Service) handleEvents(errchan chan<- error, evtWork chan *changes.Change) {
	for evt := range evtWork {
		var err error
		switch evt.Kind {
		case structs.KindLayer:
			err = c.handleLayerEvent(evt)
		case structs.KindTask:
			err = c.handleTaskEvent(evt)
		default:
			err = fmt.Errorf("%w %s unknown kind", errors.ErrNotSupported, evt.Kind)
		}
		if err != nil {
			errchan <- err
		}
	}
}

func (c *Service) handleLayerEvent(evt *changes.Change) error {
	if evt.New == nil { // deleted
		return nil
	}
	eNew := evt.New.(*structs.Layer)

	if structs.IsFinalStatus(eNew.Status) {
		if eNew.Status == structs.SKIPPED {
			c.errs <- c.skipLayerTasks(eNew.ID)
		}
		jobStatus, layers, err := c.determineJobStatus(eNew.JobID)
		if err != nil {
			return err
		}
		if structs.IsFinalStatus(jobStatus) {
			jobs, err := c.db.Jobs(&structs.Query{JobIDs: []string{eNew.JobID}, Limit: 1})
			if err != nil {
				return err
			}
			if len(jobs) != 1 {
				return fmt.Errorf("expected 1 job with id %s, found %d", eNew.JobID, len(jobs))
			}
			_, err = c.db.SetJobsStatus(
				jobStatus,
				utils.NewRandomID(),
				[]*structs.ObjectRef{{ID: jobs[0].ID, ETag: jobs[0].ETag}},
			)
			return err
		}
		if len(layers) == 0 {
			return nil
		}
		ids := []*structs.ObjectRef{}
		for _, l := range layers {
			ids = append(ids, &structs.ObjectRef{ID: l.ID, ETag: l.ETag})
		}
		_, err = c.db.SetLayersStatus(
			structs.RUNNING,
			utils.NewRandomID(),
			ids,
		)
		return err
	} else if eNew.PausedAt > 0 { // we're paused (even if we weren't just set paused)
		return nil
	} else if eNew.Status == structs.RUNNING {
		// layer should be running; enqueue all tasks that need queuing
		enqueued, err := c.enqueueLayerTasks(eNew.ID)
		if err != nil {
			return err
		}
		if enqueued > 0 {
			return nil // running must be the correct status
		}
		desired, err := c.determineLayerStatus(eNew.ID)
		if err != nil {
			return err
		}
		if desired != eNew.Status {
			_, err = c.db.SetLayersStatus(desired, utils.NewRandomID(), []*structs.ObjectRef{{ID: eNew.ID, ETag: eNew.ETag}})
			return err
		}
	}
	return nil
}

func (c *Service) enqueueLayerTasks(layerID string) (int, error) {
	q := &structs.Query{
		Limit:    defLimit,
		Offset:   0,
		LayerIDs: []string{layerID},
		Statuses: []structs.Status{structs.PENDING},
	}
	total := 0
	for {
		tasks, err := c.db.Tasks(q)
		if err != nil {
			return total, err
		}
		total += len(tasks)

		err = c.enqueueTasks(tasks)
		if err != nil {
			return total, err
		}

		if len(tasks) < q.Limit {
			return total, nil
		}
		q.Offset += q.Limit
	}
}

func (c *Service) determineLayerStatus(layerID string) (structs.Status, error) {
	q := &structs.Query{
		Limit:    defLimit,
		Offset:   0,
		LayerIDs: []string{layerID},
		Statuses: append(incompleteStates, structs.ERRORED, structs.KILLED),
	}

	// metrics
	finalStates := 0
	nonFinalStates := 0
	states := map[structs.Status]int64{
		structs.PENDING: 0,
		structs.QUEUED:  0,
		structs.RUNNING: 0,
		structs.KILLED:  0,
		structs.ERRORED: 0,
	}

	// gather task metrics
	for {
		tasks, err := c.db.Tasks(q)
		if err != nil {
			return structs.PENDING, err
		}
		for _, t := range tasks {
			if t.Status == structs.RUNNING || t.Status == structs.QUEUED {
				// if any task is running, the layer is running
				return structs.RUNNING, nil
			}
			states[t.Status]++
			if structs.IsFinalStatus(t.Status) {
				finalStates++
			} else {
				nonFinalStates++
			}
		}
		if len(tasks) < q.Limit {
			break
		}
		q.Offset += q.Limit
	}

	if nonFinalStates > 0 {
		// since it can't be RUNNING or QUEUED it must be PENDING (only TASKs are KILLED)
		return structs.PENDING, nil
	}
	if states[structs.ERRORED] > 0 || states[structs.KILLED] > 0 {
		// if a task ERRORED & hasn't been skipped, the layer is ERRORED
		return structs.ERRORED, nil
	}
	// This ignores 'SKIPPED' tasks, for the purpose of that layer status they don't exist.
	return structs.COMPLETED, nil
}

func (c *Service) determineJobStatus(jobID string) (structs.Status, []*structs.Layer, error) {
	q := &structs.Query{
		Limit:    defLimit,
		JobIDs:   []string{jobID},
		Statuses: append(incompleteStates, structs.ERRORED, structs.KILLED),
	}
	all_layers := []*structs.Layer{}
	for {
		layers, err := c.db.Layers(q)
		if err != nil {
			return structs.RUNNING, nil, err
		}
		all_layers = append(all_layers, layers...)
		if len(layers) < q.Limit {
			break
		}
		q.Offset += q.Limit
	}
	jobState, canRun := determineJobStatus(all_layers)
	return jobState, canRun, nil
}

func (c *Service) killTask(task *structs.Task) error {
	if task.QueueTaskID == "" {
		return nil
	}
	err := c.qu.Kill(task.QueueTaskID)
	if err != nil {
		return err
	}
	etag := utils.NewRandomID()
	err = c.db.SetTaskQueueID(task.ID, task.ETag, etag, "", structs.ERRORED)
	task.ETag = etag
	task.QueueTaskID = ""
	task.Status = structs.ERRORED
	return err
}

func (c *Service) handleTaskEvent(evt *changes.Change) error {
	// deleted or created
	if evt.New == nil || evt.Old == nil {
		return nil
	}
	// updated
	eNew := evt.New.(*structs.Task)

	parent, err := c.db.Layers(&structs.Query{Limit: 1, LayerIDs: []string{eNew.LayerID}})
	if err != nil {
		return err
	}
	if len(parent) != 1 {
		return fmt.Errorf("expected 1 parent layer, got %d", len(parent))
	}

	if structs.IsFinalStatus(eNew.Status) { // task is finished
		if eNew.Status == structs.KILLED {
			err = c.killTask(eNew)
			if err != nil {
				return err
			}
		}

		// Check: If I have to update the parent layer status?
		// ie. COMPLETED | SKIPPED | ERRORED | KILLED
		if parent[0].Status == structs.SKIPPED {
			return nil
		}
		desired, err := c.determineLayerStatus(eNew.LayerID)
		if err != nil {
			return err
		}
		if parent[0].Status == desired {
			return nil // no update is needed
		}
		_, err = c.db.SetLayersStatus(desired, utils.NewRandomID(), []*structs.ObjectRef{{ID: parent[0].ID, ETag: parent[0].ETag}})
		return err
	} else if eNew.PausedAt > 0 || parent[0].PausedAt > 0 {
		return nil
	} else if parent[0].Status == structs.SKIPPED { // implies we're not skipped since we're not in a final state
		if eNew.QueueTaskID != "" {
			c.killTask(eNew)
		}
		_, err := c.db.SetTasksStatus(structs.SKIPPED, utils.NewRandomID(), []*structs.ObjectRef{{ID: eNew.ID, ETag: eNew.ETag}})
		return err
	} else if parent[0].Status == structs.RUNNING && eNew.QueueTaskID == "" { // we should be queued but aren't
		return c.enqueueTasks([]*structs.Task{eNew})
	}

	return nil
}

func (c *Service) skipLayerTasks(layerID string) error {
	q := &structs.Query{
		Limit:    defLimit,
		Offset:   0,
		LayerIDs: []string{layerID},
		Statuses: incompleteStates,
	}

	for {
		tasks, err := c.db.Tasks(q)
		if err != nil {
			return err
		}

		toSkip := []*structs.ObjectRef{}
		for _, t := range tasks {
			toSkip = append(toSkip, &structs.ObjectRef{ID: t.ID, ETag: t.ETag})
		}
		if len(toSkip) > 0 {
			_, err = c.db.SetTasksStatus(structs.SKIPPED, utils.NewRandomID(), toSkip)
			if err != nil {
				return err
			}
		}
		if len(tasks) < q.Limit {
			return nil
		}
		q.Offset += q.Limit
	}
}

func (c *Service) queueTidyLayerWork(errchan chan<- error, work chan<- *structs.Layer) {
	q := &structs.Query{
		Limit:         defLimit,
		Offset:        0,
		Statuses:      incompleteStates,
		UpdatedBefore: timeNow() - int64(c.opts.TidyUpdateThreshold.Seconds()),
	}
	for {
		layers, err := c.db.Layers(q)
		if err != nil {
			errchan <- err
			break
		}
		for _, l := range layers {
			work <- l
		}
		if len(layers) < q.Limit {
			return
		}
		q.Offset += q.Limit
	}
}

func (c *Service) queueTidyTaskWork(errchan chan<- error, work chan<- *structs.Task) {
	q := &structs.Query{
		Limit:         defLimit,
		Offset:        0,
		Statuses:      incompleteStates,
		UpdatedBefore: timeNow() - int64(c.opts.TidyUpdateThreshold.Seconds()),
	}
	for {
		tasks, err := c.db.Tasks(q)
		if err != nil {
			errchan <- err
			break
		}
		for _, t := range tasks {
			work <- t
		}
		if len(tasks) < q.Limit {
			return
		}
		q.Offset += q.Limit
	}
}
