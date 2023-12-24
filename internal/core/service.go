package core

import (
	"fmt"
	"log"
	"time"

	"github.com/voidshard/igor/internal/utils"
	"github.com/voidshard/igor/pkg/database"
	"github.com/voidshard/igor/pkg/errors"
	"github.com/voidshard/igor/pkg/queue"
	"github.com/voidshard/igor/pkg/structs"
)

const (
	// max values
	maxNameLength = 500
	maxTypeLength = 500
	maxArgsLength = 10000

	// defaults
	defEventRoutines = 10
	defTidyRoutines  = 2
	defTidyFrequency = 2 * time.Minute
	defReapFrequency = 10 * time.Minute
	defMaxTaskTime   = 24 * time.Hour
)

var (
	incompleteStates = []structs.Status{
		structs.PENDING,
		structs.QUEUED,
		structs.RUNNING,
	}
)

type Service struct {
	db   database.Database
	qu   queue.Queue
	opts *structs.Options
}

func NewService(db database.Database, qu queue.Queue, opts *structs.Options) (*Service, error) {
	if opts == nil {
		opts = &structs.Options{
			EventRoutines:     defEventRoutines,
			TidyRoutines:      defTidyRoutines,
			TidyJobFrequency:  defTidyFrequency,
			TidyTaskFrequency: defReapFrequency,
			MaxTaskRuntime:    defMaxTaskTime,
		}
	}
	me := &Service{db: db, qu: qu, opts: opts}

	errs := make(chan error)
	go func() {
		for err := range errs {
			if err != nil {
				log.Println("[Service]", err)
			}
		}
	}()

	if opts.TidyRoutines > 0 {
		// TidyRoutines work in batches, periodically rechecking jobs that aren't complete
		// (and their layers / tasks) in case some event(s) were dropped / missed.

		tidyJobWork := make(chan []*structs.Job)
		reapTaskWork := make(chan []*structs.Task)
		go func() {
			defer close(tidyJobWork)
			defer close(reapTaskWork)

			tickJob := time.NewTicker(opts.TidyJobFrequency)
			tickTask := time.NewTicker(opts.TidyTaskFrequency)
			for {
				select {
				case <-tickJob.C:
					me.queueTidyJobWork(errs, tidyJobWork)
				case <-tickTask.C:
					me.queueTidyTaskWork(errs, reapTaskWork)
				}
			}
		}()

		for i := int64(0); i < opts.TidyRoutines; i++ {
			go func() {
				for {
					select {
					case jobs := <-tidyJobWork:
						me.handleTidyJobs(errs, jobs)
					case tasks := <-reapTaskWork:
						me.handleReapTasks(errs, tasks)
					}
				}
			}()
		}
	}

	if opts.EventRoutines > 0 {
		// Rather than have each worker routine listen for events and deal with massive
		// numbers of duplicates (and more work for the DB) we have a single routine
		// that fetches events and passes them to a channel.
		//
		// We can still have multiple processes fetching events (ie. multiple igor services)
		// but this greatly cuts down on duplicate work.

		evtWork := make(chan *database.Change)

		go func() {
			// Listen to changes. Retry on error(s) forever.
			defer close(evtWork)

			var stream database.ChangeStream
			var err error
			for {
				stream, err = db.Changes()
				if err == nil {
					errs <- err
					continue
				}

				for {
					evt, err := stream.Next()
					if err != nil {
						errs <- err
						break
					}
					if evt == nil {
						return
					}
					evtWork <- evt
				}
			}
		}()

		for i := int64(0); i < opts.EventRoutines; i++ {
			go func() {
				me.handleEvents(errs, evtWork)
			}()
		}
	}

	return me, nil
}

func (c *Service) Close() error {
	c.qu.Close()
	c.db.Close()
	return nil
}

func (c *Service) Pause(r []*structs.ToggleRequest) (int64, error) {
	return c.togglePause(time.Now().Unix(), r)
}

func (c *Service) Unpause(r []*structs.ToggleRequest) (int64, error) {
	return c.togglePause(0, r)
}

func (c *Service) togglePause(now int64, r []*structs.ToggleRequest) (int64, error) {
	byKind, err := validateToggles(r)
	if err != nil {
		return 0, err
	}

	var count int64
	etag := utils.NewRandomID()
	for k, toggles := range byKind {
		switch k {
		case structs.KindLayer:
			c, err := c.db.SetLayersPaused(now, etag, toggles)
			count += c
			if err != nil {
				return count, err
			}
		case structs.KindTask:
			c, err := c.db.SetTasksPaused(now, etag, toggles)
			count += c
			if err != nil {
				return count, err
			}
		default:
			log.Println("pause not supported on kind", k)
		}
	}

	return count, nil
}

func (c *Service) Skip(r []*structs.ToggleRequest) (int64, error) {
	byKind, err := validateToggles(r)
	if err != nil {
		return 0, err
	}

	var count int64
	etag := utils.NewRandomID()
	for k, toggles := range byKind {
		switch k {
		case structs.KindLayer:
			c, err := c.db.SetLayersStatus(structs.SKIPPED, etag, toggles)
			count += c
			if err != nil {
				return count, err
			}
		case structs.KindTask:
			c, err := c.db.SetTasksStatus(structs.SKIPPED, etag, toggles)
			count += c
			if err != nil {
				return count, err
			}
		default:
			log.Println("skip not supported on kind", k)
		}
	}

	return count, nil
}

func (c *Service) Kill(r []*structs.ToggleRequest) (int64, error) {
	byKind, err := validateToggles(r)
	if err != nil {
		return 0, err
	}

	var count int64
	etag := utils.NewRandomID()
	for k, toggles := range byKind {
		switch k {
		case structs.KindTask:
			c, err := c.db.SetTasksStatus(structs.KILLED, etag, toggles)
			count += c
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

func (c *Service) Runs(q *structs.Query) ([]*structs.Run, error) {
	q.Sanitize()
	return c.db.Runs(q)
}

func (c *Service) CreateTasks(in []*structs.CreateTaskRequest) ([]*structs.Task, error) {
	// validate; the input must be valid and layers / jobs where given must exist
	if in == nil || len(in) == 0 {
		return nil, errors.ErrNoTasks
	}
	needNewJob := false
	layerIDs := map[string]*structs.Layer{}
	for _, t := range in {
		err := validateTaskSpec(&t.TaskSpec)
		if err != nil {
			return nil, err
		}
		if t.LayerID == "" {
			needNewJob = true
		} else {
			layerIDs[t.LayerID] = nil
		}
	}
	ids := []string{}
	for k := range layerIDs {
		ids = append(ids, k)
	}
	layers, err := c.db.Layers(&structs.Query{Limit: len(layerIDs), LayerIDs: ids})
	if err != nil {
		return nil, err
	}
	for _, l := range layers {
		layerIDs[l.ID] = l
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
		layerIDs[newLayer.ID] = newLayer
		layerIDs[""] = newLayer
	}

	newTasks := []*structs.Task{}
	addTasks := []*structs.Task{}
	for _, t := range in {
		parentLayer, ok := layerIDs[t.LayerID]
		if !ok || parentLayer == nil {
			return nil, fmt.Errorf("%w parent layer %s not found for task", errors.ErrParentNoFound, t.LayerID)
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
	runs := []*structs.Run{}
	etag := utils.NewRandomID()
	for _, t := range ts {
		r := &structs.Run{
			ID:      utils.NewRandomID(),
			Status:  structs.QUEUED,
			ETag:    etag,
			JobID:   t.JobID,
			LayerID: t.LayerID,
			TaskID:  t.ID,
		}
		queueTaskID, err := c.qu.Enqueue(t, r)
		r.QueueTaskID = queueTaskID
		if err != nil {
			r.Message = err.Error()
			r.Status = structs.ERRORED
		}
		runs = append(runs, r)
	}
	return c.db.InsertRuns(runs)
}

func (c *Service) handleEvents(errchan chan<- error, evtWork chan *database.Change) {
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

func (c *Service) handleLayerEvent(evt *database.Change) error {
	if evt.Old == nil || evt.New == nil { // deleted or created
		// nb. tasks that are created with an already running layer are enqueued
		// (see handleTaskEvent) otherwise we would enqueue them here
		return nil
	}

	eNew := evt.New.(*structs.Layer)
	eOld := evt.Old.(*structs.Layer)
	if !isFinalStatus(eOld.Status) && isFinalStatus(eNew.Status) { // we changed to some end state
		if eNew.Status == structs.ERRORED { // we're broken :(
			return nil
		}
		jobStatus, layers, err := c.runnableJobLayers(eNew.JobID)
		if err != nil {
			return err
		}
		if jobStatus == structs.COMPLETED {
			_, err = c.db.SetJobsStatus(
				structs.COMPLETED,
				utils.NewRandomID(),
				[]*database.IDTag{&database.IDTag{ID: eNew.JobID, ETag: eNew.ETag}},
			)
			return err
		}
		if len(layers) == 0 {
			return nil
		}
		ids := []*database.IDTag{}
		for _, l := range layers {
			ids = append(ids, &database.IDTag{ID: l.ID, ETag: l.ETag})
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
		return c.setLayerTasksQueued(eNew.ID)
	}
	return nil
}

func (c *Service) setLayerTasksQueued(layerID string) error {
	q := &structs.Query{
		Limit:    2000,
		Offset:   0,
		LayerIDs: []string{layerID},
		Statuses: []structs.Status{structs.PENDING},
	}
	for {
		tasks, err := c.db.Tasks(q)
		if err != nil {
			return err
		}
		ids := []*database.IDTag{}
		for _, t := range tasks {
			ids = append(ids, &database.IDTag{ID: t.ID, ETag: t.ETag})
		}
		_, err = c.db.SetTasksStatus(
			structs.QUEUED,
			utils.NewRandomID(),
			ids,
		)
		if err != nil {
			return err
		}
		if len(tasks) < q.Limit {
			return nil
		}
		q.Offset += q.Limit
	}
}

func (c *Service) determineLayerStatus(jobID, layerID string) (structs.Status, error) {
	q := &structs.Query{
		Limit:    2000,
		Offset:   0,
		LayerIDs: []string{layerID},
		Statuses: append(incompleteStates, structs.ERRORED),
	}

	// metrics
	finalStates := 0
	nonFinalStates := 0
	states := map[structs.Status]int64{
		structs.PENDING:   0,
		structs.QUEUED:    0,
		structs.RUNNING:   0,
		structs.KILLED:    0,
		structs.COMPLETED: 0,
		structs.ERRORED:   0,
		structs.SKIPPED:   0,
	}

	// gather task metrics
	for {
		tasks, err := c.db.Tasks(&structs.Query{
			LayerIDs: []string{layerID},
		})
		if err != nil {
			return structs.PENDING, err
		}
		for _, t := range tasks {
			if t.Status == structs.RUNNING || t.Status == structs.QUEUED {
				// if any task is running, the layer is running
				return structs.RUNNING, nil
			}
			states[t.Status]++
			if isFinalStatus(t.Status) {
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
	if states[structs.ERRORED] > 0 {
		// if a task ERRORED & hasn't been skipped, the layer is ERRORED
		return structs.ERRORED, nil
	}
	// This ignores 'SKIPPED' tasks, for the purpose of that layer status they don't exist.
	return structs.COMPLETED, nil
}

func (c *Service) runnableJobLayers(jobID string) (structs.Status, []*structs.Layer, error) {
	layers, err := c.db.Layers(&structs.Query{JobIDs: []string{jobID}})
	if err != nil {
		return structs.PENDING, nil, err
	}
	jobState, canRun := runnableJobLayers(layers)
	return jobState, canRun, nil
}

func (c *Service) killTask(task *structs.Task) error {
	runs, err := c.db.Runs(&structs.Query{TaskIDs: []string{task.ID}})
	if err != nil {
		return err
	}
	for _, r := range runs {
		err = c.qu.Kill(r.QueueTaskID)
		if err != nil {
			log.Println("failed to kill task", task.ID, "run", r.ID, "queue task", r.QueueTaskID, err)
			continue
		}
	}
	newState := structs.QUEUED
	if int64(len(runs)) >= task.Retries {
		newState = structs.ERRORED
	}
	_, err = c.db.SetTasksStatus(
		newState,
		utils.NewRandomID(),
		[]*database.IDTag{&database.IDTag{ID: task.ID, ETag: task.ETag}},
	)
	return err
}

func (c *Service) handleTaskEvent(evt *database.Change) error {
	// deleted
	if evt.New == nil {
		return nil
	}

	// created
	eNew := evt.New.(*structs.Task)
	if evt.Old == nil { // created
		if eNew.Status == structs.QUEUED && eNew.PausedAt == 0 {
			// the task was just created 'QUEUED' (ie, layer created RUNNING)
			return c.enqueueTasks([]*structs.Task{eNew})
		}
		return nil
	}

	// updated
	eOld := evt.Old.(*structs.Task)
	if eOld.Status != structs.KILLED && eNew.Status == structs.KILLED { // we've been killed
		return c.killTask(eNew)
	} else if !isFinalStatus(eOld.Status) && isFinalStatus(eNew.Status) { // task is finished
		// ie.
		// PENDING | QUEUED | RUNNING
		//  - to -
		// COMPLETED | SKIPPED | ERRORED
		parent, err := c.db.Layers(&structs.Query{LayerIDs: []string{eNew.LayerID}})
		if err != nil {
			return err
		}
		if len(parent) != 1 {
			return fmt.Errorf("expected 1 parent layer, got %d", len(parent))
		}
		if parent[0].Status == structs.SKIPPED {
			return nil
		}
		desired, err := c.determineLayerStatus(eNew.JobID, eNew.LayerID)
		if err != nil {
			return err
		}
		if parent[0].Status == desired {
			return nil // no update is needed
		}
		_, err = c.db.SetLayersStatus(desired, utils.NewRandomID(), []*database.IDTag{&database.IDTag{ID: eNew.LayerID, ETag: eNew.ETag}})
		return err
	} else if eNew.PausedAt > 0 { // we're paused (even if we weren't just set paused)
		return nil
	} else if eOld.Status != structs.QUEUED && eNew.Status == structs.QUEUED { // we've been queued
		return c.enqueueTasks([]*structs.Task{eNew})
	}

	return nil
}

func (c *Service) tidyJob(job *structs.Job, layers []*structs.Layer) error {
	jobState, canRun := runnableJobLayers(layers)

	// maybe we're done?
	if jobState == structs.COMPLETED {
		_, err := c.db.SetJobsStatus(
			structs.COMPLETED,
			utils.NewRandomID(),
			[]*database.IDTag{&database.IDTag{ID: job.ID, ETag: job.ETag}},
		)
		return err
	}

	// layers we could set to running
	runMap := map[string]bool{}
	for _, l := range canRun {
		runMap[l.ID] = true
	}

	for _, l := range layers {
		// workout the layer status from task statuses
		proposedStatus, err := c.determineLayerStatus(l.JobID, l.ID)
		if err != nil {
			return err
		}

		// now, if we could be run, and neither the proposed or current state is final
		// (ie. completed, errored, skipped) then we should be running
		couldBeRun, _ := runMap[l.ID]
		if couldBeRun && !isFinalStatus(l.Status) && !isFinalStatus(proposedStatus) {
			proposedStatus = structs.RUNNING
		}

		// finally, if we're not already in the proposed state, set it
		if l.Status != proposedStatus {
			_, err = c.db.SetLayersStatus(
				proposedStatus,
				utils.NewRandomID(),
				[]*database.IDTag{&database.IDTag{ID: l.ID, ETag: l.ETag}},
			)
			if err != nil {
				return err
			}
			continue
		}

	}

	return nil
}

func (c *Service) handleTidyJobs(errchan chan<- error, jobs []*structs.Job) {
	if jobs == nil || len(jobs) == 0 {
		return
	}

	// batch get layers
	jobIDs := []string{}
	for _, j := range jobs {
		jobIDs = append(jobIDs, j.ID)
	}
	layers, err := c.db.Layers(&structs.Query{JobIDs: jobIDs, Statuses: incompleteStates})
	if err != nil {
		errchan <- err
		return
	}
	byJob := map[string][]*structs.Layer{}
	for _, l := range layers {
		jobLayers, ok := byJob[l.JobID]
		if !ok {
			jobLayers = []*structs.Layer{}
		}
		byJob[l.JobID] = append(jobLayers, l)
	}

	// then for each job & it's layers: tidy
	for _, j := range jobs {
		layers, ok := byJob[j.ID]
		if !ok {
			layers = []*structs.Layer{}
		}
		err = c.tidyJob(j, layers)
		if err != nil {
			errchan <- err
			continue
		}
	}
}

func (c *Service) handleReapTasks(errchan chan<- error, tasks []*structs.Task) {
	for _, t := range tasks {
		// if we're running too long, kill it
		if t.Status == structs.RUNNING && time.Now().Unix() > t.CreatedAt+int64(c.opts.MaxTaskRuntime.Seconds()) {
			err := c.killTask(t)
			if err != nil {
				errchan <- err
			}
		}
		// tasks shouldn't be in the killed status long; they should transition to either QUEUED or ERRORED
		// (depending on if they're retrying or not)
		if t.Status == structs.KILLED && time.Now().Unix() > t.UpdatedAt+int64(time.Minute.Seconds()) {
			err := c.killTask(t)
			if err != nil {
				errchan <- err
			}
		}
	}
}

func (c *Service) queueTidyTaskWork(errchan chan<- error, taskWork chan<- []*structs.Task) {
	q := &structs.Query{Limit: 2000, Offset: 0, Statuses: []structs.Status{structs.RUNNING, structs.KILLED}}
	for {
		tasks, err := c.db.Tasks(q)
		if err != nil {
			errchan <- err
			break
		}
		if len(tasks) > 0 {
			taskWork <- tasks
		}
		if len(tasks) < q.Limit {
			return
		}
		q.Offset += q.Limit
	}
}

// queueTidyWork finds all jobs that are in a state that needs tidying and sends batches to worker routines
// to process them.
// We do this periodically just in case something died / was missed / restarted.
func (c *Service) queueTidyJobWork(errchan chan<- error, jobWork chan<- []*structs.Job) {
	// fetch jobs that are incomplete & pass them over to be looked at
	q := &structs.Query{Limit: 500, Offset: 0, Statuses: incompleteStates}
	for {
		jobs, err := c.db.Jobs(q)
		if err != nil {
			errchan <- err
			break
		}
		if len(jobs) > 0 {
			jobWork <- jobs
		}
		if len(jobs) < q.Limit {
			break
		}
		q.Offset += q.Limit
	}
}
