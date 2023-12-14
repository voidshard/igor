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

	// Defaut internal vars
	// - set here as it's easier to change & test (ie. pagination)
	defLimitQueueTidyJobWork     = 500
	defLimitSkipLayerTasks       = 2000
	defLimitHandleTidyJobs       = 2000
	defLimitDetermineJobStatus   = 2000
	defLimitDetermineLayerStatus = 2000
	defLimitEnqueueLayerTasks    = 2000
	defLimitQueueTidyTaskWork    = 2000
)

type Service struct {
	// implements

	db   database.Database
	qu   queue.Queue
	opts *Options
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

		evtWork := make(chan *changes.Change)

		go func() {
			// Listen to changes. Retry on error(s) forever.
			defer close(evtWork)

			var stream changes.Stream
			var err error
			for {
				stream, err = db.Changes()
				if err != nil {
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
		if t.ETag != expected {
			continue
		}
		err := c.killTask(t)
		if err != nil {
			log.Println("failed to kill task", t.ID, err)
			continue
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
			err = c.skipLayerTasks(toggles)
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
	etag := utils.NewRandomID()
	setReady := []*structs.ObjectRef{}
	for _, t := range ts {
		if t.PausedAt > 0 {
			if t.Status != structs.READY {
				setReady = append(setReady, &structs.ObjectRef{ID: t.ID, ETag: t.ETag})
			}
			continue
		}
		queueTaskID, err := c.qu.Enqueue(t)
		if err != nil {
			_, derr := c.db.SetTasksStatus(structs.ERRORED, etag, []*structs.ObjectRef{&structs.ObjectRef{ID: t.ID, ETag: t.ETag}}, err.Error())
			if err != nil {
				log.Printf("failed to set task %s status to ERRORED, database error %v, original queue error %v\n", t.ID, derr, err)
				continue
			}
		}
		_, err = c.db.SetTaskQueueID(t.ID, t.ETag, etag, queueTaskID, structs.QUEUED)
		if err != nil {
			c.qu.Kill(queueTaskID)
			_, derr := c.db.SetTasksStatus(structs.ERRORED, etag, []*structs.ObjectRef{&structs.ObjectRef{ID: t.ID, ETag: t.ETag}}, err.Error())
			if err != nil {
				log.Printf("failed to set task %s queue id, database error %v, original queue error %v\n", t.ID, derr, err)
				continue
			}
		}
		if err != nil {
			t.QueueTaskID = queueTaskID
			t.Status = structs.QUEUED
			t.ETag = etag
		}
	}
	if len(setReady) > 0 { // we couldn't enqueue these tasks, so set them as ready instead
		_, err := c.db.SetTasksStatus(structs.READY, etag, setReady)
		return err
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
		if eNew.Status == structs.ERRORED {
			return nil
		}
		jobStatus, layers, err := c.determineJobStatus(eNew.JobID)
		if err != nil {
			return err
		}
		if jobStatus == structs.COMPLETED {
			jobs, err := c.db.Jobs(&structs.Query{JobIDs: []string{eNew.JobID}, Limit: 1})
			if err != nil {
				return err
			}
			if len(jobs) != 1 {
				return fmt.Errorf("expected 1 job with id %s, found %d", eNew.JobID, len(jobs))
			}
			_, err = c.db.SetJobsStatus(
				structs.COMPLETED,
				utils.NewRandomID(),
				[]*structs.ObjectRef{&structs.ObjectRef{ID: jobs[0].ID, ETag: jobs[0].ETag}},
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
			_, err = c.db.SetLayersStatus(desired, utils.NewRandomID(), []*structs.ObjectRef{&structs.ObjectRef{ID: eNew.ID, ETag: eNew.ETag}})
			return err
		}
	}
	return nil
}

func (c *Service) enqueueLayerTasks(layerID string) (int, error) {
	q := &structs.Query{
		Limit:    defLimitEnqueueLayerTasks,
		Offset:   0,
		LayerIDs: []string{layerID},
		Statuses: []structs.Status{
			structs.PENDING,
			structs.READY,
		},
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
		Limit:    defLimitDetermineLayerStatus,
		Offset:   0,
		LayerIDs: []string{layerID},
		// ie. not COMPLETED or SKIPPED
		Statuses: append(incompleteStates, structs.ERRORED, structs.KILLED),
	}

	// metrics
	finalStates := 0
	nonFinalStates := 0
	states := map[structs.Status]int64{
		structs.PENDING: 0,
		structs.READY:   0,
		structs.QUEUED:  0,
		structs.RUNNING: 0,
		structs.KILLED:  0,
		structs.ERRORED: 0,
		//		structs.COMPLETED: 0,
		//		structs.SKIPPED:   0,
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
		Limit:    defLimitDetermineJobStatus,
		JobIDs:   []string{jobID},
		Statuses: append(incompleteStates, structs.ERRORED, structs.KILLED),
	}
	all_layers := []*structs.Layer{}
	for {
		layers, err := c.db.Layers(q)
		if err != nil {
			return structs.PENDING, nil, err
		}
		all_layers = append(all_layers, layers...)
		if len(layers) < q.Limit {
			break
		}
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
	// TODO: shouldn't this change the etag :thinking:
	etag := utils.NewRandomID()
	_, err = c.db.SetTaskQueueID(task.ID, task.ETag, etag, "", structs.READY)
	task.ETag = etag
	task.QueueTaskID = ""
	task.Status = structs.READY
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
		_, err = c.db.SetLayersStatus(desired, utils.NewRandomID(), []*structs.ObjectRef{&structs.ObjectRef{ID: parent[0].ID, ETag: parent[0].ETag}})
		return err
	} else if eNew.PausedAt > 0 || parent[0].PausedAt > 0 {
		return nil
	} else if (eNew.Status == structs.PENDING || eNew.Status == structs.READY) && eNew.QueueTaskID == "" { // we should be queued but aren't
		return c.enqueueTasks([]*structs.Task{eNew})
	}

	return nil
}

func (c *Service) tidyJob(job *structs.Job, layers []*structs.Layer) error {
	jobState, canRun := determineJobStatus(layers)

	// maybe we're done?
	if jobState == structs.COMPLETED {
		_, err := c.db.SetJobsStatus(
			structs.COMPLETED,
			utils.NewRandomID(),
			[]*structs.ObjectRef{&structs.ObjectRef{ID: job.ID, ETag: job.ETag}},
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
		proposedStatus, err := c.determineLayerStatus(l.ID)
		if err != nil {
			return err
		}

		// now, if we could be run, and neither the proposed or current state is final
		// (ie. completed, errored, skipped) then we should be running
		jobWantsLayerToRun, _ := runMap[l.ID]
		if jobWantsLayerToRun && !structs.IsFinalStatus(proposedStatus) {
			proposedStatus = structs.RUNNING
		}

		// finally, if we're not already in the proposed state, set it
		if l.Status != proposedStatus {
			_, err = c.db.SetLayersStatus(
				proposedStatus,
				utils.NewRandomID(),
				[]*structs.ObjectRef{&structs.ObjectRef{ID: l.ID, ETag: l.ETag}},
			)
			if err != nil {
				return err
			}
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

	query := &structs.Query{Limit: defLimitHandleTidyJobs, JobIDs: jobIDs, Statuses: incompleteStates}
	byJob := map[string][]*structs.Layer{}
	for {
		layers, err := c.db.Layers(query)
		if err != nil {
			errchan <- err
			return
		}
		for _, l := range layers {
			jobLayers, ok := byJob[l.JobID]
			if !ok {
				jobLayers = []*structs.Layer{}
			}
			byJob[l.JobID] = append(jobLayers, l)
		}
		if len(layers) < query.Limit {
			break
		}
		query.Offset += query.Limit
	}

	// then for each job & it's layers: tidy
	for _, j := range jobs {
		layers, ok := byJob[j.ID]
		if !ok {
			layers = []*structs.Layer{}
		}
		err := c.tidyJob(j, layers)
		if err != nil {
			errchan <- err
			continue
		}
	}
}

func (c *Service) handleReapTasks(errchan chan<- error, tasks []*structs.Task) {
	layerIDs := map[string]bool{}
	for _, t := range tasks {
		// tasks shouldn't be in the killed status long; they should transition to either QUEUED or ERRORED
		// (depending on if they're retrying or not)
		if t.Status == structs.KILLED && time.Now().Unix() > t.UpdatedAt+int64(time.Minute.Seconds()) {
			err := c.killTask(t)
			if err != nil {
				errchan <- err
			}
		}

		// we'd like to check if these need enqueuing, but for that we have to check their parent layer
		// nb. we know the State here is READY or PENDING (see query in queueTidyTaskWork)
		if t.QueueTaskID == "" && time.Now().Unix() > t.UpdatedAt+int64(5*time.Minute.Seconds()) && t.PausedAt == 0 {
			layerIDs[t.LayerID] = true
		}
	}

	// ok, look up the layers and check if we can enqueue any tasks (layer isnt paused / skipped)
	if len(layerIDs) == 0 {
		return
	}
	q := &structs.Query{Limit: 0, Offset: 0, LayerIDs: []string{}}
	for k := range layerIDs {
		q.LayerIDs = append(q.LayerIDs, k)
	}
	q.Limit = len(q.LayerIDs)

	layers, err := c.db.Layers(q)
	if err != nil {
		errchan <- err
		return
	}
	layerMap := map[string]*structs.Layer{}
	for _, l := range layers {
		layerMap[l.ID] = l
	}

	// check for tasks that should be queued but aren't & queue them
	toSkip := []*structs.ObjectRef{}
	for _, t := range tasks {
		layer, ok := layerMap[t.LayerID]
		if !ok {
			continue
		}
		if layer.Status == structs.SKIPPED {
			toSkip = append(toSkip, &structs.ObjectRef{ID: t.ID, ETag: t.ETag})
			continue
		}
		// it seems to have failed to enqueue, try again
		err := c.enqueueTasks([]*structs.Task{t})
		if err != nil {
			errchan <- err
		}
	}

	// finally, if the layer is skipped then we'll skip the tasks too
	if len(toSkip) > 0 {
		_, err := c.db.SetTasksStatus(structs.SKIPPED, utils.NewRandomID(), toSkip)
		errchan <- err
	}
}

func (c *Service) queueTidyTaskWork(errchan chan<- error, taskWork chan<- []*structs.Task) {
	q := &structs.Query{Limit: defLimitQueueTidyTaskWork, Offset: 0, Statuses: []structs.Status{structs.KILLED, structs.READY, structs.PENDING}}
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
	q := &structs.Query{Limit: defLimitQueueTidyJobWork, Offset: 0, Statuses: incompleteStates}
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

func (c *Service) skipLayerTasks(in []*structs.ObjectRef) error {
	if in == nil || len(in) == 0 {
		return nil
	}
	q := &structs.Query{
		Limit:    defLimitSkipLayerTasks,
		Offset:   0,
		LayerIDs: []string{},
		Statuses: incompleteStates,
	}
	for _, r := range in {
		q.LayerIDs = append(q.LayerIDs, r.ID)
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
