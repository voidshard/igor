package api

import (
	"fmt"
	"sort"
	"time"

	"github.com/voidshard/igor/internal/utils"
	"github.com/voidshard/igor/pkg/errors"
	"github.com/voidshard/igor/pkg/structs"
)

var (
	// timeNow is a function that returns the current time in unix seconds.
	// Set this to a mock function for testing.
	timeNow = func() int64 { return time.Now().Unix() }
)

// determineJobStatus takes a list of layers and returns the status of the job
// as well as the next layers that should be run (if any).
func determineJobStatus(layers []*structs.Layer) (structs.Status, []*structs.Layer) {
	sort.Slice(layers, func(i, j int) bool {
		return layers[i].Priority < layers[j].Priority
	})

	// finds the first layer that we can run (ie, all layers with lower priority are done)
	mustRunNext := []*structs.Layer{} // layers of the same priority that must run next
	canRun := []*structs.Layer{}      // layers we can & should set to running
	var lowest int64
	seenErrored := false
	for _, l := range layers {
		if l.Status == structs.SKIPPED || l.Status == structs.COMPLETED {
			continue
		}
		if len(mustRunNext) > 0 && l.Priority > lowest {
			// ie. if there's another layer that can run, whose priority is lower than this one
			// that layer must run first (and we cannot run this at the same time)
			break
		}
		// otherwise, if all previous layers haven't died (error/killed) and either
		// - we have no layers yet (nb. we're processing in priority priority)
		// - our priority is less than or equal to the lowest priority layer that can run
		// then this layer should run next
		lowest = l.Priority
		mustRunNext = append(mustRunNext, l)

		// record if we've seen an errored layer
		seenErrored = seenErrored || l.Status == structs.ERRORED

		if l.Status == structs.PENDING || l.Status == structs.QUEUED {
			canRun = append(canRun, l)
		}
	}

	if len(canRun) == 0 && seenErrored { // we have more work to do but cannot proceed
		return structs.ERRORED, canRun
	}
	if len(mustRunNext) > 0 {
		return structs.RUNNING, canRun
	}
	return structs.COMPLETED, canRun
}

// layerCanHaveMoreTasks returns true if the given layer can have more tasks.
func layerCanHaveMoreTasks(layer *structs.Layer) bool {
	if structs.IsFinalStatus(layer.Status) {
		return false
	}
	switch layer.Status {
	case structs.PENDING, structs.QUEUED:
		return true
	case structs.RUNNING:
		return layer.PausedAt > 0
	default:
		return false
	}
}

// validateToggles takes a list of object references and returns a map of kind -> object references.
// It also validates given inputs & spits out an error if something isn't aggreeable.
func validateToggles(in []*structs.ObjectRef) (map[structs.Kind][]*structs.ObjectRef, error) {
	out := map[structs.Kind][]*structs.ObjectRef{}
	if in == nil || len(in) == 0 {
		return out, nil
	}
	for _, t := range in {
		if !utils.IsValidID(t.ID) {
			return nil, fmt.Errorf("%w %s", errors.ErrInvalidArg, t.ID)
		}
		if !utils.IsValidID(t.ETag) {
			return nil, fmt.Errorf("%w %s", errors.ErrInvalidArg, t.ETag)
		}
		err := validateKind(t.Kind)
		if err != nil {
			return nil, err
		}
		currently, ok := out[t.Kind]
		if !ok {
			currently = []*structs.ObjectRef{}
		}
		out[t.Kind] = append(currently, &structs.ObjectRef{ID: t.ID, ETag: t.ETag, Kind: t.Kind})
	}
	return out, nil
}

// validateKind returns an error if the given kind is not valid.
func validateKind(k structs.Kind) error {
	switch k {
	case structs.KindJob, structs.KindLayer, structs.KindTask:
		return nil
	default:
		return fmt.Errorf("%w %s", errors.ErrInvalidArg, k)
	}
}

// buildJob takes a CreateJobRequest and returns a Job, Layers, Tasks and a map of LayerID -> Tasks.
//
// Implies that the input has been validated previously.
func buildJob(cjr *structs.CreateJobRequest) (*structs.Job, []*structs.Layer, []*structs.Task, map[string][]*structs.Task) {
	etag := utils.NewRandomID()
	job := &structs.Job{
		JobSpec: cjr.JobSpec,
		ID:      utils.NewRandomID(),
		Status:  structs.RUNNING,
		ETag:    etag,
	}

	lowestPriority := cjr.Layers[0].Priority
	for _, l := range cjr.Layers {
		if l.Priority < lowestPriority {
			lowestPriority = l.Priority
		}
	}

	layers := []*structs.Layer{}
	tasks := []*structs.Task{}
	tasks_by_layer := map[string][]*structs.Task{}
	for _, l := range cjr.Layers {
		layer := &structs.Layer{
			LayerSpec: l.LayerSpec,
			ID:        utils.NewRandomID(),
			JobID:     job.ID,
			Status:    structs.PENDING,
			ETag:      etag,
		}
		if layer.Priority == lowestPriority {
			layer.Status = structs.RUNNING
		}
		layers = append(layers, layer)

		by_layer := []*structs.Task{}
		for _, t := range l.Tasks {
			new_task := &structs.Task{
				TaskSpec: t,
				ID:       utils.NewRandomID(),
				JobID:    job.ID,
				LayerID:  layer.ID,
				Status:   structs.PENDING,
				ETag:     etag,
			}
			tasks = append(tasks, new_task)
			by_layer = append(by_layer, new_task)
		}
		tasks_by_layer[layer.ID] = by_layer
	}

	return job, layers, tasks, tasks_by_layer
}

// validateTaskSpec returns an error if the given task spec is invalid.
func validateTaskSpec(t *structs.TaskSpec) error {
	if len(t.Name) > maxNameLength {
		return fmt.Errorf("%w task name %s is %d chars, max %d", errors.ErrMaxExceeded, t.Name, len(t.Name), maxNameLength)
	}
	if t.Type == "" {
		return errors.ErrNoTaskType
	}
	if len(t.Type) > maxTypeLength {
		return fmt.Errorf("%w task type %s is %d chars, max %d", errors.ErrMaxExceeded, t.Type, len(t.Type), maxTypeLength)
	}
	if t.Args != nil && len(t.Args) > maxArgsLength {
		return fmt.Errorf("%w task args %s is %d chars, max %d", errors.ErrMaxExceeded, t.Args, len(t.Args), maxArgsLength)
	}
	return nil
}

// validateCreateJobRequest returns an error if the given job request is invalid.
func validateCreateJobRequest(cjr *structs.CreateJobRequest) error {
	if cjr.Layers == nil || len(cjr.Layers) == 0 {
		return errors.ErrNoLayers
	}
	if len(cjr.Name) > maxNameLength {
		return fmt.Errorf("%w job name %s is %d chars, max %d", errors.ErrMaxExceeded, cjr.Name, len(cjr.Name), maxNameLength)
	}
	for _, l := range cjr.Layers {
		if len(l.Name) > maxNameLength {
			return fmt.Errorf("%w layer name %s is %d chars, max %d", errors.ErrMaxExceeded, l.Name, len(l.Name), maxNameLength)
		}
		if l.Tasks == nil || len(l.Tasks) == 0 {
			continue
		}
		for _, t := range l.Tasks {
			err := validateTaskSpec(&t)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
