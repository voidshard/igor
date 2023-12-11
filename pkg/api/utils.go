package api

import (
	"fmt"
	"sort"

	"github.com/voidshard/igor/internal/utils"
	"github.com/voidshard/igor/pkg/errors"
	"github.com/voidshard/igor/pkg/structs"
)

func determineJobStatus(layers []*structs.Layer) (structs.Status, []*structs.Layer) {
	sort.Slice(layers, func(i, j int) bool {
		return layers[i].Priority < layers[j].Priority
	})

	// finds the first layer that we can run (ie, all layers with lower priority are done)
	mustRunNext := []*structs.Layer{} // layers of the same priority that must run next
	canRun := []*structs.Layer{}      // layers we can & should set to running
	var lowest int64
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
		// - we have no layers yet (nb. we're processing in priority order)
		// - our priority is less than or equal to the lowest priority layer that can run
		// then this layer should run next
		lowest = l.Priority
		mustRunNext = append(mustRunNext, l)

		if l.Status == structs.PENDING || l.Status == structs.QUEUED || l.Status == structs.READY {
			canRun = append(canRun, l)
		}
	}

	if len(mustRunNext) > 0 {
		return structs.RUNNING, canRun
	}
	return structs.COMPLETED, canRun
}

func layerCanHaveMoreTasks(layer *structs.Layer) bool {
	switch layer.Status {
	case structs.PENDING, structs.QUEUED, structs.READY:
		return true
	case structs.RUNNING:
		return layer.PausedAt > 0
	default:
		return false
	}
}

func validateToggles(in []*structs.ObjectRef) (map[structs.Kind][]*structs.ObjectRef, error) {
	out := map[structs.Kind][]*structs.ObjectRef{}
	for _, t := range in {
		if !utils.IsValidID(t.ID) {
			return nil, fmt.Errorf("%w %s", errors.ErrInvalidArg, t.ID)
		}
		if !utils.IsValidID(t.ETag) {
			return nil, fmt.Errorf("%w %s", errors.ErrInvalidArg, t.ETag)
		}
		k := structs.Kind(t.Kind)
		err := validateKind(k)
		if err != nil {
			return nil, err
		}
		currently, ok := out[k]
		if !ok {
			currently = []*structs.ObjectRef{}
		}
		out[k] = append(currently, &structs.ObjectRef{ID: t.ID, ETag: t.ETag})
	}
	return out, nil
}

func validateKind(k structs.Kind) error {
	switch k {
	case structs.KindJob, structs.KindLayer, structs.KindTask:
		return nil
	default:
		return fmt.Errorf("%w %s", errors.ErrInvalidArg, k)
	}
}

func buildJob(cjr *structs.CreateJobRequest) (*structs.Job, []*structs.Layer, []*structs.Task, map[string][]*structs.Task) {
	etag := utils.NewRandomID()
	job := &structs.Job{
		JobSpec: cjr.JobSpec,
		ID:      utils.NewRandomID(),
		Status:  structs.RUNNING,
		ETag:    etag,
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
		if layer.Priority <= 0 {
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
			if layer.Status == structs.RUNNING && layer.PausedAt == 0 {
				new_task.Status = structs.READY
			}
			tasks = append(tasks, new_task)
			by_layer = append(by_layer, new_task)
		}
		tasks_by_layer[layer.ID] = by_layer
	}

	return job, layers, tasks, tasks_by_layer
}

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
