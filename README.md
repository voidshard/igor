# Igor

Open source distributed workflow system. 

* [Before You Start](#before-you-start)
* [Usage](#usage)
* [Concepts](#concepts)
* [API](#API)
* [Building and Requirements](#building-and-requirements)


## Before You Start

##### What

Igor is a task queuing system, similar to [Asynq](https://github.com/hibiken/asynq) or [Tasqueue](https://github.com/kalbhor/Tasqueue) but building on top of these a straight forward workflow system & API for batch processing.


##### Why

I know I know. We already have task queues, hell I just mentioned two of them. Why do we need another? Quite simply, the design goals of Igor are quite different to the aforementioned systems. In fact Igor is built on top of [existing task queues](/pkg/queue/asynq.go).

If you're after realtime task execution and / or don't need workflows for some embarrassingly parallel problem: Igor isn't for you. If however you require more order, easily inspectable workflows that might need to stop & start or dynamically expand: read on!


## Usage

As in [other](https://github.com/hibiken/asynq) [systems](https://github.com/kalbhor/Tasqueue) you register some process to perform work on your desired tasks like so
```golang
import (
	"github.com/voidshard/igor/pkg/api"
	"github.com/voidshard/igor/pkg/database"
	"github.com/voidshard/igor/pkg/queue"
)

func main() {
	service, _ := api.New(&database.Options{URL: x}, &queue.Options{URL: y}, api.OptionsClientDefault())
    
    service.Register("mytask", func(work []*queue.Meta) error {
        // do something
    })

    service.Run()
}
```
Note that in Igor you register a handler to accept multiple tasks at a time, this is to allow the queue to perform task batching / aggregation if the queue supports it & is configured to do so.

The work [meta object](/pkg/queue/meta.go) passed in contains the [task object](/pkg/structs/task.go) and allows the user to set the tasks' status & an optional message. If your task handler doesn't explicitly set a status, the task will be set to completed by default.


No surprises so far, now let's kick off some tasks
```golang
import (
	"github.com/voidshard/igor/pkg/api"
	"github.com/voidshard/igor/pkg/database"
	"github.com/voidshard/igor/pkg/queue"
	"github.com/voidshard/igor/pkg/structs"
)

func main() {
	service, _ := api.New(&database.Options{URL: x}, &queue.Options{URL: y}, api.OptionsClientDefault())

    // kick off a job
    service.CreateJob(&structs.CreateJobRequest{
        JobSpec: structs.JobSpec{Name: "my-job"},
        Layers: []structs.JobLayerRequest{
            {
                LayerSpec: structs.LayerSpec{Name: "layer-01", Priority: 0},
                Tasks: []structs.TaskSpec{
                    {Type: "mytask", Name: "task-01", Args: []byte("hello world")},
                    // more tasks
                },
            },
            // more layers
        },
    })

    // or add task(s) to existing layer(s)
    service.CreateTasks([]*structs.CreateTaskRequest{
        {LayerID: existing_layer_id, TaskSpec: structs.TaskSpec{Type: "mytask", ..}},
        // more tasks
    })
}
```
Note we don't explicitly 'enqueue' here, Igor takes care of calling the queue & pushing tasks to it as needed. We could also talk to an API server rather than using Igor's libs directly.

Examples can be found in the test suite including infra with [docker compose](/tests/compose.yaml), a [dummy worker](/tests/dummy_worker.go) and some [example calls](/tests/end_to_end_test.go) over HTTP.


## Concepts

###### Terminology

With a basic example out of the way, let's dive a bit more into terms & concepts.

* [Job](/pkg/structs/job.go)
    
    What might be called a 'workflow'. Essentially a collection of *Layers*

* [Layer](/pkg/structs/layer.go)
    
    A Layer is a collection of tasks that belong to a Job. Layers have an 'priority' value, those with the same priority run at the same time, higher layers run only after previous layers have completed or are explicitly skipped (by a human). Tasks within a layer run in parallel.

* [Task](/pkg/structs/task.go)
    
    A task is a single unit of work, and belongs to a layer.


###### Workflows as first class objects

Tasks cannot exist outside of a job. The API supports creating a job with layers & tasks up front, and adding tasks to existing layers (before the layer begins processing).

Other systems allow you to define tasks with support for chains & groups as an afterthought. Igor is built entirely the other way around. And when I say *entirely* I mean it: you can define layers *without* tasks if you so wish. You can also create layers & tasks paused if you wished to, say, add some unknown or unbounded number of tasks before beginning processing.


###### Visibility, tracking all the things

To explain by a counter example: In systems where workflows are secondary concerns it may not be possible to fetch the status of tasks not yet launched. That is, often these systems are implemented so that task A fires off task B when it completes in a sort of daisy-chain approach. Such a system has no way of knowing about task B (other than perhaps task A has a post-run instruction) before it completes task A. From this point, task B is created and can be looked up.

In Igor the status of the entire workflow is known from the begining - whether it has run, will run or is currently running (or even rerunning).


###### Architecture 

Igor has a few working parts & interfaces are used everywhere so sections can be replaced or expanded on.

* [Database](/pkg/database/interface.go)

  Where Igor stores job, layer and task data. Postgres is set up to partition tables into sections by created_at.
  Each partition in postgres is for a single day, this is to make long term maintainence reasonably painless.
  Igor does not archive / drop old partitions, this is left up to you to decide on.

* [Queue](/pkg/queue/interface.go)

  What Igor uses to actually run tasks. Igor handles telling the queue *when* to run something and leaves the 
  queue to work out *how* to run it. Igor also leaves other details like retrying & batching / aggregation to the queue too.
  The queue currently used is [asynq](https://github.com/hibiken/asynq) which supports [aggregation!](https://github.com/hibiken/asynq/wiki/Task-aggregation) (as all task queues should *ahem*).

* [API](/pkg/api/interface.go)

  The functionality provided by Igor to external clients. For those wishing to keep things in native Go you'll probably want the [api service](/pkg/api/service.go) with provides the ability to Register() handlers with the underlying queue.

* [HTTP](/pkg/api/http/)

  Is the Igor API served over HTTP, the package includes both server and client. Other mechanisms might be added in future (ie. gRPC).

* [API Server](/cmd/apiserver/main.go)

  Binary for serving API requests.

* [Worker](/cmd/worker/main.go)

  Binary that performs the internal logic of Igor itself. API Server(s) and Worker(s) are divided so they can be scaled up / down in isolation from each other. For demo purposes / small installations you could roll these together if desired.


## API

In short, you can create a Job by
* POST /api/v1/jobs/
```json
{
    "name": "my_job",
    "layers": [
        {
            "priority": 0,
            "name": "first_layer",
            "tasks": [
                {
                    "name": "task_0",
                    "type" "sleep"
                }
            ]
        },
        {
            "priority": 10,
            "name": "second_layer",
            "tasks": []
        }
    ]
}
```
You must define all layers that you want upfront. Igor doesn't mind it if it goes to run a layer and finds there are no tasks. It just considers it "complete" immediately :)

You can add tasks to an already existing layer with 
- POST /api/v1/layers
```json
[{
    "layer_id": "id-of-layer-to-add-to",
    "type": "sleep",
    "name": "hello!"
}]
```
Once the layer is running you can no longer do this.

You can kill, pause, unpause and retry tasks at the corresponding API endpoints via sending a PATCH like
- PATCH /api/v1/pause
```
[{
    "kind": "Task",
    "id": "task-id",
    "etag": "task-etag"
}]
```
The response here contains the number of objects successfully marked paused (or whatever the operation is). It's worth checking this as Igor uses [optimistic locking](https://en.wikipedia.org/wiki/Optimistic_concurrency_control) for all updates and ignores operations on out-of-date etags.

Check out the [HTTP server](/pkg/api/http/service/api.go), [client](/pkg/api/http/client/client.go) and [endpoints](/pkg/api/http/common/constants.go) for more details.


## Building and Requirements

The project is written in Go and uses Postgres as it's primary database.

In addition Igor sits on top of a task queue; technically we can support [any queue](/pkg/queue/interface.go) but currently we use [Asynq](https://github.com/hibiken/asynq), so a Redis is required for that.

Just once, you'll need to setup postgres. This makes a schema, tables, a read-only user, a read-write user & some triggers
```bash
./cmd/db_migrate/run.sh 
```
This script uses envsubst to sub in some values you might want to override (default passwords?).

There is a PyQt5 UI which requires in addition 
```bash
pip install pyqt5 requests
```
that you can then launch with 
```bash
./ui/start.py
```
Fair warning this UI is ported from the previous version of Igor and it may be replaced with a web UI at some point.


## Glossary

Terms you'll see thrown about.

* Pending ([status](/pkg/structs/status.go))

    The job/layer/task has been created, but has yet to run.
    
* Queued ([status](/pkg/structs/status.go))

    Igor has scheduled the task to run.

* Running ([status](/pkg/structs/status.go))
    
    The job/layer/task is currently running.
    
* Completed ([status](/pkg/structs/status.go))
   
    The job/layer/task has completed.
    
* Errored ([status](/pkg/structs/status.go))
    
    The job/layer/task is errored, or contains errors, Igor will not continue without human intervention.

* Skipped ([status](/pkg/structs/status.go))

    An explicitly user set state that Igor regards as "completed" (ie. it does not block following tasks / layers).

* Paused (action)
    
    Tells Igor not to schedule the paused layer/task. This does *not* make Igor stop an already running task. 
    If you want to pause *and* stop a task you'll need to follow this up with a kill.
    Objects in Igor keep 'paused' as a 'paused_at' unix timestamp, with a value > 0 considered "paused"

* Kill (action)

    A killed task is set to errored, currently running task(s) are stopped if possible.

* Retry (action)

    Kill & explicitly enqueue a task for reprocessing.


## TODO

- Add SSL configuration options
- Complete & expand on code comments


## Notes

- Feel free to push up bugs / features.
