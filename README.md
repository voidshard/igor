# Igor

Open source distributed workflow system. 

* [Before You Start](#before-you-start)
* [Building and Requirements](#building-and-requirements)
* [Concepts](#concepts)
* [Examples](#examples)
* [UI](#ui)

## Before You Start

##### What

Igor is a simple workflow running system, similar to Celery or TaskTiger in that it allows one to spin up some number of workers and farm tasks out to them to perform.

##### Why

I know I know. We already have task queues, hell I just mentioned two of them. Why do we need another? Quite simply, the design goals of Igor are quite different to the aforementioned systems.

In fact Igor is built on top of existing task queues, adding visibility, editable workflows & an API.

If you're after realtime task execution and / or don't need workflows at all: Igor isn't for you.


## Building and Requirements

The project is written in Go and uses Postgres as it's primary database.

In addition Igor sits on top of a task queue; technically we can support any queue but currently we use [Asynq](https://github.com/hibiken/asynq), so a Redis is required for that.

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
At some point I might re-write the UI as a small webserver, but we'll see.


## Concepts

###### Terminology

Some terminology to get us started!

* Job
    
    What might be called a 'workflow'. Essentially a collection of *Layers*

* Layer
    
    A Layer is a collection of tasks that belong to a Job. Layers have an 'priority' value, those with the same priority run at the same time, higher layers run only after previous layers have completed or are explicitly skipped (by a human). Tasks within a layer run in parallel.

* Task
    
    A task is a single unit of work, and belongs to a layer.


Ok now that you have the basic objects in mind, onward with Igor concepts! 


###### Workflows as first class objects

Tasks cannot exist outside of a job. The API supports creating a job with layers & tasks up front, and adding tasks to existing layers (before the layer begins processing).

Other systems allow you to define tasks with support for chains, groups and sets of tasks as an afterthought. Igor is built entirely the other way around. And when I say *entirely* I mean it: you can define layers *without* tasks if you so wish. 

Why might you want to do this? We'll get to that later...


###### Visibility, tracking all the things

To explain by a counter example: In systems where workflows are secondary concerns it may not be possible to fetch the status of tasks not yet launched. That is, often these systems are implemented so that task A fires off task B when it completes in a sort of daisy-chain approach. Such a system has no way of knowing about task B (other than that, perhaps, task A has a post-run instruction) before it completes task A. From this point, task B is created and can be looked up. 

In Igor the status of the entire workflow is known from the begining - whether it has run, will run or is currently running (or even rerunning).


###### Live modification

* Layer expansion at runtime 

    You can create tasks in any layer up until it begins running, even while the parent job 
    is running. It's a snap to launch a two layer job, where the first layer adds tasks to 
    the second. Don't know how many tasks you're going to need exactly? Not a problem.

* Pause and Unpause

    Pause and unpause any task or layer. This don't stop currently running tasks, but 
    nothing paused will be picked up and processed. You can even create things paused if you want.

* Kill and retry

    If pausing isn't your thing you can order Igor to kill tasks whenever you feel like it.
    You can also retry - kill and then remark task(s) as pending (ie to-be-run).
    You can of course, continue telling Igor to retry such a task until you're blue in the face.


###### Architecture 

Igor has a few working parts & interfaces are used so sections can be replaced or expanded on.

* [Database](/pkg/database/interface.go)

  Where Igor stores job, layer and task data. Postgres is set up to partition tables into sections via created_at.
  Each partition in postgres is for a single day, this is to make long term maintainence painless.
  Igor does not archive / drop old partitions, this is left up to you to decide on.

* [Queue](/pkg/queue/interface.go)

  What Igor uses to actually run tasks. Igor handles telling the queue *when* to run something and leaves the 
  queue to work out *how* to run it. Igor also leaves other details like retrying to the queue too (since it's a common built-in feature).
  The queue currently used is [asynq](https://github.com/hibiken/asynq) which supports [aggregation!](https://github.com/hibiken/asynq/wiki/Task-aggregation)

* [API](/pkg/api/interface.go)

  The functionality provided by Igor to external clients. For those wishing to keep things in native Go you'll probably want the [api service](/pkg/api/service.go) with provides the ability to Register() tasks with the underlying queue. You could also register outside of Igor, I wont judge you.

* [HTTP](/pkg/api/http/)

  Is the Igor API served over HTTP, the package includes both server and client. Other mechanisms might be added in future.

* [API Server](/cmd/apiserver/main.go)

  Binary that serves API requests.

* [Worker](/cmd/worker/main.go)

  Binary that performs the internal logic of Igor itself. API Server(s) and Worker(s) are divided so they can be scaled up / down
  in isolation from each other. For demo purposes / small installations you could roll these together if desired.


## UI

Igor includes a PyQt5 UI that gives visibility into the system and running objects. It also gives the ability to pause/unpause kill/retry things in Igor. Checkout the [/ui folder](/ui).


## Examples

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

You can add a task to an already existing layer with 
- POST /v1/layers/[layer_id]/tasks
```json
[{
    "layer_id": "id-of-layer-to-add-to",
    "type": "sleep",
    "name": "hello!"
}]
```
Once the layer is running you can no longer do this.

For more info check out /tests/data for more examples.


## Glossary

Terms you'll see thrown about.

* Pending (state)

    The job/layer/task has been created, but has yet to run.
    
* Queued (state)

    Igor has scheduled the layer/task to run.

* Running (state)
    
    The job/layer/task is currently running.
    
* Completed (state)
   
    The job/layer/task has completed.
    
* Errored (state)
    
    The job/layer/task is errored, or contains errors. Igor will reschedule tasks that have
    not yet been retried more than their maximum number of attempts.

* Skipped (state)

    A user set state that Igor regards as "completed" (ie. Igor will not run it, and will kick 
    off following task(s)).

* Paused (flag)
    
    Tells Igor not to schedule the paused job/layer/task. This does *not* make Igor stop a running task. 
    If you want to pause *and* stop something running, you're after 'pause' then 'kill' ... 

* Kill (action)

    A killed job/layer/task is set to errored, currently running task(s) are stopped. Task(s) can be 
    retried after being killed (but this counts against thier max attempts).

* Retry (action)

    A variant of kill, retry works on job/layer/task(s) that are *not* running and ignores a task(s) max attempts.


## Notes

Feel free to push up bugs / features.
