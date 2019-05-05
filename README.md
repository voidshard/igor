# Igor

Open source distributed workflow system. 

* [Before You Start](#before-you-start)
* [Building and Requirements](#building-and-requirements)
* [Concepts](#concepts)
* [Examples](#examples)
* [UI](#ui)

## Before You Start

##### What

Igor is a simple workflow running system, similar to Celery or TaskTiger in that it 
allows one to spin up some number of workers and farm tasks out to them to perform.

##### Why

I know I know. We already have task launching systems in python. 
Hell I just mentioned two of them. Why do we need another? 
Quite simply, the design goals of Igor are quite different to the aforementioned systems. Very, very different. 

In short Igor is *all* about workflows, visibility, live workflow modification and easy integration 
... and not singular rapid fire and / or realtime tasks. 

If you're after realtime task execution and / or don't need workflows at all: Igor isn't for you. 


## Building and Requirements

The project is python3.

At the moment the only dependencies are:
```bash
pip install psycopg2 redis psutil Flask pyopenssl
``` 

If you're planning on running the test suite you'll need docker too. Docker & docker compose files are included. 

## Concepts

###### Terminology

Some terminology to get us started!

First class lives-in-the-database honest-to-goodness objects:

* Job
    
    What might be called a 'workflow'. Essentially a collection of *Layers*

* Layer
    
    A Layer is a collection of tasks that belong to a Job.
    Layers have an 'order' value and are formed into a tree when a Job is 
    created. That is, layers with the same 'order' run at the same time, and higher ordered
    layers run only after previous layers have completed or are explicitly skipped (by a human).

* Task
    
    A task is a single OS command to be run, and belongs to a layer. It includes
    - custom environment data to set before running (the igor Job, Layer and Task IDs are automatically
      included in the task environment before launch)
    - space for a task result that can be set by the user
    - records about retries / attempts / statistics etc

* Worker

    A daemon process that is spun up on some host to run task(s). Daemons in Igor spin up tasks
    by running them as full blown OS commands. This means you never need to restart daemons
    to register more tasks on them or when you alter code that the tasks run. Worker daemons 
    regularly ping home so the system knows they're alive. Igor automatically recovers tasks 
    from daemons that become unresponsive.

* User

    In addition to this Igor has it's own concept of a user. Each Job, Layer and Task has a user
    id associated with it. Users that aren't admins can only see and modify their own objects, 
    and can't see workers. Admins can see & modify anything.


There are other objects that are used to talk to the API, for searching and what not:

* Query

    A query is a simple list of filters with some global settings (limit, offset, user etc).
    These are sent to Igor in order to ..well.. perform searches. No surprises there.

* Filter
    
    A filter is an instruction as to what to match. A query will return any result(s) that 
    match any of the given filters.


Ok now that you have the basic objects in mind, onward with Igor concepts! 


###### Workflows as first class objects

You cannot define tasks outside of a job. Period.

Other systems allow you to define tasks with support for chains, groups and sets of tasks 
as an afterthought. Igor is built entirely the other way around. And when I say *entirely* I 
mean it: you can define layers *without* tasks if you so wish. 

Why might you want to do this? We'll get to that later...

###### Visibility, tracking all the things

The state of every task, layer, job and worker is tracked in Igor, right down to process IDs
and memory statistics. It doesn't matter if a task has been submitted to the queue or not, 
whether it wont run in the next 10 years, whether it is running right now, has been paused, 
retried or errored. It's visible from the API (assuming you have the right to see it).

###### Live modification

* Layer expansion at runtime 

    You can create tasks in any layer up until it begins running, even while the parent job 
    is running. It's a snap to launch a two layer job, where the first layer adds tasks to 
    the second. Don't know how many tasks you're going to need exactly? Not a problem.

* Pause and Unpause

    Pause and unpause any task, layer or job. This don't stop currently running tasks, but 
    nothing paused will be picked up and processed.

* Kill and retry

    If pausing isn't your thing you can order Igor to kill (SIGABORT followed by SIGKILL
    if the process doesn't respond within some grace time) whatever whenever you feel like it.
    You can also retry - kill and then remark task(s) as pending (ie to-be-run).
    If not told otherwise, Igor will retry any task 3 times before deciding that perhaps the 
    task is hopeless. (Idempotency is still important people!).
    You can of course, continue telling Igor to retry such a task until you're blue in the face.

* Task results

   Task results are stored alongside task objects in Igor. A simple API allows you to get or 
   set the result of any task(s) you want. Igor doesn't try to automatically pass your task results 
   to or from tasks, that's left for you to do if you want/need to. 

* Task environment

   The environment of every task is also stored alongside task objects in Igor. Again the API
   allows you to modify this whenever you feel like it, not that this will affect
   already running tasks. These are optional user set environment variables, in addition the 
   default env vars Igor sets.


###### Easy integration


* Transport

    The current transport system is simple JSON over HTTPS. Mostly because it's simple and
    everything can use it with minimal effort. But there's no reason Igor can't support
    more transport mechanisms and run them all simultaneously.

* Any language
    
    If your task language of choice can read/write JSON and do HTTPS then it can talk to Igor.
    Even without talking to Igor, if it can be launched with an OS command it's fair game.

* Interfaces

    The database, api transport, work scheduler and task runners are all behind interfaces.
    You could run TaskTiger inside of Igor to manage the running the tasks with Nats.io to ship 
    data back and fourth. Whatever. Write an implementation and make a PR.


## UI

Igor includes a PyQt5 UI that gives visibility into the system and running objects.
It also gives the ability to pause/unpause kill/retry things in Igor.

Obviously, you'll need PyQt5 installed to run it. Checkout the /bin folder.


## Examples

In short, you can create a Job by
* POST /v1/jobs/
```json
{
    "name": "my_job",
    "layers": [
        {
            "order": 0,
            "name": "first_layer",
            "tasks": [
                {
                    "name": "sleep",
                    "cmd": ["sleep", "60"]
                }
            ]
        },
        {
            "order": 10,
            "name": "second_layer",
            "tasks": []
        }
    ]
}
```
You must define all layers that you want upfront. Igor doesn't mind it if it goes to run
a layer and finds there are no tasks. It just considers it "complete" immediately :)

You can add a task to an already existing layer with 
- POST /v1/layers/[layer_id]/tasks
```json
{"name": "a_new_task", "cmd": ["sleep", "10"]}
```
Once the layer is running - or just about to run - you can no longer do this.

For more info check out 
 - /tests/example_jobs for more examples
 - /lib/pyigor for a more complete simple http client 
 - /lib/igor/api/domain contains definitions for objects accepted over transport 


## Status

Igor is functional and reasonably tested. I'd like to add *more* tests before advising 
folks to run it in anger, but it currently functions well enough for my own use at home.
More tests & refinements to come as issues crop up. 

Feel free to push up bugs / features.


