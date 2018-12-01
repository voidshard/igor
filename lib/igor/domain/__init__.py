"""The igor domain objects are pretty dumb straight forward data containers.

Job:
Essentially a container for related layers.

Layer:
A layer represents a set of tasks that should (or at least could) be run in parallel.
A layer is sent to the system with an 'order' number which helps us establish which layers
are before, after or at the same time as a given layer.

Task:
The most basic unit, a task is simply a cmd with some args & environment data that will be set
when executing it.

Worker:
A worker is some daemon process somewhere that connects in and registers itself to perform work
for us. How a worker actually works is defined in the 'runner' module, but here we define what
data they need supply the system.

Query & Filter:
Rather than expose the db layer & concepts to the rest of the world the required logic is built
into the Query & Filter objects. All 'get_<object>' functions in the db module accept these Query
objects. We will also expose these over our API, although we may place limits on them around
fetching, sorting and deleting ..

"""

from igor.domain.job import Job
from igor.domain.layer import Layer
from igor.domain.task import Task
from igor.domain.worker import Worker
from igor.domain.user import User
from igor.domain.query import Query, Filter


__all__ = [
    "Job",
    "Layer",
    "Task",
    "Worker",
    "Query",
    "Filter",
    "User",
]
