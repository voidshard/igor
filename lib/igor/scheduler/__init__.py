"""A scheduler is responsible for
- deciding when a layer is ready to run
- pushing tasks from ready layer(s) on the the queue
- deciding when a layer / job has completed (or errored)
- archiving old job data after jobs have completed.
- removing old workers & reclaiming their unfinished tasks for re-queuing.

"""

from igor.scheduler.simple_impl import Simple


__all__ = [
    "Simple",
]
