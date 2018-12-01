"""A 'runner' is any task runner engine that can fulfill the base contract in
igor.runner.base.Base.

Essentially it's responsible for
 - maintaining a (priority ordered) queue of tasks that need to be done
 - sending tasks to workers (highest priority first)
 - monitoring the tasks & updating the system (keep alive ping w/ task statistics)
 - being able to kill (stop) running tasks via job, layer or task id(s)
 - being able to pause a worker (not allow it to pick up more work); and unpause it
 - updating the system on tasks that are done - whether errored or completed.

"""

from igor.runner.default_impl import DefaultRunner


__all__ = [
    "DefaultRunner",
]
