import enum


class LoggerType(enum.Enum):
    """What to do with stdout / stderr from running processes.

    """
    # the default is simply to stream log lines to some TCP socket over SSL to some address
    UDP = "UDP"

    # if all else fails, print to stdout *shrug*
    STDOUT = "STDOUT"


class TaskType(enum.Enum):
    CMD = "CMD"


class State(enum.Enum):
    # the obj has been created, but has not been queued to run yet
    PENDING = "PENDING"

    # the obj has been queued. Ie we've decided that it *can* now run, so it'll be run
    # as soon as a worker is found to process it
    QUEUED = "QUEUED"

    # the obj (or parts of it) are in the process of being worked on
    RUNNING = "RUNNING"

    # the obj has completed. In the case of a task it means it returned a 0 exit code.
    # In the case of a layer it means all tasks in the layer have either completed or have been
    # skipped.
    COMPLETED = "COMPLETED"

    # the obj is dead as a doornail. In the case of a task, it could (yet) be retried when the
    # scheduler process gets around to it. In the case of a layer this is essentially an end state
    # and means that the next layer(s) cannot be run (as their parent layers did not complete
    # successfully). A job / layer with this requires intervention.
    ERRORED = "ERRORED"

    # the obj has been earmarked not to be run. This is understood to be a 'success'. Ie, marking
    # a layer as 'skipped' implies that child layers SHOULD run.
    # The system never sets this state as part of it's normal operation.
    SKIPPED = "SKIPPED"
