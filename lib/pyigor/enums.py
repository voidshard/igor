import enum


class State(enum.Enum):
    PENDING = "PENDING"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    ERRORED = "ERRORED"
    SKIPPED = "SKIPPED"


ALL = [
    State.PENDING.value,
    State.QUEUED.value,
    State.RUNNING.value,
    State.COMPLETED.value,
    State.ERRORED.value,
    State.SKIPPED.value,
]

ALL_NOT_COMPLETE = [
    State.PENDING.value,
    State.QUEUED.value,
    State.RUNNING.value,
    State.ERRORED.value,
    State.SKIPPED.value,
]
