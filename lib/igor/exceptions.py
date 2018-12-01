
class WriteFailError(Exception):
    """ A generic "we failed to write to the db"
    """
    pass


class IllegalOp(Exception):
    """An error that implies the operation is not permitted.
    """
    pass


class ChildExists(IllegalOp):
    """The operation cannot be performed as a child object(s) exists.
    """
    pass


class InvalidArg(Exception):
    """Represents an error stemming from some setting mismatch or invalid arg / state.
    """
    pass


class NotFound(InvalidArg):
    """A required obj indicated by an ID was not found
    """
    pass


class JobNotFound(NotFound):
    """A Job was not found
    """
    pass


class LayerNotFound(NotFound):
    """A Layer was not found
    """
    pass


class TaskNotFound(NotFound):
    """A Task was not found
    """
    pass


class WorkerNotFound(NotFound):
    """A Worker was not found
    """
    pass


class UserNotFound(NotFound):
    """A User was not found
    """
    pass


class InvalidState(IllegalOp):
    """Generic "the object is in the wrong state to do the thing"
    """
    pass


class WorkerMismatch(InvalidState):
    """Another worker is doing this task & the given worker cannot update it.
    """
    pass
