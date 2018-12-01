
class UnknownMessage(Exception):
    """The given message type is unknown: unsure what to do"""
    pass


class MalformedMessage(Exception):
    """The given message is malformed"""
    pass


class DuplicateMessage(Exception):
    """Message has been seen before
    """
    pass
