

class NotFoundError(Exception):
    """Thing wasn't found on server.
    """
    pass


class ServerError(Exception):
    """??
    """
    pass


class AuthError(Exception):
    """The action / request was denied
    """
    pass


class MalformedRequestError(Exception):
    """The action / request was malformed when it reached the server.
    """
    pass


class EtagMismatchError(Exception):
    """Write action denied because the object was altered whilst performing the request.
    """
    pass
