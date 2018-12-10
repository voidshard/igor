
class Forbidden(Exception):
    """Action not permitted due to permissions.
    """
    pass


class InvalidState(Exception):
    """Action not permitted because it's .. well .. weird.
    """
    pass


class Unauthorized(Exception):
    """User needs to log in / give creds etc
    """
    pass


class InvalidSpec(Exception):
    """Spec incorrect
    """
    pass


class WriteConflict(Exception):
    """You need to give the correct etag
    """
    pass
