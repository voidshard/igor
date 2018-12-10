import time


def human_time(some_input) -> str:
    """

    :param some_input:

    """
    if some_input is None:
        return None

    try:
        return time.ctime(int(some_input))
    except Exception as e:
        pass

    return None
