import os


_root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_resource_dir = os.path.join(_root_dir, "resources")


def get(name):
    """Return path to file resource on disk

    :param name: resource name
    :return: str
    :raises IOError: if file not found
    """

    fpath = os.path.join(_resource_dir, name)
    if not os.path.isfile(fpath):
        raise IOError(f"Not found {fpath}")
    return fpath
