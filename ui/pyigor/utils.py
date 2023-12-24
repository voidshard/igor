import os
import sys
import logging


_logger = None


def logger(name="pyigor", filename=None, logpath="/tmp"):
    """Build a new logger.

    :param name:
    :param filename:
    :param logpath:

    """
    global _logger

    if not _logger:
        if not filename:
            filename = name + ".log"

        fmt = logging.Formatter(
            "%(asctime)s | %(levelname)-5.5s | %(message)s"
        )
        fullpath = os.path.join(logpath, filename)

        _logger = logging.getLogger(name)

        fh = logging.FileHandler(fullpath)
        fh.setFormatter(fmt)
        _logger.addHandler(fh)

        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(fmt)
        _logger.addHandler(sh)

        _logger.setLevel(logging.DEBUG)

        _logger.info(f"logpath: {fullpath}")

    return _logger
