import socket
import sys
import os
import logging
import random


_LIST_ADJ = [
    "sleepy",
    "silent",
    "ambitious",
    "speedy",
    "immense",
    "teeny",
    "malicious",
    "angelic",
    "prancing",
    "preening",
    "quiet",
    "sneaky",
]
_LIST_THINGS = [
    "lion",
    "tiger",
    "dove",
    "sparrow",
    "hawk",
    "cat",
    "mouse",
    "buffalo",
    "tadpole",
]


_logger = None
_ENV_DEFAULT_USERS = 'IGOR_DEFAULT_ADMIN_USERS'


def default_admin_users() -> list:
    """Read IGOR_DEFAULT_ADMIN_USERS env var for admin user(s) to create

    Nb. This nullifies the env var before returning.

    :return: list

    """
    raw_line = os.environ.get(_ENV_DEFAULT_USERS)
    if not raw_line:
        return []

    users = []
    for name_pass_tuple in raw_line.split(","):
        if name_pass_tuple.count(":") != 1:
            continue

        name, password = name_pass_tuple.split(":")
        users.append((name, password))

    os.environ[_ENV_DEFAULT_USERS] = ''  # stomp over the value now that we've started

    return users


def logger(name="igor", filename=None, logpath="/tmp"):
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
            "%(asctime)s | %(threadName)-12.12s | %(levelname)-5.5s | %(message)s"
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


def hostname() -> str:
    """Return name (or addr) of host.

    :return: str

    """
    return socket.gethostbyname(socket.getfqdn())


def random_name_worker(suffix=None) -> str:
    """Return random garbage name for a worker.

    :return: str

    """
    adj = random.choice(_LIST_ADJ)
    thing = random.choice(_LIST_THINGS)

    if not suffix:
        suffix = random.randint(0, 99)

    return f'{adj}-{thing}-{suffix}'
