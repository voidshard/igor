import os
import sys

from copy import copy

import configparser


_DEFAULT_CONFIG_ENV = 'IGOR_CONFIG'
_DEFAULT_CONFIG_NAME = 'igor.ini'
_DEFAULT_CONFIG_DIR = 'igor'

_default_config = {
    'database': {
        # mongo settings for MongoDB database
        'host': 'localhost',
        'port': 27017,
    },
    'runner': {
        # redis settings for default runner
        'host': 'localhost',
        'port': 6379,
    },
    'gateway': {
        'port': 8080,
    },
}


def read_default_config() -> dict:
    """We read the first one of

        ${IGOR_CONFIG}
        igor.ini
        ~/.config/igor/igor.ini
        ~/.igor/igor.ini
        /etc/igor/igor.ini

    :return: dict

    """
    for p in [
        os.getenv(_DEFAULT_CONFIG_ENV),
        _DEFAULT_CONFIG_NAME,
        os.path.join("~/.config", _DEFAULT_CONFIG_DIR, _DEFAULT_CONFIG_NAME),
        os.path.join(f"~/.{_DEFAULT_CONFIG_DIR}", _DEFAULT_CONFIG_NAME),
        os.path.join("/etc", _DEFAULT_CONFIG_DIR, _DEFAULT_CONFIG_NAME),
    ]:

        if p and os.path.isfile(p):
            try:
                return read_config_file(p)
            except Exception as e:
                print(f"failed to read {p}: {e}", file=sys.stderr)

    # we didn't find anything (or couldn't read), so we'll fallback on the default setup
    return copy(_default_config)


def read_config_file(configpath: str) -> dict:
    """Read in config file & return as dict

    :return: dict

    """
    config = configparser.ConfigParser()
    config.read(configpath)

    data = {}
    for section in config.sections():
        data[section.lower()] = {}
        for opt in config.options(section):
            data[section.lower()][opt.lower()] = config.get(section, opt)
    return data
