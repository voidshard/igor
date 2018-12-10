import os
import sys

from copy import copy

import configparser


_DEFAULT_CONFIG_ENV = 'IGOR_UI_CONFIG'
_DEFAULT_CONFIG_NAME = 'igor.ui.ini'
_DEFAULT_CONFIG_DIR = 'igor'

_root_dir = os.path.dirname(
        os.path.dirname(
            os.path.dirname(
                os.path.abspath(__file__)
            )
        )
)
_etc_dir = os.path.join(_root_dir, "etc")

_default_config = {
    "client": {
        "url": "https://localhost:9025/v1/",
        "username": "admin",
        "password": "admin",
        "ssl_pem": os.path.join(_etc_dir, "ssl.pem"),
        "ssl_verify": False,
    }
}


def read_default_config() -> dict:
    """We read the first one of

        ${IGOR_UI_CONFIG}
        igor.ui.ini
        ~/.config/igor/igor.ui.ini
        ~/.igor/igor.ui.ini
        /etc/igor/igor.ui.ini

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
