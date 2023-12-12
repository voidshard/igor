from igorUI import config

from pyigor import service
from pyigor import enums


_cfg = config.read_default_config()
Service = service.Service(**_cfg.get("client", {}))
