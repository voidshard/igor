from igor import config
from igor import service
from igor.runner import DefaultRunner
from igor.api.gateway import IgorGateway
from igor.api.transport.http import HttpTransport


conf = config.read_default_config()

svc = service.IgorDBService(config=conf)

runner = DefaultRunner(
    svc,
    host=conf.get("runner", {})["host"],
    port=int(conf.get("runner", {})["port"]),
)

trans = HttpTransport(
    port=int(conf.get("gateway", {})["port"]),
    ssl_key=conf.get("gateway", {}).get("ssl_key"),
    ssl_cert=conf.get("gateway", {}).get("ssl_cert"),
)

try:
    trans.serve(IgorGateway(svc, runner))
finally:
    trans.stop()
