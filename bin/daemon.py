from igor import config
from igor import service
from igor.runner import DefaultRunner


conf = config.read_default_config()

svc = service.IgorDBService(config=conf)

runner = DefaultRunner(
    svc,
    host=conf.get("runner", {})["host"],
    port=int(conf.get("runner", {})["port"]),
)

try:
    runner.start_daemon()
finally:
    runner.stop()
