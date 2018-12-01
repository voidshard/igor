from igor import config
from igor import service
from igor.runner import DefaultRunner
from igor import scheduler


conf = config.read_default_config()

svc = service.IgorDBService(config=conf)

runner = DefaultRunner(
    svc,
    host=conf.get("runner", {})["host"],
    port=conf.get("runner", {})["port"],
)

s = scheduler.Simple(svc, runner)

try:
    s.run()
finally:
    s.stop()
