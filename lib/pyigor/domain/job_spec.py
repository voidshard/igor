

class JobSpec:

    def __init__(self, layers, user_id=None, name=None, paused=False):
        self.layers = layers
        self.user_id = user_id
        self.name = name
        self.paused = paused

    def encode(self) -> dict:
        return {
            "name": self.name,
            "paused": self.paused,
            "layers": [l.encode() for l in self.layers]
        }


class LayerSpec:

    def __init__(self, tasks=None, order=0, name=None, paused=False):
        self.order = order
        self.name = name
        self.tasks = tasks or []
        self.paused = paused

    def encode(self) -> dict:
        return {
            "name": self.name,
            "order": self.order,
            "paused": self.paused,
            "tasks": [t.encode() for t in self.tasks],
        }


class TaskSpec:

    def __init__(self, cmd, name=None, paused=False, max_attempts=3):
        self.name = name
        self.cmd = cmd
        self.paused = paused
        self.max_attempts = max_attempts

    def encode(self) -> dict:
        return {
            "name": self.name,
            "paused": self.paused,
            "cmd": self.cmd,
            "max_attempts": self.max_attempts,
        }


class UpdateReply:

    def __init__(self, id: str, etag: str):
        self.id = id
        self.etag = etag

