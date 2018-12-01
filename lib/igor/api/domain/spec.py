"""Various objects that the API will accept inbound.
"""

import abc
import uuid

from igor import enums
from igor.api import exceptions as exc


_VALID_STATES = [
    enums.State.PENDING.value,
    enums.State.QUEUED.value,
    enums.State.RUNNING.value,
    enums.State.COMPLETED.value,
    enums.State.ERRORED.value,
    enums.State.SKIPPED.value,
]


class _Decode(metaclass=abc.ABCMeta):

    @abc.abstractclassmethod
    def decode(cls, data: dict):
        pass

    @classmethod
    def to_name(cls, i):
        """

        :param i:

        """
        if i is None:
            return i
        if isinstance(i, bytes):
            return str(i, encoding='utf8')
        return str(i)

    @classmethod
    def to_int(cls, i):
        """Force to int.

        :param i:
        :return: int

        """
        try:
            return int(i)
        except:
            raise exc.InvalidSpec(f"unable to cast to int: {i}")

    @classmethod
    def to_id(cls, i):
        """

        :param i:

        """
        if i is None:
            raise exc.InvalidSpec(f"invalid id: {i}")

        s = cls.to_name(i)

        try:
            uuid.UUID(s)
        except ValueError:
            raise exc.InvalidSpec(f"igor ids are uuid4 strings, got: {s}")

        if len(s) == 36:
            return s

        raise exc.InvalidSpec(f"igor ids are len 36 strings, got: {s}")

    @classmethod
    def to_list_ids(cls, ids):
        """

        :param ids:

        """
        if not isinstance(ids, list):
            raise exc.InvalidSpec(f"require list of ids to be a list, got: {ids}")

        return [cls.to_id(i) for i in ids]

    @classmethod
    def to_list_str(cls, ls):
        """

        :param ls:

        """
        if not isinstance(ls, list):
            raise exc.InvalidSpec(f"require a list, got: {ls}")

        strings = [cls.to_name(i) for i in ls]
        return [s for s in strings if s]

    @classmethod
    def to_list_children(cls, class_name, child_class, children):
        if not children:
            return []

        if not isinstance(children, list):
            raise exc.InvalidSpec(f"{class_name} data expected as list got: {children}")

        parsed = []
        for c in children:
            if not isinstance(c, dict):
                raise exc.InvalidSpec(f"each {class_name} is expected to be a dict got: {c}")

            parsed.append(child_class.decode(c))
        return parsed

    @classmethod
    def to_bool(cls, i):
        try:
            return bool(i)
        except Exception:
            return False


class UserSpec(_Decode):

    def __init__(self):
        self.name = ""
        self.is_admin = False
        self.password = ""

    @classmethod
    def decode(cls, data: dict):
        me = cls()

        me.name = cls.to_name(data.get("name"))
        me.is_admin = cls.to_bool(data.get("is_admin", False))
        me.password = cls.to_name(data.get("password"))
        if not me.password:
            raise exc.InvalidSpec(f"cannot have empty password")

        return me


class QuerySpec(_Decode):

    _DEFAULT_LIMIT = 1000
    _MAX_LIMIT = 10000

    def __init__(self):
        self.filters = []
        self.limit = self._DEFAULT_LIMIT
        self.offset = 0
        self.user_id = None

    @classmethod
    def decode(cls, data: dict):
        me = cls()

        me.user_id = None
        uid = data.get("user_id")
        if uid:
            me.user_id = cls.to_id(uid)

        me.limit = max([
            min([
                cls.to_int(data.get("limit", cls._DEFAULT_LIMIT)),
                cls._MAX_LIMIT
            ]),
            1
        ])
        me.offset = max([
            cls.to_int(data.get("offset", 0)),
            0
        ])

        me.filters = cls.to_list_children(
            "filter",
            FilterSpec,
            data.get("filters")
        )

        return me


class FilterSpec(_Decode):

    def __init__(self):
        self.job_ids = []
        self.layer_ids = []
        self.task_ids = []
        self.states = []
        self.keys = []

    @classmethod
    def decode(cls, data: dict):
        me = cls()

        me.job_ids = cls.to_list_ids(data.get("job_ids", []))
        me.layer_ids = cls.to_list_ids(data.get("layer_ids", []))
        me.task_ids = cls.to_list_ids(data.get("task_ids", []))

        me.states = cls.to_list_str(data.get("states", []))
        for state in me.states:
            if state not in _VALID_STATES:
                raise exc.InvalidSpec(f"unknown state: {state}")

        me.keys = cls.to_list_str(data.get("keys", []))

        return me


class JobSpec(_Decode):
    def __init__(self):
        # required
        self.user_id = None
        self.layers = []  # []LayerSpec

        # optional
        self.name = None
        self.paused = False
        self.logger = {
            "type": enums.LoggerType.STDOUT.value,
            "host": None,
            "port": None,
        }

    @classmethod
    def to_logger_config(cls, ldata):
        """

        :param ldata:
        :return: dict

        """
        if ldata is None:
            return {"type": enums.LoggerType.STDOUT.value}

        if not isinstance(ldata, dict):
            raise exc.InvalidSpec(f"expected logger config as dict, got: {ldata}")

        ltype = ldata.get("type")
        if ltype == enums.LoggerType.STDOUT.value:
            return {"type": enums.LoggerType.STDOUT.value}

        elif ltype == enums.LoggerType.UDP.value:
            host = ldata.get("host")
            if not host:
                raise exc.InvalidSpec(f"expected logger config to contain 'host' property")

            port = cls.to_int(ldata.get("port", -1))
            if not port:
                raise exc.InvalidSpec(f"expected logger config to contain 'port' property")

            if not (0 < port < 65536):
                raise exc.InvalidSpec(f"logger config port number out of bounds: {port}")

            return {
                "type": enums.LoggerType.UDP.value,
                "host": str(host),
                "port": cls.to_int(port),
            }

        raise exc.InvalidSpec(f"unknown logger 'type' got: {ltype}")

    @classmethod
    def decode(cls, data: dict):
        me = cls()
        me.name = cls.to_name(data.get("name"))
        me.user_id = None
        me.paused = cls.to_bool(data.get("paused",  False))

        uid = data.get("user_id")
        if uid:
            me.user_id = cls.to_id(uid)

        me.logger = cls.to_logger_config(data.get("logger"))
        me.layers = cls.to_list_children(
            "layer",
            LayerSpec,
            data.get("layers")
        )
        if not me.layers:
            raise exc.InvalidSpec("layer data expected at least one child")

        return me


class LayerSpec(_Decode):
    def __init__(self):
        # required
        self.tasks = []  # []TaskSpec

        # optional
        self.paused = False
        self.order = 0
        self.priority = 0
        self.name = None

    @classmethod
    def decode(cls, data: dict):
        me = cls()

        me.order = cls.to_int(data.get("order", 0))
        me.priority = cls.to_int(data.get("priority", 0))
        me.name = cls.to_name(data.get("name"))
        me.paused = cls.to_bool(data.get("paused",  False))

        me.tasks = cls.to_list_children(
            "task",
            TaskSpec,
            data.get("tasks")
        )

        return me


class TaskSpec(_Decode):
    def __init__(self):
        # required
        self.cmd = []  # []str

        # optional
        self.paused = False
        self.max_attempts = 3
        self.env = {}
        self.name = None

    @classmethod
    def to_env_dict(cls, i) -> dict:
        """

        :param i:
        :return: dict

        """
        if not i:
            return {}

        if not isinstance(i, dict):
            raise exc.InvalidSpec(f"env data must be passed as dict got: {i}")

        parsed = {}
        for k, v in i.items():
            k = cls.to_name(k)
            v = cls.to_name(v)

            if not all([k, v]):
                continue

            parsed[k] = v
        return parsed

    @classmethod
    def decode(cls, data: dict):
        me = cls()

        me.cmd = cls.to_list_str(data.get("cmd", []))
        if not me.cmd:
            raise exc.InvalidSpec("task data requires cmd")

        me.paused = cls.to_bool(data.get("paused",  False))

        me.max_attempts = cls.to_int(data.get("max_attempts", 3))
        me.env = cls.to_env_dict(data.get("env", {}))
        me.name = cls.to_name(data.get("name"))

        return me
