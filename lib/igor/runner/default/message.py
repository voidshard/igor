import time
import json
import uuid

from collections import namedtuple

from igor.runner.default import exceptions as exc


MSG_WORK = "do-work"  # a piece of work that needs doing
_MSG_WORK_ARGS = ["job_id", "layer_id", "task_id"]

MSG_KILL = "stop-work"  # anyone working on the given job / layer / task is ordered to SIGINT
_MSG_KILL_ARGS = ["job_id", "layer_id", "task_id"]

MSG_PAUSE = "pause-worker"  # a worker should stop accepting new tasks until notified to "start"
_MSG_PAUSE_ARGS = []

MSG_UNPAUSE = "unpause-worker"  # a worker is asked to start processing work (defaults to true)
_MSG_UNPAUSE_ARGS = []

MSG_ANNOUNCE = "announcement"  # general events chan
EVENT_WORK_QUEUED = "work-queued"  # work has been queued up
_MSG_ANNOUNCE_ARGS = ["event"]

_MSG_KEY_ID = "id"
_MSG_KEY_TIME = "time"
_MSG_KEY_TYPE = "type"
_MSG_KEY_DATA = "data"
_MSG_REQUIRED_KEYS = [_MSG_KEY_ID, _MSG_KEY_TYPE, _MSG_KEY_TIME, _MSG_KEY_DATA]

# what we decode messages to after receiving
_Message = namedtuple("message", [_MSG_KEY_ID, _MSG_KEY_TYPE, _MSG_KEY_TIME, _MSG_KEY_DATA])


def encode_message(type_: str, **kwargs) -> str:
    """Encode a nicely formatted JSON message as a string.

    :param type_:
    :param kwargs:
    :return: str

    """
    return json.dumps({
        _MSG_KEY_ID: str(uuid.uuid4()),
        _MSG_KEY_TYPE: type_,
        _MSG_KEY_TIME: time.time(),
        _MSG_KEY_DATA: _message_data(type_, **kwargs),
    })


def _message_data(type_: str, **kwargs) -> dict:
    """For a given message type and inputs, construct a valid message dict

    :param type_:
    :param kwargs:
    :return: dict
    :raises UnknownMessage: unknown message type
    :raises MalformedMessage: invalid or missing message args

    """
    if type_ not in [MSG_WORK, MSG_KILL, MSG_PAUSE, MSG_UNPAUSE, MSG_ANNOUNCE]:
        raise exc.UnknownMessage(type_)

    data = {}
    args = []

    if type_ == MSG_WORK:
        args = _MSG_WORK_ARGS
    elif type_ == MSG_ANNOUNCE:
        args = _MSG_ANNOUNCE_ARGS

    for k in args:
        if k not in kwargs:
            raise exc.MalformedMessage(f"message type {type_} missing key {k}")

        data[k] = kwargs[k]

    return data


def decode_message(message) -> _Message:
    """Decode message handles both of
     - a redis message dict returned by "listen()" on a pubsub
     - a raw encoded message string as written by "_encode_message()"
    And returns a valid message as intended by "_encode_message()" .. or raises trying ..

    :param message: message dict or string
    :return: dict
    :raises UnknownMessage: unknown message type
    :raises MalformedMessage:

    """
    if isinstance(message, str):
        msg = json.loads(message)
    elif isinstance(message, dict):
        msg = message
    else:
        raise exc.MalformedMessage(f"unsure how to parse message, given {message}")

    if isinstance(msg.get("data"), int):
        # occurs when redis push subscription notifications on a chan
        raise exc.MalformedMessage(f"message doesn't include igor data")

    if any(["pattern" in msg, "channel" in msg]):
        # redis sends messages like
        # {'type': 'message', 'pattern': None, 'channel': b'test', 'data': b'hi!'}
        # Where the 'data' bit is our set message
        return decode_message(msg.get("data", {}))

    for required_key in _MSG_REQUIRED_KEYS:
        if required_key not in msg:
            raise exc.MalformedMessage(f"message requires all of {_MSG_REQUIRED_KEYS} got {msg}")

    msg_type = msg.get(_MSG_KEY_TYPE)

    return _Message(
        msg.get(_MSG_KEY_ID),
        msg.get(_MSG_KEY_TYPE),
        msg.get(_MSG_KEY_TIME),
        _message_data(
            msg_type,
            **msg.get(_MSG_KEY_DATA, {})
        )
    )


class MessageBuffer:
    """Holds a fixed number of messages around so we don't repeat things (much).
    Not perfect, but it's something.

    """

    def __init__(self, size=100):
        self._size = size
        self._buffer = [None] * size  # fixed size list of message IDs
        self._count = 0
        self._messages = {}  # map message Id -> message

    def _incr_count(self):
        """Increment count by 1, returning to 0 iff count >= buffer size

        :return: next count

        """
        self._count += 1
        if self._count >= self._size:
            self._count = 0
        return self._count

    def decode_message(self, message) -> _Message:
        """Decode the given message.

        Nb. This can only be called by one thread at a time.

        :param message:
        :return: namedtuple
        :raises UnknownMessage: unknown message type
        :raises MalformedMessage:
        :raises DuplicateMessage: if message has appeared in the last 'size' messages

        """
        msg = decode_message(message)

        if msg.id in self._messages:
            raise exc.DuplicateMessage(msg.id)

        slot = self._incr_count()

        old_msg_id = self._buffer[slot]
        if old_msg_id:
            try:
                del self._messages[old_msg_id]
            except KeyError:
                pass

        self._buffer[slot] = msg.id
        self._messages[msg.id] = msg

        return msg
