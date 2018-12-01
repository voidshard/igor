"""In 'default' (as in "default runner") we supply utils used by runner.default_impl.

They are essentially task queue primitives
 - messages w/ various types & enums
 - small message buffer to protect us from duplicate messages sent in quick succession
 - encode / decode message functions
 - a priority queue implemented in redis
 - a process manager for forking, monitoring & killing commands

"""

from igor.runner.default.exceptions import DuplicateMessage
from igor.runner.default.exceptions import MalformedMessage
from igor.runner.default.exceptions import UnknownMessage

from igor.runner.default.message import MSG_WORK
from igor.runner.default.message import MSG_PAUSE
from igor.runner.default.message import MSG_UNPAUSE
from igor.runner.default.message import MSG_KILL
from igor.runner.default.message import MSG_ANNOUNCE
from igor.runner.default.message import EVENT_WORK_QUEUED

from igor.runner.default.message import MessageBuffer
from igor.runner.default.message import decode_message
from igor.runner.default.message import encode_message

from igor.runner.default.process_manager import ProcessManager

from igor.runner.default.queue import TaskQueue


__all__ = [
    "DuplicateMessage",
    "MalformedMessage",
    "UnknownMessage",

    "MSG_WORK",
    "MSG_PAUSE",
    "MSG_UNPAUSE",
    "MSG_KILL",
    "MSG_ANNOUNCE",

    "EVENT_WORK_QUEUED",

    "MessageBuffer",
    "decode_message",
    "encode_message",

    "ProcessManager",

    "TaskQueue",
]

