import abc
import uuid

from igor import enums


class Serializable(metaclass=abc.ABCMeta):
    """Something that can be encoded & decoded to/from a dict.
    """

    @abc.abstractmethod
    def encode(self) -> dict:
        """Deflate obj to dict

        :return: dict

        """
        pass

    @abc.abstractclassmethod
    def decode(cls, data: dict):
        """Hydrate class instance from data

        :param data:
        :return: cls instance

        """
        pass


class Stateful(Serializable, metaclass=abc.ABCMeta):
    """A unique thing that also includes state and metadata.
    """

    def __init__(self):
        super(Stateful, self).__init__()
        self.id = str(uuid.uuid4())
        self.etag = str(uuid.uuid4())
        self.state = enums.State.PENDING.value
        self.metadata = {}
        self.key = None


class Runnable(Stateful, metaclass=abc.ABCMeta):
    """Something that can be executed by a Runner.
    """

    def __init__(self):
        super(Runnable, self).__init__()
        self.user_id = None
        self.runner_id = None
        self.paused = None
