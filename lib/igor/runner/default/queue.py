import time
import redis


_LIMIT_LOWER = 0  # lowest possible priority value
_LIMIT_OFFSET_UPPER = 1000000  # highest possible priority value is this + epoch time in hours


def _hours_since_epoch() -> int:
    """Return an int representing the number of hours it's been since the epoch.

    :return: int

    """
    return int(time.time() / 3600)


def _upper_limit() -> int:
    """Return an int representing the highest possible priority value.

    :return: int

    """
    return _LIMIT_OFFSET_UPPER + _hours_since_epoch()


def _final_priority(base: int) -> int:
    """Calculate a priority value to write into redis. This value is weighted against the number
    of hours since the epoch (so older tasks get progressively more important and we avoid
    starvation).

    The input to this is a priority - the higher the better - the output is a priority
    the *lower* the better. This is simply because it suits our redis impl better.

    :param base: the base priority (where bigger numbers imply "send sooner")
    :return: int

    """
    # this ensures that older notifications are queued with +1 priority per hour
    hours_since_epoch = int(time.time() / 3600)

    # in the case of our redis queue the -smaller- the priority the sooner it'll get sent
    return min([
        max([hours_since_epoch - base, _LIMIT_LOWER]),
        _upper_limit(),
    ])


class UnsupportedRedisVersion(Exception):
    pass


class TaskQueue:
    """Implements ordered queue using Redis & sorted sets.

       - Nb. 'zadd' 'zcard' and similar 'z' funcs are all redis sorted set operations.
         See https://redis-py.readthedocs.io/en/latest/

    """

    def __init__(self, host: str="redis", port: int=6379):
        self._conn = redis.Redis(db=0, host=host, port=port)

        info = self._conn.info()
        if info.get("redis_version") < "5.0.0":
            # zpopmin and bzpopmin are were added redis ~v5. We can approximate zpopmin using
            # some locks, but it's cleaner & simpler to use a higher redis version.
            raise UnsupportedRedisVersion(f"Require redis 5+: info={info}")

    @classmethod
    def _key(cls, queue: str) -> str:
        """Turn given id into a set name to be used in redis.

        :param queue:
        :return: str

        """
        return f"igor:queue:{queue}"

    def add(self, queue: str, key: str, priority: int):
        """Add given (key, priority) to queue.

        :param queue:
        :param key:
        :param priority:

        """
        self._conn.zadd(
            self._key(queue),
            {key: _final_priority(priority)}
        )

    def pop(self, queue: str) -> str:
        """Pop the the next key from the queue.

        :param queue:
        :return: str or None

        """
        name = self._key(queue)
        value = self._conn.zpopmin(name)
        if not value:
            return None
        return str(value[0][0], encoding="utf8")

    def count(self, queue: str) -> int:
        """Return the number of items in the given queue.

        :param queue:
        :return: int

        """
        return self._conn.zcard(self._key(queue)) or 0
