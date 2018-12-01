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


class _Lock:
    """Simple distributed lock using Redis.

    From: https://chris-lamb.co.uk/posts/distributing-locking-python-and-redis
    """

    def __init__(self, conn, key, expires=60, timeout=10):
        """
        Distributed locking using Redis SETNX and GETSET.

        Usage::

            with Lock('my_lock'):
                print "Critical section"

        :param  expires     We consider any existing lock older than
                            ``expires`` seconds to be invalid in order to
                            detect crashed clients. This value must be higher
                            than it takes the critical section to execute.
        :param  timeout     If another client has already obtained the lock,
                            sleep for a maximum of ``timeout`` seconds before
                            giving up. A value of 0 means we never wait.
        """
        self._conn = conn
        self.key = "lock:" + str(key)
        self.timeout = timeout
        self.expires = expires

    def __enter__(self):
        timeout = self.timeout
        while timeout >= 0:
            expires = float(time.time() + self.expires + 1)

            if self._conn.setnx(self.key, expires):
                # We gained the lock; enter critical section
                return

            current_value = float(self._conn.get(self.key))

            if all([
                current_value,
                current_value < time.time(),
                self._conn.getset(self.key, expires) == current_value,
            ]):
                # We found an expired lock and nobody raced us to replacing it
                return

            timeout -= 1
            time.sleep(1)

        raise LockTimeout(f"Timeout whilst waiting for lock: {self.key}")

    def __exit__(self, exc_type, exc_value, traceback):
        self._conn.delete(self.key)


class LockTimeout(Exception):
    """We timed out trying to get the lock.

    """
    pass


class TaskQueue:
    """Implements ordered queue using Redis & sorted sets.

       - Nb. 'zadd' 'zcard' and similar 'z' funcs are all redis sorted set operations.
         See https://redis-py.readthedocs.io/en/latest/

    """

    def __init__(self, host: str="redis", port: int=6379, lock_timeout: int=5):
        self._conn = redis.Redis(db=0, host=host, port=port)
        self._lock_timeout = lock_timeout

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
        :raises LockTimeout: if we can't get the queue lock fast enough.

        """
        name = self._key(queue)

        with _Lock(self._conn, name, timeout=self._lock_timeout):
            # lock approximates atomic 'zpop' which doesn't exist in redis ie. pop(0)
            value = self._conn.zrangebyscore(
                name,
                _LIMIT_LOWER, _upper_limit(),  # range of possible values (ie. all of them)
                start=_LIMIT_LOWER, num=1,  # analogous to 'offset 0, limit 1'
                score_cast_func=int  # cast result to int
            )

            if not value:
                return None  # queue is empty

            self._conn.zrem(name, value[0])
            return str(value[0], encoding='utf8')

    def count(self, queue: str) -> int:
        """Return the number of items in the given queue.

        :param queue:
        :return: int

        """
        return self._conn.zcard(self._key(queue)) or 0
