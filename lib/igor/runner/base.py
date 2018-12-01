import abc


class Base(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def stop(self):
        """Order daemon to stop & exit.

        """
        pass

    @abc.abstractmethod
    def start_daemon(self, *args, **kwargs):
        """Should start a worker process on the current host.

        - This should block & perform tasks until killed.
        - Workers should register themselves with the IgorDBService when they start
        - Workers should deregister themselves with the IgorDBService when they exit

        :param args:
        :param kwargs:

        """
        pass

    @abc.abstractmethod
    def queued_tasks(self, name=None) -> int:
        """Return the number of tasks that are currently queued to be run.

        :param name: name of queue to count. Should default to main queue.
        :return: int

        """
        pass

    @abc.abstractmethod
    def queue_tasks(self, layer, tasks) -> int:
        """Queue task(s) to be run.

        :param layer: parent layer
        :param tasks:
        :returns int: number of tasks actually launched

        """
        pass

    @abc.abstractmethod
    def send_kill(self, worker_id=None, job_id=None, layer_id=None, task_id=None):
        """Order tasks of the given job / layer / task killed.

        - If worker_id is not None only the given worker should be affected.
        - one of job_id layer_id task_id is required

        :param worker_id: send only to the given worker.
        :param job_id:
        :param layer_id:
        :param task_id:
        :raises ValueError: If no kwargs given.

        """
        pass

    @abc.abstractmethod
    def send_worker_pause(self, worker_id):
        """Order the given worker to stop accepting new tasks.

        This should stop the currently running task(s).

        :param worker_id:

        """
        pass

    @abc.abstractmethod
    def send_worker_unpause(self, worker_id):
        """Order the given worker to start accepting new tasks.

        Ie this is the inverse of "worker_pause()" - a worker must be sent this before it will
        begin processing new tasks after being set to drain.

        :param worker_id:

        """
        pass
