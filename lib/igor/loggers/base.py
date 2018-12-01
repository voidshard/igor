import abc
import datetime


class Base(metaclass=abc.ABCMeta):

    @staticmethod
    def _format(task_id, worker_id, text, **kwargs):
        """Returns log line in standard format.

        :param task_id: id of task
        :param worker_id: id of worker
        :param text: some string to be logged
        :param kwargs: random data that'll be encoded into the line

        """
        misc = " ".join("%s=%s" % (k, v) for k, v in kwargs.items())
        return f"{task_id}|{worker_id}|{misc}|{text}"

    @abc.abstractmethod
    def log(self, task_id, worker_id, text, **kwargs):
        pass

    @abc.abstractmethod
    def __enter__(self):
        pass

    @abc.abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
