from igor.loggers.base import Base


class StdoutLogger(Base):

    def log(self, task_id, worker_id, text, **kwargs):
        """Send log line to stdout.

        :param task_id: id of task
        :param worker_id: id of worker
        :param text: some string to be logged
        :param kwargs: random data that'll be encoded into the line

        """
        print(self._format(task_id, worker_id, text, **kwargs))

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
