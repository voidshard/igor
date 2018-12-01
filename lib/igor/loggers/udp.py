import socket

from igor.loggers.base import Base


class UDPLogger(Base):

    def __init__(self, host, port):
        self._addr = (host, port)
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def log(self, task_id, worker_id, text, **kwargs):
        """Stream log line over SSL to other end.

        :param task_id: id of task
        :param worker_id: id of worker
        :param text: some string to be logged
        :param kwargs: random data that'll be encoded into the line

        """
        text = self._format(task_id, worker_id, text, **kwargs)
        try:
            self._sock.sendto(bytes(text.strip() + "\n", "utf-8"), self._addr)
        except Exception as e:
            print(f"unable to write to socket {e} {text}")

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
