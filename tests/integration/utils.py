import docker
from docker import errors


_DEFAULT_URL = "unix:///var/run/docker.sock"


class _Container:

    def __init__(self, container):
        self._container = container

    def logs(self, **kwargs):
        """

        :param kwargs:
        :return: generator

        """
        kwargs["stream"] = kwargs.get("stream", True)
        return self._container.logs(**kwargs)

    def stop(self):
        """Kill the container

        """
        self._container.stop(timeout=3)
        self._container.remove()

    def start(self):
        """Start the container

        """
        self._container.start()

    def __enter__(self):
        self._container.start()

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self.stop()
        except errors.NotFound:
            pass


class Client:

    _mongo = 'docker.io/mongo'
    _redis = 'docker.io/redis'

    def __init__(self, url=_DEFAULT_URL):
        self._client = docker.DockerClient(base_url=url)

    def mongo_container(self):
        """Return a 'mongo db' docker container.

        The container is automatically configured with
         - detach mode
         - auto remove

        """
        return self.create_container(self._mongo, ports={27017: 27017})

    def redis_container(self):
        """Return a 'redis db' docker container.

        The container is automatically configured with
         - detach mode
         - auto remove

        """
        return self.create_container(self._redis, ports={6379: 6379})

    def _create_container(self, image, cmd=None, **kwargs):
        """Return a generic container.

        The container is automatically configured with
         - detach mode
         - auto remove

        :param image:
        :param cmd:
        :param kwargs:
        :return: _Container

        """
        kwargs["detach"] = True
        kwargs["auto_remove"] = True
        return self._client.containers.create(image, command=cmd, **kwargs)

    def create_container(self, image, cmd=None, **kwargs):
        """Return a generic container.

        The container is automatically configured with
         - detach mode
         - auto remove

        :param image:
        :param cmd:
        :param kwargs:
        :return: _Container

        """
        return _Container(self._create_container(image, cmd=cmd, **kwargs))
