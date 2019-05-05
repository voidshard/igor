import os
import time
import psutil
import subprocess

import docker
import redis
import psycopg2

from docker import errors


_DEFAULT_URL = "unix:///var/run/docker.sock"
_SCHEMA_PG = "ddl/postgres/schema_template.sql"
_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def sql_postgres_schema() -> str:
    """Return filename of postgres schema sql.

    :return: str

    """
    return os.path.join(_ROOT, _SCHEMA_PG)


def run_cmd(cmd):
    """Runs some cmd. Raises if cmd returns non zero exit code.

    :param cmd:

    """
    proc = psutil.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    if proc.wait() != 0:
        raise Exception(proc.stdout.read())


class _Container:
    """Wrapper for a docker container
    """

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
        try:
            self._container.stop(timeout=3)
        except Exception:
            pass

        try:
            self._container.remove()
        except Exception:
            pass

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

    _postgres = 'docker.io/postgres:9.5'
    _redis = 'docker.io/redis:latest'

    def __init__(self, url=_DEFAULT_URL):
        self._client = docker.DockerClient(base_url=url)

    def postgres_container(self):
        """Return a 'postgres db' docker container.

        The container is automatically configured with
         - detach mode
         - auto remove

        In addition we try to run our create table schema against the db.

        """
        pg = self.create_container(self._postgres, ports={5432: 5432})

        conn = None
        err = None

        for i in range(1, 5):
            # 5 attempts to connect to the db - 'cause containers don't start immediately
            try:
                conn = psycopg2.connect(
                    host="localhost",
                    user="postgres",
                )
            except Exception as e:
                err = e
                time.sleep(2)

        if not conn:  # if we didn't manage to connect raise the exception & tear down container
            pg.stop()
            raise err

        try:
            # run our schema against the db
            run_cmd([
                "psql",
                "-U", "postgres",
                "-h", "localhost",
                "-f", sql_postgres_schema(),
                "postgres"
            ])
        except Exception:
            pg.stop()
            raise

        return pg

    def redis_container(self):
        """Return a 'redis db' docker container.

        The container is automatically configured with
         - detach mode
         - auto remove

        """
        rds = self.create_container(self._redis, ports={6379: 6379})

        conn = None
        err = None

        for i in range(1, 5):
            # 5 attempts to connect to redis 'cause containers don't start immediately
            try:
                conn = redis.Redis(host="localhost")
                conn.ping()
            except Exception as e:
                err = e
                time.sleep(2)

        if not conn:
            rds.stop()
            raise err

        return rds

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
        conn = self._client.containers.create(image, command=cmd, **kwargs)
        conn.start()
        return conn

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


if __name__ == "__main__":
    # client = Client()
    # with client.postgres_container():
    #     time.sleep(120)
    # with client.redis_container():
    #     time.sleep(120)
    pass
