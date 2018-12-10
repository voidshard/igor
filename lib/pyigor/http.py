import base64
import json

import requests

from pyigor import domain
from pyigor import enums
from pyigor import exceptions as exc
from pyigor import utils


logger = utils.logger()


class IgorClient:
    """Super simple HTTP client for talking to an igor gateway.
    """

    _action_pause = "pause"
    _action_retry = "retry"

    _jobs = "jobs/"
    _layers = "layers/"
    _tasks = "tasks/"
    _workers = "workers/"

    def __init__(
        self,
        url="https://localhost:9025/v1/",
        username="admin",
        password="admin",
        ssl_verify=False,
        ssl_pem=None,
    ):
        self._url = url
        self._token = str(base64.b64encode(
            bytes(f"{username}:{password}", encoding="utf8")
        ), encoding="utf8")
        self._pem = ssl_pem
        self._verify_ssl = self._pem if ssl_verify else False

    @classmethod
    def _query(
        cls,
        states=None,
        job_ids=None,
        layer_ids=None,
        task_ids=None,
        worker_ids=None,
        keys=None,
        limit=500,
        offset=0
    ) -> dict:
        """

        :param limit:
        :param offset:
        :param states:
        :return: dict

        """
        filt = {}

        if states:
            filt["states"] = states
        if job_ids:
            filt["job_ids"] = job_ids
        if layer_ids:
            filt["layer_ids"] = layer_ids
        if task_ids:
            filt["task_ids"] = task_ids
        if worker_ids:
            filt["worker_ids"] = worker_ids
        if keys:
            filt["keys"] = keys

        query = {
            "limit": limit,
            "offset": offset,
        }
        if filt:
            query["filters"] = [filt]

        return json.dumps(query)

    def _get(self, endpoint, data: dict):
        """"""
        url = self._url + endpoint
        logger.info(f"    --> GET {url}")
        result = requests.get(
            url,
            cert=self._pem,
            data=data,
            headers=self._headers,
            verify=self._verify_ssl,
        )
        logger.info(f"{result.status_code} <-- GET {url}")
        self._check_status(result)
        return result

    def _yield_all(self, func, endpoint: str, result_cls, **kwargs):
        """

        :param func:
        :param endpoint:
        :param result_cls:
        :param kwargs: query filters
        :return:

        """
        limit = 500
        offset = 0
        found = limit + 1

        while found >= limit:
            found = 0
            query = self._query(limit=limit, offset=offset, **kwargs)

            result = func(endpoint, query)

            for data in result.json():
                found += 1
                yield result_cls(data)

    def _perform_action(self, endpoint, obj_id, obj_etag, action):
        """

        :param endpoint:
        :param obj_id:
        :param obj_etag:

        """
        headers = self._headers
        headers["Etag"] = obj_etag

        url = self._url + endpoint + obj_id
        logger.info(f"    --> POST {url}")
        result = requests.post(
            url,
            data=json.dumps({"action": action}),
            cert=self._pem,
            headers=headers,
            verify=self._verify_ssl,
        )
        logger.info(f"{result.status_code} <-- POST {url}")
        self._check_status(result)

    def _kill(self, endpoint, obj_id, obj_etag):
        """

        :param endpoint:
        :param obj_id:
        :param obj_etag:

        """
        headers = self._headers
        headers["Etag"] = obj_etag

        url = self._url + endpoint + obj_id
        logger.info(f"    --> DELETE {url}")
        result = requests.delete(
            self._url + endpoint + obj_id,
            cert=self._pem,
            headers=headers,
            verify=self._verify_ssl,
        )
        logger.info(f"{result.status_code} <-- DELETE {url}")
        self._check_status(result)

    def pause_job(self, id_: str, tag: str):
        """

        :param id_:
        :param tag:

        """
        self._perform_action(self._jobs, id_, tag, self._action_pause)

    def pause_layer(self, id_: str, tag: str):
        """

        :param id_:
        :param tag:

        """
        self._perform_action(self._layers, id_, tag, self._action_pause)

    def pause_task(self, id_: str, tag: str):
        """

        :param id_:
        :param tag:

        """
        self._perform_action(self._tasks, id_, tag, self._action_pause)

    def set_task_result(self, id_: str, tag: str, result):
        """

        :param id_:
        :param tag:
        :param result:
        :return:

        """
        headers = self._headers
        headers["Etag"] = tag
        headers["Content-Type"] = "application/octet-stream"

        url = self._url + self._tasks + id_ + "/result"
        logger.info(f"    --> POST {url}")
        result = requests.post(
            url,
            data=result,  # careful: this isn't JSON
            cert=self._pem,
            headers=headers,
            verify=self._verify_ssl,
        )
        logger.info(f"{result.status_code} <-- POST {url}")
        self._check_status(result)

    def set_task_env(self, id_: str, tag: str, env: dict):
        """

        :param id_:
        :param tag:
        :param env:
        :return:

        """
        headers = self._headers
        headers["Etag"] = tag

        url = self._url + self._tasks + id_ + "/environment"
        logger.info(f"    --> POST {url}")
        result = requests.post(
            url,
            data=json.dumps(env),
            cert=self._pem,
            headers=headers,
            verify=self._verify_ssl,
        )
        logger.info(f"{result.status_code} <-- POST {url}")
        self._check_status(result)

    def retry_task(self, id_: str, tag: str):
        """

        :param id_:
        :param tag:

        """
        self._perform_action(self._tasks, id_, tag, self._action_retry)

    def kill_job(self, id_: str, tag: str):
        """

        :param id_:
        :param tag:

        """
        self._kill(self._jobs, id_, tag)

    def kill_layer(self, id_: str, tag: str):
        """

        :param id_:
        :param tag:

        """
        self._kill(self._layers, id_, tag)

    def kill_task(self, id_: str, tag: str):
        """

        :param id_:
        :param tag:

        """
        self._kill(self._tasks, id_, tag)

    def get_jobs(self, keys=None, states=None, job_ids=None) -> list:
        """

        :return: []domain.Job

        """
        for o in self._yield_all(
            self._get,
            self._jobs,
            domain.Job,
            keys=keys,
            states=states,
            job_ids=job_ids,
        ):
            yield o

    def get_layers(self, keys=None, job_ids=None, layer_ids=None, states=None) -> list:
        """

        :return: []domain.Layer

        """
        for o in self._yield_all(
            self._get,
            self._layers,
            domain.Layer,
            keys=keys,
            states=states,
            job_ids=job_ids,
            layer_ids=layer_ids,
        ):
            yield o

    def get_tasks(
        self, keys=None, job_ids=None, layer_ids=None, task_ids=None, states=None
    ) -> list:
        """

        :return: []domain.Task

        """
        for o in self._yield_all(
            self._get,
            self._tasks,
            domain.Task,
            keys=keys,
            states=states,
            job_ids=job_ids,
            layer_ids=layer_ids,
            task_ids=task_ids,
        ):
            yield o

    def get_workers(
        self, job_ids=None, layer_ids=None, task_ids=None, worker_ids=None, keys=None
    ) -> list:
        """

        :return: []domain.Worker

        """
        for o in self._yield_all(
            self._get,
            self._workers,
            domain.Worker,
            keys=keys,
            worker_ids=worker_ids,
            job_ids=job_ids,
            layer_ids=layer_ids,
            task_ids=task_ids,
        ):
            yield o

    @classmethod
    def _check_status(cls, request_result):
        """Raises appropriate errors if required

        :param request_result:

        """
        status = request_result.status_code
        if status in [200, 201, 203]:
            return

        err = request_result.json().get("error", f"{status}")

        if status == 400:
            raise exc.MalformedRequestError(err)
        elif status == 401:
            raise exc.AuthError(err)
        elif status == 404:
            raise exc.NotFoundError(err)
        elif status == 409:
            raise exc.EtagMismatchError(err)
        elif status == 500:
            raise exc.ServerError(err)
        raise Exception(err)

    @property
    def _headers(self) -> dict:
        """

        :return: dict

        """
        return {
            "Content-Type": "application/json",
            "Authorization": f"Basic {self._token}",
        }
