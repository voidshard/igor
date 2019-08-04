import collections
import time
import json
import uuid
import sys
import random

import psycopg2
from psycopg2.extras import DictCursor

from igor import domain
from igor import enums
from igor import exceptions as exc
from igor.database.base import Base, TransactionBase


_DEFAULT_LIMIT = 500
_MAX_LIMIT = 10000


class _Transaction(TransactionBase):
    # tables
    _T_JOB = "jobs"
    _T_LYR = "layers"
    _T_TSK = "tasks"
    _T_REC = "task_records"
    _T_WKR = "workers"
    _T_USR = "users"

    def __init__(self, conn):
        self._conn = conn
        self._cur = None

    def __enter__(self):
        self._cur = self._conn.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type:
                raise exc_type(exc_val)

            self._conn.commit()
        except Exception as e:
            self._conn.rollback()
            raise e
        finally:
            self._cur.close()

    INSERT_REC = f"""INSERT INTO {_T_REC} (
        str_task_record_id,
        str_job_id,
        str_layer_id,
        str_task_id,
        str_worker_id,
        str_reason,
        enum_state) VALUES (%s, %s, %s, %s, %s, %s, %s);
    """

    def create_task_record(self, tsk: domain.Task, wkr_id: str, state, reason=""):
        """

        :param tsk:
        :param wkr_id:
        :param state:
        :param reason:

        """
        self._cur.execute(
            self.INSERT_REC,
            (uuid.uuid4().hex, tsk.job_id, tsk.layer_id, tsk.id, wkr_id, reason, state)
        )

    INSERT_USER = f"""INSERT INTO {_T_USR} (
      str_user_id, 
      str_etag, 
      str_key, 
      bool_is_admin, 
      json_metadata, 
      str_password) VALUES (%s, %s, %s, %s, %s, %s);"""

    def create_user(self, user: domain.User):
        """Create a single user.

        :param user:

        """
        try:
            self._cur.execute(
                self.INSERT_USER,
                (
                    user.id,
                    user.etag,
                    user.name,
                    user.is_admin,
                    json.dumps(user.metadata),
                    user.password
                )
            )
            self._conn.commit()
        except psycopg2.IntegrityError:
            raise exc.WriteConflictError(f"user with name exists already: {user.name}")

    def delete_user(self, user_id: str):
        """Remove single user by id.

        :param user_id:

        """
        self._cur.execute(f"DELETE FROM {self._T_USR} WHERE str_user_id = %s;", (user_id,))

    def update_user(self, user_id: str, etag: str, name=None, password=None, metadata=None):
        """

        :param user_id:
        :param etag:
        :param name:
        :param password:
        :param metadata:

        """
        new_tag = str(uuid.uuid4())
        update = {
            "str_etag": new_tag,
            "time_updated": time.time(),
        }

        for n, v in [
            ("str_key", name),
            ("str_password", password),
            ("json_metadata", json.dumps(metadata) if metadata else None),
        ]:
            if v is None:
                continue

            update[n] = v

        sql, values = self._to_update_query(
            self._T_USR, "str_user_id", user_id, etag, update
        )

        self._cur.execute(sql, values)

        if self._cur.rowcount != 1:
            raise exc.WriteConflictError(f'failed to update user {user_id} {etag}')

        return new_tag

    def get_users(self, query: domain.Query) -> list:
        """

        :param query:
        :return: []domain.User

        """
        sql, values = self._filter(query, self._T_USR, user_id=True, key=True)
        results = []

        self._cur.execute(sql, values)

        for row in self._cur.fetchall():
            results.append(domain.User.decode({
                k[k.index("_") + 1:]: v for k, v in row.items()
            }))

        return results

    INSERT_JOB = f"""INSERT INTO {_T_JOB} (
      enum_state, 
      str_job_id, 
      str_user_id, 
      str_etag, 
      str_runner_id,
      str_key, 
      time_paused, 
      json_metadata) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"""
    INSERT_LYR = f"""INSERT INTO {_T_LYR} (
      str_job_id, 
      str_layer_id, 
      str_user_id, 
      str_etag, 
      str_runner_id,
      str_key, 
      time_paused,
      json_metadata, 
      enum_state, 
      int_priority, 
      int_order, 
      json_parents, 
      json_siblings, 
      json_children) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
    INSERT_TSK = f"""INSERT INTO {_T_TSK} (
      enum_state, 
      str_job_id, 
      str_layer_id, 
      str_task_id, 
      str_user_id, 
      str_etag, 
      str_runner_id, 
      str_key, 
      time_paused,
      json_metadata, 
      json_cmd,
      json_env,
      int_max_attempts) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""

    def create_job(self, job: domain.Job, layers: list, tasks: list):
        """Insert a job, along with all layers & tasks.

        Nb. This is all or nothing.

        :param job:
        :param layers:
        :param tasks:

        """
        layer_ids = []
        layer_data = []
        task_data = []
        for l in layers:
            layer_ids.append(l.id)
            layer_data.append((
                job.id,
                l.id,
                job.user_id,
                l.etag,
                l.runner_id,
                l.key,
                l.paused,
                json.dumps(l.metadata),
                l.state or enums.State.PENDING.value,
                l.priority,
                l.order,
                json.dumps(l.parents),
                json.dumps(l.siblings),
                json.dumps(l.children),
            ))

        for t in tasks:
            if t.layer_id not in layer_ids:
                raise exc.WriteConflictError(
                    f"layer for layer_id {t.layer_id} not supplied in job {job.id}"
                )

            task_data.append(self._task_creation_row(job.id, job.user_id, t))

        self._cur.execute(
            self.INSERT_JOB,
            (
                job.state or enums.State.PENDING.value,
                job.id,
                job.user_id,
                job.etag,
                job.runner_id,
                job.key,
                job.paused,
                json.dumps(job.metadata)
            )
        )

        self._cur.executemany(self.INSERT_LYR, layer_data)

        self._cur.executemany(self.INSERT_TSK, task_data)

    def update_job(
            self, job_id: str, etag: str, runner_id=None, metadata=None, state=None, **kwargs
    ) -> str:
        """

        :param job_id:
        :param etag:
        :param runner_id:
        :param metadata:
        :param state:
        :param paused:
        :return:

        """
        new_tag = str(uuid.uuid4())
        update = {
            "str_etag": new_tag,
            "time_updated": time.time(),
        }

        for n, v in [
            ("str_runner_id", runner_id),
            ("json_metadata", json.dumps(metadata) if metadata else None),
            ("enum_state", state),
        ]:
            if v is None:
                continue

            update[n] = v

        if 'paused' in kwargs:
            update["time_paused"] = kwargs['paused']

        sql, values = self._to_update_query(
            self._T_JOB, "str_job_id", job_id, etag, update
        )

        try:
            self._cur.execute(sql, values)
        except psycopg2.DataError:
            raise exc.InvalidState(f"state {state} is not a permitted state")

        if self._cur.rowcount != 1:
            raise exc.WriteConflictError(f'failed to update job {job_id} {etag}')

        return new_tag

    def force_delete_job(self, job_id: str):
        """Remove Job & dependent objects from db.

        :param job_id:

        """
        self._cur.execute(  # firstly, set any worker working on this job to null
            (
                f"UPDATE {self._T_WKR} "
                "SET "
                "str_job_id=null, "
                "str_layer_id=null, "
                "str_task_id=null, "
                "str_etag=%s, "
                "time_updated=%s "
                "WHERE str_job_id=%s;"
            ),
            (str(uuid.uuid4()), time.time(), job_id)
        )

        # now we can delete Tasks, Layers & finally the Job.
        self._cur.execute(f"DELETE FROM {self._T_TSK} WHERE str_job_id=%s;", (job_id,))
        self._cur.execute(f"DELETE FROM {self._T_LYR} WHERE str_job_id=%s;", (job_id,))
        self._cur.execute(f"DELETE FROM {self._T_JOB} WHERE str_job_id=%s;", (job_id,))

    def get_jobs(self, query: domain.Query) -> list:
        """Return all jobs matching the given query.

        :param query:
        :return: []domain.Job

        """
        sql, values = self._filter(
            query, self._T_JOB, job_id=True, state=True, key=True, user_id=True
        )

        results = []
        self._cur.execute(sql, values)

        for row in self._cur.fetchall():
            results.append(domain.Job.decode({
                k[k.index("_") + 1:]: v for k, v in row.items()
            }))

        return results

    def delete_jobs(self, job_ids: list):
        """Delete all jobs with IDs in the given list.

        :param job_ids:

        """
        sql = (
            f"DELETE FROM {self._T_JOB} "
            "WHERE str_job_id IN (%s);" % ", ".join(["%s"] * len(job_ids))
        )

        try:
            self._cur.execute(sql, job_ids)
        except psycopg2.IntegrityError:
            raise exc.ChildExists("child layer(s) exist: unable to delete")

    def update_layer(
        self, layer_id: str, etag: str, priority=None, state=None, runner_id=None,
        metadata=None, **kwargs
    ) -> str:
        """

        :param layer_id:
        :param etag:
        :param priority:
        :param state:
        :param runner_id:
        :param metadata:
        :param paused:
        :return: str

        """
        new_tag = str(uuid.uuid4())
        update = {
            "str_etag": new_tag,
            "time_updated": time.time(),
        }

        if priority:
            priority = abs(priority)

        for n, v in [
            ("str_runner_id", runner_id),
            ("json_metadata", json.dumps(metadata) if metadata else None),
            ("enum_state", state),
            ("int_priority", priority),
        ]:
            if v is None:
                continue

            update[n] = v

        if 'paused' in kwargs:
            update["time_paused"] = kwargs['paused']

        sql, values = self._to_update_query(
            self._T_LYR, "str_layer_id", layer_id, etag, update
        )

        try:
            self._cur.execute(sql, values)
            self._conn.commit()
        except psycopg2.DataError:
            raise exc.InvalidState(f"state {state} is not a permitted state")

        if self._cur.rowcount != 1:
            raise exc.WriteConflictError(f'failed to update layer {layer_id} {etag}')

        return new_tag

    def get_layers(self, query: domain.Query) -> list:
        """Return all layers matching the given query.

        :param query:
        :return: []domain.Layer

        """
        sql, values = self._filter(
            query,
            self._T_LYR,
            job_id=True,
            layer_id=True,
            state=True,
            key=True,
            user_id=True,
        )

        results = []
        self._cur.execute(sql, values)

        for row in self._cur.fetchall():
            results.append(domain.Layer.decode({
                k[k.index("_") + 1:]: v for k, v in row.items()
            }))

        return results

    def create_tasks(self, layer_id: str, tasks: list):
        """

        :param layer_id:
        :param tasks:

        """
        layers = self.get_layers(
            domain.Query(limit=1, filters=[domain.Filter(layer_ids=[layer_id])])
        )
        if not layers:
            raise exc.LayerNotFound(layer_id)

        job_id = layers[0].job_id
        user_id = layers[0].user_id

        task_data = []
        for t in tasks:
            task_data.append(self._task_creation_row(job_id, user_id, t, layer_id=layer_id))

        try:
            self._cur.executemany(self.INSERT_TSK, task_data)
        except psycopg2.IntegrityError:
            raise exc.WriteConflictError(
                f"integrity err: failed to create tasks for layer: {layer_id}"
            )

        if self._cur.rowcount != len(tasks):
            raise exc.WriteConflictError(f'failed to create tasks for layer: {layer_id}')

    @staticmethod
    def _to_binary(value) -> psycopg2.Binary:
        """Encode whatever value is given to be a postgres Binary obj.

        This assumes the given value is a python simple type of some sort.

        :param value: ?
        :return: psycopg2.Binary

        """
        if value is None:
            return psycopg2.Binary(None)
        if isinstance(value, (str, bool, int, float, list, dict)):
            return psycopg2.Binary(bytes(str(value), encoding="utf8"))
        if isinstance(value, bytes):
            # avoid double encoding ie. b'b'mydata''
            return psycopg2.Binary(bytes(str(value, encoding="utf8"), encoding="utf8"))
        raise exc.InvalidArg(f"result type unsupported {type(value)}")

    def update_task(
            self,
            task_id: str,
            etag: str,
            runner_id=None,
            metadata=None,
            state=None,
            attempts=None,
            env=None,
            **kwargs
    ) -> str:
        """

        :param task_id:
        :param etag:
        :param runner_id:
        :param metadata:
        :param state:
        :param attempts:
        :param env:
        :param result:
        :param worker_id:
        :return: str

        """
        new_tag = str(uuid.uuid4())
        update = {
            "str_etag": new_tag,
            "time_updated": time.time(),
        }

        for n, v in [
            ("str_runner_id", runner_id),
            ("json_metadata", json.dumps(metadata) if metadata else None),
            ("enum_state", state),
            ("int_attempts", attempts),
            ("json_env", json.dumps(env) if env else None),
        ]:
            if v is None:
                continue

            update[n] = v

        if "worker_id" in kwargs:  # allow setting to none
            update["str_worker_id"] = kwargs["worker_id"]

        if 'paused' in kwargs:
            update["time_paused"] = kwargs['paused']

        if 'result' in kwargs:
            update["bytea_result"] = self._to_binary(kwargs["result"])

        sql, values = self._to_update_query(
            self._T_TSK, "str_task_id", task_id, etag, update
        )

        try:
            self._cur.execute(sql, values)
        except psycopg2.DataError:
            raise exc.InvalidState(f"state {state} is not a permitted state")

        if self._cur.rowcount != 1:
            raise exc.WriteConflictError(f'failed to update task {task_id} {etag}')

        return new_tag

    def get_tasks(self, query: domain.Query) -> list:
        """Return all tasks matching the given query.

        :param query:
        :return: []domain.Task

        """
        tasks = {}

        sql, values = self._filter(  # fetch task data
            query,
            self._T_TSK,
            job_id=True,
            layer_id=True,
            task_id=True,
            state=True,
            key=True,
            user_id=True,
        )
        self._cur.execute(sql, values)
        for row in self._cur.fetchall():
            data = {k[k.index("_") + 1:]: v for k, v in row.items()}
            tasks[data["task_id"]] = data

        records = collections.defaultdict(list)
        sql, values = self._filter(  # fetch record data
            query,
            self._T_REC,
            task_id=True,
        )
        self._cur.execute(sql, values)
        for row in self._cur.fetchall():
            data = {k[k.index("_") + 1:]: v for k, v in row.items()}
            records[data["task_id"]].append(data)

        results = []
        for task_id, task_data in tasks.items():
            t = domain.Task.decode(task_data)
            t.result = str(bytes(t.result), encoding="utf8") if t.result else None
            t.records = records.get(task_id, [])
            results.append(t)

        return results

    def get_workers(self, query: domain.Query) -> list:
        """Return all workers matching the given query.

        :param query:
        :return: []domain.Worker

        """
        sql, values = self._filter(
            query,
            self._T_WKR,
            job_id=True,
            layer_id=True,
            task_id=True,
            worker_id=True,
            key=True,
        )

        results = []
        self._cur.execute(sql, values)

        for row in self._cur.fetchall():
            results.append(domain.Worker.decode({
                k[k.index("_") + 1:]: v for k, v in row.items()
            }))

        return results

    def idle_worker_count(self) -> int:
        """Return the number of idle workers.

        :return: int

        """
        self._cur.execute(f"SELECT count(*) FROM {self._T_WKR} WHERE str_task_id is null;")
        return self._cur.fetchone()[0]

    def idle_workers(self, limit: int = _DEFAULT_LIMIT, offset: int = 0) -> list:
        """Return all idle workers, obeying the given limits.

        :param limit:
        :param offset:
        :return: []domain.Worker

        """
        results = []

        self._cur.execute(
            (
                f"SELECT * FROM {self._T_WKR} "
                f"WHERE str_task_id is null "
                f"ORDER BY time_created "
                f"LIMIT %s OFFSET %s;"
            ),
            (limit, offset)
        )

        for row in self._cur.fetchall():
            results.append(domain.Worker.decode({
                k[k.index("_") + 1:]: v for k, v in row.items()
            }))

        return results

    INSERT_WKR = f"""INSERT INTO {_T_WKR} (
        str_worker_id,
        str_etag,
        str_key,
        str_host,
        json_metadata) VALUES (%s, %s, %s, %s, %s);"""

    def create_worker(self, worker: domain.Worker):
        """

        :param worker:
        :return: str

        """
        self._cur.execute(
            self.INSERT_WKR,
            (worker.id, worker.etag, worker.key, worker.host, json.dumps(worker.metadata))
        )

    def delete_worker(self, id_: str):
        """Delete a worker from the database.

        - Also nulls any task worker_id fields currently set to this worker id.

        :param id_:

        """
        self._cur.execute(  # firstly, set any worker working on this job to null
            (
                f"UPDATE {self._T_TSK} "
                "SET "
                "str_worker_id=null, "
                "str_etag=%s, "
                "time_updated=%s "
                "WHERE str_worker_id=%s;"
            ),
            (str(uuid.uuid4()), time.time(), id_)
        )

        self._cur.execute(f"DELETE FROM {self._T_WKR} WHERE str_worker_id=%s;", (id_,))

    def update_worker(
        self,
        worker_id: str,
        etag: str,
        task_started=None,
        task_finished=None,
        last_ping=None,
        metadata=None,
        **kwargs
    ) -> str:
        """

        :param worker_id:
        :param etag:
        :param job_id:
        :param layer_id:
        :param task_id:
        :param task_started:
        :param task_finished:
        :param last_ping:
        :param metadata:
        :return: str

        """
        new_tag = str(uuid.uuid4())
        update = {
            "str_etag": new_tag,
            "time_updated": time.time(),
        }

        for n, v in [
            ("time_task_started", task_started),
            ("time_task_finished", task_finished),
            ("time_last_ping", last_ping),
            ("json_metadata", json.dumps(metadata) if metadata else None),
        ]:
            if v is None:
                continue

            update[n] = v

        for n in ["job_id", "layer_id", "task_id"]:  # allow setting of None
            if n in kwargs:
                update["str_" + n] = kwargs[n]

        sql, values = self._to_update_query(
            self._T_WKR, "str_worker_id", worker_id, etag, update
        )

        self._cur.execute(sql, values)

        if self._cur.rowcount != 1:
            raise exc.WriteConflictError(f'failed to update worker {worker_id} {etag}')

        return new_tag

    @staticmethod
    def _task_creation_row(job_id: str, user_id: str, t: domain.Task, layer_id=None) -> tuple:
        """Return data formatted for writing into db.

        :param job_id:
        :param user_id:
        :param t:
        :return: tuple

        """
        return (
            t.state or enums.State.PENDING.value,
            job_id,
            layer_id or t.layer_id,
            t.id,
            user_id,
            t.etag,
            t.runner_id,
            t.key,
            t.paused,
            json.dumps(t.metadata),
            json.dumps(t.cmd),
            json.dumps(t.env),
            t.max_attempts
        )

    @staticmethod
    def _to_update_query(table_name, id_column, id_, etag, key_values) -> (str, list):
        """Return a dict of key-value pairs as a
            UPDATE table_name SET k=x, k=x WHERE id_column=id_ AND etag=etag
        and a list of values.

        :param key_values:
        :return: str, list

        """
        sets = []
        values = []
        for k, v in key_values.items():
            sets.append(f"{k}=%s")
            values.append(v)

        update = ", ".join(sets)
        values.append(id_)

        sql = f"UPDATE {table_name} SET {update} WHERE {id_column}=%s"
        if etag:
            sql += " AND str_etag=%s"
            values.append(etag)

        return sql, values

    @classmethod
    def _sort_column(cls, name) -> str:
        """Return sql column name for given name.

        :param name:
        :return: str

        """
        if not name:
            return "time_created"
        elif name == 'state':
            return f"enum_state"
        elif name in ["priority", "order"]:
            return f"int_{name}"
        elif name in [
            "job_id", "layer_id", "task_id", "worker_id", "state", "key", "name", "host", "etag"
        ]:
            return f"str_{name}"
        elif name in ["created", "updated", "paused"]:
            return f"time_{name}"
        else:
            return "time_created"

    @classmethod
    def _filter(
        cls,
        q: domain.Query,
        table_name: str,
        job_id=False,
        layer_id=False,
        task_id=False,
        worker_id=False,
        state=False,
        key=False,
        user_id=False,
    ) -> (str, list):
        """Builds a Postgres sql query

        :param q:
        :param job_id: include checks for job ids
        :param layer_id: include checks for layer ids
        :param task_id: include checks for task ids
        :param worker_id: include checks for worker ids
        :param state: include checks for object state(s)
        :param key: include checks for object key(s)
        :param key: include checks for user_id
        :return: str, list

        """
        q.limit = min([_MAX_LIMIT, q.limit])
        sort_by = cls._sort_column(q.sorting)

        if not q.filters:
            if user_id and q.user_id:
                return (
                    f"SELECT * FROM {table_name} "
                    "WHERE str_user_id=%s "
                    f"ORDER BY {sort_by} "
                    "LIMIT %s OFFSET %s;"
                ), [q.user_id, q.limit, q.offset]

            return (
                f"SELECT * FROM {table_name} "
                f"ORDER BY {sort_by} "
                "LIMIT %s OFFSET %s;"
            ), [q.limit, q.offset]

        ors = []
        values = []
        for f in q.filters:
            sub_and = []

            if user_id and q.user_id:
                sub_and.append("str_user_id=%s")
                values.append(q.user_id)

            if job_id and f.job_ids:
                sub_and.append("str_job_id IN (%s)" % ", ".join(["%s"] * len(f.job_ids)))
                values.extend(f.job_ids)

            if layer_id and f.layer_ids:
                sub_and.append("str_layer_id IN (%s)" % ", ".join(["%s"] * len(f.layer_ids)))
                values.extend(f.layer_ids)

            if task_id and f.task_ids:
                sub_and.append("str_task_id IN (%s)" % ", ".join(["%s"] * len(f.task_ids)))
                values.extend(f.task_ids)

            if worker_id and f.worker_ids:
                sub_and.append("str_worker_id IN (%s)" % ", ".join(["%s"] * len(f.worker_ids)))
                values.extend(f.worker_ids)

            if state and f.states:
                sub_and.append("enum_state IN (%s)" % ", ".join(["%s"] * len(f.states)))
                values.extend(f.states)

            if key and f.keys:
                sub_and.append("str_key IN (%s)" % ", ".join(["%s"] * len(f.keys)))
                values.extend(f.keys)

            if sub_and:
                ors.append("(%s)" % " AND ".join(sub_and))

        where = ""
        if ors:
            where = "WHERE " + " OR ".join(ors)

        return (
           f"SELECT * FROM {table_name} "
           f"{where} "
           f"ORDER BY {sort_by} "
           "LIMIT %s OFFSET %s;"
       ), values + [q.limit, q.offset]


class PostgresDB(Base):
    # database
    _DB = "igor"
    _CONNECT_ATTEMPTS = 3

    def __init__(self, host='database', port=5432, user="postgres"):
        self._host = host
        self._port = port
        self._user = user
        self.__conn = None

    @property
    def _conn(self):
        if not self.__conn:
            for i in range(0, max([1, self._CONNECT_ATTEMPTS])):
                try:
                    self.__conn = psycopg2.connect(
                        host=self._host,
                        port=self._port,
                        database=self._DB,
                        user=self._user,
                        cursor_factory=DictCursor,
                    )
                except Exception as e:
                    print("Error connecting to database:", str(e), file=sys.stderr)
                    time.sleep((2 ** i) + (random.randint(0, 1000) / 1000.0))
        return self.__conn

    def create_task_record(self, tsk: domain.Task, wkr_id: str, state, reason=""):
        """

        :param tsk:
        :param wkr_id:
        :param state:
        :param reason:

        """
        with self.transaction() as t:
            t.create_task_record(tsk, wkr_id, state, reason=reason)

    def transaction(self):
        """

        """
        return _Transaction(self._conn)

    def create_job(self, job: domain.Job, layers: list, tasks: list):
        """

        :param job:
        :param layers:
        :param tasks:

        """
        with self.transaction() as t:
            return t.create_job(job, layers, tasks)

    def update_job(
        self,
        job_id: str,
        etag: str,
        **kwargs,
    ) -> str:
        """

        :param job_id:
        :param etag:
        :param runner_id:
        :param metadata:
        :param state:
        :param paused:
        :return: str

        """
        with self.transaction() as t:
            return t.update_job(job_id, etag, **kwargs)

    def force_delete_job(self, job_id: str):
        """Delete job, it's layers & tasks.

        :param job_id:

        """
        with self.transaction() as t:
            return t.force_delete_job(job_id)

    def get_jobs(self, query: domain.Query) -> list:
        """

        :param query:
        :return: list

        """
        with self.transaction() as t:
            return t.get_jobs(query)

    def delete_jobs(self, job_ids: list):
        """

        :param job_ids:

        """
        with self.transaction() as t:
            return t.delete_jobs(job_ids)

    def update_layer(
        self,
        layer_id: str,
        etag: str,
        **kwargs,
    ) -> str:
        """

        :param layer_id:
        :param etag:
        :param priority:
        :param state:
        :param runner_id:
        :param metadata:
        :param paused:
        :return: str

        """
        with self.transaction() as t:
            return t.update_layer(layer_id, etag, **kwargs)

    def get_layers(self, query: domain.Query) -> list:
        """

        :param query:
        :return: list

        """
        with self.transaction() as t:
            return t.get_layers(query)

    def create_tasks(self, layer_id: str, tasks: list):
        """

        :param layer_id:
        :param tasks:

        """
        with self.transaction() as t:
            return t.create_tasks(layer_id, tasks)

    def update_task(
        self,
        task_id: str,
        etag: str,
        **kwargs,
    ) -> str:
        """

        :param task_id:
        :param etag:
        :param worker_id:
        :param runner_id:
        :param metadata:
        :param state:
        :param attempts:
        :param paused:
        :param env:
        :return: str

        """
        with self.transaction() as t:
            return t.update_task(task_id, etag, **kwargs)

    def get_tasks(self, query: domain.Query) -> list:
        """

        :param query:
        :return: list

        """
        with self.transaction() as t:
            return t.get_tasks(query)

    def get_workers(self, query: domain.Query) -> list:
        """

        :param query:
        :return: list

        """
        with self.transaction() as t:
            return t.get_workers(query)

    def idle_worker_count(self) -> int:
        """

        :return: list

        """
        with self.transaction() as t:
            return t.idle_worker_count()

    def idle_workers(self, limit: int=_DEFAULT_LIMIT, offset: int=0) -> list:
        """

        :param limit:
        :param offset:
        :return: list

        """
        with self.transaction() as t:
            return t.idle_workers(limit=limit, offset=offset)

    def create_worker(self, worker: domain.Worker):
        """Create worker in system with given data.

        :param worker:

        """
        with self.transaction() as t:
            return t.create_worker(worker)

    def delete_worker(self, id_: str):
        """Remove worker with matching id.

        :param id_:

        """
        with self.transaction() as t:
            t.delete_worker(id_)

    def update_worker(
        self,
        worker_id: str,
        etag: str,
        **kwargs
    ) -> str:
        """

        :param worker_id:
        :param etag:
        :param job_id:
        :param layer_id:
        :param task_id:
        :param task_started:
        :param task_finished:
        :param last_ping:
        :param metadata:
        :return: str

        """
        with self.transaction() as t:
            return t.update_worker(worker_id, etag, **kwargs)

    def create_user(self, user: domain.User):
        """Create user.

        :param user:

        """
        with self.transaction() as t:
            return t.create_user(user)

    def delete_user(self, user_id: str):
        """Delete user.

        :param user_id:

        """
        with self.transaction() as t:
            return t.delete_user(user_id)

    def update_user(self, user_id: str, etag: str, **kwargs):
        """Update single user instance.

        :param user_id:
        :param etag:
        :param name:
        :param password: this should be the *hashed* password.
        :param metadata:

        """
        with self.transaction() as t:
            return t.update_user(user_id, etag, **kwargs)

    def get_users(self, query: domain.Query) -> list:
        """Get users workers matching query.

        :param query:
        :return: list

        """
        with self.transaction() as t:
            return t.get_users(query)
