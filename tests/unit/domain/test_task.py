import copy
import time
import random
import uuid

from igor import domain
from igor import enums


class TestTask:

    def test_work_record_update(self, monkeypatch):
        # arrange
        t = domain.Task()
        t.metadata["records"] = [{"foo": "bar"}]

        worker_id = str(uuid.uuid4())
        worker_host = "12.123.73.1"
        task_state = "OFOBAW"
        reason = "cause i said so thats why"

        def time_now():
            return 123891231.12

        expect = copy.copy(t.metadata)
        expect["records"].append({
            "worker": worker_id,
            "host": worker_host,
            "time": time_now(),
            "state": task_state,
            "reason": reason,
        })

        monkeypatch.setattr("igor.domain.task.time.time", time_now)

        # act
        new_meta = t.work_record_update(
            worker_id,
            worker_host,
            task_state,
            reason=reason,
        )

        # assert
        assert new_meta == expect
        assert t.metadata == expect

    def test_new(self):
        # arrange
        type_ = enums.TaskType.CMD.value
        cmd = ["foobar", "-x", str(uuid.uuid4())]
        env = {
            str(uuid.uuid4()): str(uuid.uuid4()),
            str(uuid.uuid4()): str(uuid.uuid4()),
            str(uuid.uuid4()): str(uuid.uuid4()),
        }
        meta = {
            str(uuid.uuid4()): str(uuid.uuid4()),
            str(uuid.uuid4()): [str(uuid.uuid4()), str(uuid.uuid4())],
            str(uuid.uuid4()): True,
            str(uuid.uuid4()): random.randint(10, 98369),
        }
        key = str(uuid.uuid4())

        # act
        result = domain.Task.new(type_, cmd, env, metadata=meta, key=key)

        # assert
        assert result.id
        assert result.state == enums.State.PENDING.value
        assert result.type == type_
        assert result.cmd == cmd
        assert result.env == env
        assert result.key == key

    def test_encode_decode(self):
        # arrange
        expected = {
            "user_id": str(uuid.uuid4()),
            "task_id": str(uuid.uuid4()),
            "job_id": str(uuid.uuid4()),
            "paused": int(time.time()),
            "layer_id": str(uuid.uuid4()),
            "result": str(uuid.uuid4()),
            "key": str(uuid.uuid4()),
            "state": "FUBAR",
            "etag": str(uuid.uuid4()),
            "runner_id": str(uuid.uuid4()),
            "metadata": {
                str(uuid.uuid4()): str(uuid.uuid4()),
                str(uuid.uuid4()): [str(uuid.uuid4()), str(uuid.uuid4())],
                str(uuid.uuid4()): True,
                str(uuid.uuid4()): random.randint(10, 98369),
            },
            "type": "SOMETYPE",
            "cmd": ["print", "vooo", "-x"],
            "env": {
                str(uuid.uuid4()): str(uuid.uuid4()),
                str(uuid.uuid4()): str(uuid.uuid4()),
                str(uuid.uuid4()): str(uuid.uuid4()),
                str(uuid.uuid4()): str(uuid.uuid4()),
                str(uuid.uuid4()): str(uuid.uuid4()),
            },
            "worker_id": str(uuid.uuid4()),
            "attempts": random.randint(1, 5),
            "max_attempts": random.randint(10, 100),
        }

        # act
        i = domain.Task.decode(expected)

        result = i.encode()

        # assert
        assert result == expected
        assert isinstance(i, domain.Task)
        assert isinstance(result, dict)
