import random
import time
import uuid

from igor import domain


class TestWorker:

    def test_encode_decode(self):
        # arrange
        expected = {
            "id": str(uuid.uuid4()),
            "last_ping": time.time() - random.randint(10, 100),
            "host": "39.1.21.1",
            "job_id": str(uuid.uuid4()),
            "layer_id": str(uuid.uuid4()),
            "task_id": str(uuid.uuid4()),
            "key": str(uuid.uuid4()),
            "etag": str(uuid.uuid4()),
            "metadata": {
                str(uuid.uuid4()): str(uuid.uuid4()),
                str(uuid.uuid4()): [str(uuid.uuid4()), str(uuid.uuid4())],
                str(uuid.uuid4()): True,
                str(uuid.uuid4()): 12112,
            },
            "task_started": time.time(),
            "task_finished": time.time() + random.randint(10, 100),
        }

        # act
        i = domain.Worker.decode(expected)

        result = i.encode()

        # assert
        assert result == expected
        assert isinstance(i, domain.Worker)
        assert isinstance(result, dict)
