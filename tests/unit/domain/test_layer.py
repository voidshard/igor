import time
import random
import uuid

from igor import domain


class TestLayer:

    def test_encode_decode(self):
        # arrange
        expected = {
            "user_id": str(uuid.uuid4()),
            "layer_id": str(uuid.uuid4()),
            "paused": int(time.time()),
            "job_id": str(uuid.uuid4()),
            "key": str(uuid.uuid4()),
            "state": "FUBAR",
            "priority": random.randint(10, 1200),
            "etag": str(uuid.uuid4()),
            "order": random.randint(10, 1000),
            "runner_id": str(uuid.uuid4()),
            "metadata": {
                str(uuid.uuid4()): str(uuid.uuid4()),
                str(uuid.uuid4()): [str(uuid.uuid4()), str(uuid.uuid4())],
                str(uuid.uuid4()): True,
                str(uuid.uuid4()): 12112,
            },
            "parents": [str(uuid.uuid4()), str(uuid.uuid4())],
            "siblings": [str(uuid.uuid4())],
            "children": [],
        }

        # act
        i = domain.Layer.decode(expected)

        result = i.encode()

        # assert
        assert result == expected
        assert isinstance(i, domain.Layer)
        assert isinstance(result, dict)
