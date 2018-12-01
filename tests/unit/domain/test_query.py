import random
import uuid

from igor import domain


def _random_filter():
    return {
        "job_ids": [str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4())],
        "layer_ids": [str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4())],
        "task_ids": [str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4())],
        "worker_ids": [str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4())],
        "states": [str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4())],
        "keys": [str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4())],
    }


class TestFilter:

    def test_encode_decode(self):
        # arrange
        expected = _random_filter()

        # act
        i = domain.Filter.decode(expected)

        result = i.encode()

        # assert
        assert result == expected
        assert isinstance(i, domain.Filter)
        assert isinstance(result, dict)


class TestQuery:
    def test_encode_decode(self):
        # arrange
        expected = {
            "filters": [_random_filter(), _random_filter()],
            "limit": random.randint(10, 100),
            "offset": random.randint(10, 100),
            "sorting": "some_column_to_sort_by",
            "user_id": str(uuid.uuid4()),
        }

        # act
        i = domain.Query.decode(expected)

        result = i.encode()

        # assert
        assert result == expected
        assert isinstance(i, domain.Query)
        assert isinstance(result, dict)
