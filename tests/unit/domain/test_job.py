import uuid

from igor import domain


class TestJob:

    def test_encode_decode(self):
        # arrange
        expected = {
            "user_id": str(uuid.uuid4()),
            "id": str(uuid.uuid4()),
            "etag": str(uuid.uuid4()),
            "state": str(uuid.uuid4()),
            "key": str(uuid.uuid4()),
            "runner_id": str(uuid.uuid4()),
            "metadata": {
                str(uuid.uuid4()): str(uuid.uuid4()),
                str(uuid.uuid4()): [str(uuid.uuid4()), str(uuid.uuid4())],
                str(uuid.uuid4()): True,
                str(uuid.uuid4()): 12112,
            },
        }

        # act
        j = domain.Job.decode(expected)

        result = j.encode()

        # assert
        assert result == expected
        assert isinstance(j, domain.Job)
        assert isinstance(result, dict)
