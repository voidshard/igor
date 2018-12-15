import uuid

from igor import domain


class TestUser:

    def test_encode_decode(self):
        # arrange
        expected = {
            "is_admin": True,
            "password": "blah",
            "user_id": str(uuid.uuid4()),
            "etag": str(uuid.uuid4()),
            "key": str(uuid.uuid4()),
            "metadata": {
                str(uuid.uuid4()): str(uuid.uuid4()),
                str(uuid.uuid4()): [str(uuid.uuid4()), str(uuid.uuid4())],
                str(uuid.uuid4()): True,
                str(uuid.uuid4()): 12112,
            },
        }

        # act
        j = domain.User.decode(expected)

        result = j.encode()

        # assert
        assert result == expected
        assert isinstance(j, domain.User)
        assert isinstance(result, dict)
