import hashlib
import uuid

from igor.domain.base import Serializable


class User(Serializable):

    def __init__(self, name=None):
        super(User, self).__init__()
        self.id = str(uuid.uuid4())
        self.etag = str(uuid.uuid4())
        self.name = name
        self._password = None
        self.metadata = {}
        self.is_admin = False

    @property
    def password(self) -> str:
        """Return password string

        :return: str

        """
        return self._password

    def is_password(self, value) -> bool:
        """

        :param value:
        :return: bool

        """
        return self._hash(value) == self.password

    @staticmethod
    def _hash(value) -> str:
        """Hash some string and return a hex digest string.

        :param value: str
        :return: str

        """
        # TODO: this isn't salted :(
        return hashlib.sha512(value.encode("utf8")).hexdigest()

    @password.setter
    def password(self, value):
        """Hash & set password

        """
        self._password = self._hash(value)

    def encode(self) -> dict:
        """

        :return: dict

        """
        return {
            "user_id": self.id,
            "etag": self.etag,
            "metadata": self.metadata,
            "key": self.name,
            "is_admin": self.is_admin,
            "password": self.password,
        }

    @classmethod
    def decode(cls, data: dict):
        """

        :param data:
        :return: Job
        :raises ValueError: if data represents an invalid job

        """
        me = cls()
        me.id = data.get("user_id")
        me.name = data.get("key")
        me.is_admin = data.get("is_admin", False)
        me._password = data.get("password")
        me.etag = data.get("etag")
        me.metadata = data.get("metadata", {})

        if not all([me.id, me.name, me.password]):
            raise ValueError('require all of id, name, password to inflate obj')

        return me
