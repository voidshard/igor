"""The database layer is exactly what it sounds like - it just provides some CRUD operations
for persistent storage.

"""

from igor.database.postgres_impl import PostgresDB


__all__ = [
    "PostgresDB",
]
