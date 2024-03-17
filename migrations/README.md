DB Migrations for Postgres.


/dev contains migrations suitable for testing; it creates a database, user(s) with default weak passwords (again, for testing), schema etc.

/prod creates our main tables & functions, implies database, schema & users exist already.
