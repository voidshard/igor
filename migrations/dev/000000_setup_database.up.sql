-- Should be run as pg admin user (to set up schema & roles)

--- Setup database
CREATE DATABASE igor;

CREATE USER igorreadwrite WITH PASSWORD 'readwrite'; 
GRANT CONNECT ON DATABASE igor TO igorreadwrite;
GRANT ALL PRIVILEGES ON DATABASE igor TO igorreadwrite;

CREATE USER igorreadonly WITH PASSWORD 'readonly';
GRANT CONNECT ON DATABASE igor TO igorreadonly;

--- Setup schema and roles, implies we are in the igor database
\connect igor
CREATE SCHEMA IF NOT EXISTS igor;

ALTER SCHEMA igor OWNER TO igorreadwrite;
ALTER USER igorreadwrite SET search_path='igor';
GRANT USAGE ON SCHEMA igor TO igorreadonly;
ALTER USER igorreadonly SET search_path='igor';

SET search_path TO igor;
