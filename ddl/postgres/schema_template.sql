--- Holds functions
CREATE OR REPLACE LANGUAGE plpgsql;

---
--- Setup encoding
---
CREATE DATABASE igor WITH ENCODING 'UTF8';

\connect igor

---
--- States defined for use as enums
---
CREATE TYPE STATE AS ENUM (
    'PENDING', 'QUEUED', 'RUNNING', 'COMPLETED', 'ERRORED', 'SKIPPED');


---
--- Func for current time (seconds)
---
CREATE OR REPLACE FUNCTION currentTimeSeconds() RETURNS BIGINT AS $$
BEGIN
    return (EXTRACT(EPOCH FROM clock_timestamp()))::bigint;
END;
$$ LANGUAGE plpgsql;



---
--- users
---  the 'key' here is the username
---
CREATE TABLE users(
	str_user_id TEXT NOT NULL PRIMARY KEY,
	str_etag TEXT NOT NULL,
	str_key TEXT UNIQUE NOT NULL,
	bool_is_admin BOOLEAN NOT NULL DEFAULT FALSE,
	json_metadata JSONB NOT NULL DEFAULT '{}',
	time_created BIGINT NOT NULL DEFAULT currentTimeSeconds(),
	time_updated BIGINT NOT NULL DEFAULT currentTimeSeconds(),
	str_password TEXT NOT NULL) WITH (OIDS = false);



---
--- jobs
---
CREATE TABLE jobs(
	str_job_id TEXT NOT NULL PRIMARY KEY,
	str_user_id TEXT NOT NULL REFERENCES users(str_user_id),
	str_runner_id TEXT,
	str_etag TEXT NOT NULL,
	enum_state STATE NOT NULL DEFAULT 'PENDING',
	time_created BIGINT NOT NULL DEFAULT currentTimeSeconds(),
	time_updated BIGINT NOT NULL DEFAULT currentTimeSeconds(),
	json_metadata JSONB NOT NULL DEFAULT '{}',
	time_paused BIGINT,
	str_key TEXT) WITH (OIDS = false);


---
--- layers
---
CREATE TABLE layers(
	str_job_id TEXT NOT NULL REFERENCES jobs(str_job_id),
	str_user_id TEXT NOT NULL REFERENCES users(str_user_id),
	str_runner_id TEXT,
	str_etag TEXT NOT NULL,
	enum_state STATE NOT NULL DEFAULT 'PENDING',
	time_created BIGINT NOT NULL DEFAULT currentTimeSeconds(),
	time_updated BIGINT NOT NULL DEFAULT currentTimeSeconds(),
	json_metadata JSONB NOT NULL DEFAULT '{}',
	time_paused BIGINT,
	str_layer_id TEXT NOT NULL PRIMARY KEY,
	int_priority INT NOT NULL DEFAULT 0,
	int_order INT NOT NULL DEFAULT 0,
	json_parents JSONB NOT NULL DEFAULT '[]',
	json_siblings JSONB NOT NULL DEFAULT '[]',
	json_children JSONB NOT NULL DEFAULT '[]',
	str_key TEXT) WITH (OIDS = false);


---
--- workers
---
CREATE TABLE workers(
	str_job_id TEXT REFERENCES jobs(str_job_id),
	str_etag TEXT NOT NULL,
	time_created BIGINT NOT NULL DEFAULT currentTimeSeconds(),
	time_updated BIGINT NOT NULL DEFAULT currentTimeSeconds(),
	str_layer_id TEXT REFERENCES layers(str_layer_id),
	str_task_id TEXT,
	str_worker_id TEXT NOT NULL PRIMARY KEY,
	str_key TEXT,
	time_task_started BIGINT,
	time_task_finished BIGINT,
	time_last_ping BIGINT NOT NULL DEFAULT currentTimeSeconds(),
	str_host TEXT NOT NULL,
	json_metadata JSONB NOT NULL DEFAULT '{}') WITH (OIDS = false);


---
--- tasks
---
CREATE TABLE tasks(
	str_job_id TEXT NOT NULL REFERENCES jobs(str_job_id),
	str_user_id TEXT NOT NULL REFERENCES users(str_user_id),
	str_runner_id TEXT,
	str_etag TEXT NOT NULL,
	enum_state STATE NOT NULL DEFAULT 'PENDING',
	time_created BIGINT NOT NULL DEFAULT currentTimeSeconds(),
	time_updated BIGINT NOT NULL DEFAULT currentTimeSeconds(),
	json_metadata JSONB NOT NULL DEFAULT '{}',
	time_paused BIGINT,
	str_layer_id TEXT NOT NULL REFERENCES layers(str_layer_id),
	str_task_id TEXT PRIMARY KEY,
	str_worker_id TEXT REFERENCES workers(str_worker_id),
	str_key TEXT,
	json_cmd JSONB NOT NULL,
	json_env JSONB NOT NULL DEFAULT '{}',
	int_attempts INT NOT NULL DEFAULT 0,
	int_max_attempts INT NOT NULL DEFAULT 3) WITH (OIDS = false);

---
--- now we need to add one more constraint ...
---
ALTER TABLE workers ADD CONSTRAINT tasks_str_task_id_fk FOREIGN KEY (str_task_id) REFERENCES tasks(str_task_id);


---
--- Finally, create some indexes
---
CREATE INDEX users_str_user_id ON users(str_user_id);

CREATE INDEX jobs_str_job_id ON jobs(str_job_id);
CREATE INDEX jobs_str_user_id ON jobs(str_user_id);
CREATE INDEX jobs_enum_state ON jobs(enum_state);
CREATE INDEX jobs_str_key ON jobs(str_key);

CREATE INDEX layers_str_job_id ON layers(str_job_id);
CREATE INDEX layers_str_layer_id ON layers(str_layer_id);
CREATE INDEX layers_str_user_id ON layers(str_user_id);
CREATE INDEX layers_enum_state ON layers(enum_state);
CREATE INDEX layers_str_key ON layers(str_key);

CREATE INDEX tasks_str_job_id ON tasks(str_job_id);
CREATE INDEX tasks_str_layer_id ON tasks(str_layer_id);
CREATE INDEX tasks_str_task_id ON tasks(str_task_id);
CREATE INDEX tasks_str_user_id ON tasks(str_user_id);
CREATE INDEX tasks_enum_state ON tasks(enum_state);
CREATE INDEX tasks_str_key ON tasks(str_key);

CREATE INDEX workers_str_job_id ON workers(str_job_id);
CREATE INDEX workers_str_layer_id ON workers(str_layer_id);
CREATE INDEX workers_str_task_id ON workers(str_task_id);
CREATE INDEX workers_str_worker_id ON workers(str_worker_id);
CREATE INDEX workers_str_key ON workers(str_key);
