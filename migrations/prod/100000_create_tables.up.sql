-- Should be run as igor read-write user 
-- (so this user owns tables)

-- notify_event function sends a JSON notification to channel 'igor_events' on insert, update, or delete.
-- We only need this on Layer and Task tables - this is obviously expensive to do, but considered less
-- expensive than polling the database for changes (and a great deal easier / less promlematic).
CREATE OR REPLACE FUNCTION notify_event() RETURNS TRIGGER AS $$
    DECLARE 
        old_data json;
	new_data json;
        notification json;
    BEGIN
        -- Convert the old or new row to JSON, based on the kind of action.
        -- Args as bytea is not a valid JSON type .. so we remove it
        -- we actually don't need it for deciding what to do with the event anyway
        IF (TG_OP = 'DELETE') THEN
            old_data = row_to_json(OLD)::jsonb - 'args';
	    new_data = NULL;
	ELSIF (TG_OP = 'INSERT') THEN
	    old_data = NULL;
	    new_data = row_to_json(NEW)::jsonb - 'args';
        ELSE
	    old_data = row_to_json(OLD)::jsonb - 'args';
            new_data = row_to_json(NEW)::jsonb - 'args';
        END IF;
        -- Contruct the notification as a JSON string.
        notification = json_build_object('table',TG_TABLE_NAME, 'old', old_data, 'new', new_data);
        -- Execute pg_notify(channel, notification)
        PERFORM pg_notify('igor_events',notification::text);
        -- Result is ignored since this is an AFTER trigger
        RETURN NULL; 
    END;
$$ LANGUAGE plpgsql;

-- create_partition_and_insert function creates a partition table if it doesn't exist and inserts the row.
-- This is easier to deal with than installing partman or using cronjobs to create partitions, though, less
-- performant.
-- Partitioning makes is easy to remove old data from the DB (either DROP or some kind of archiving).
CREATE OR REPLACE FUNCTION create_partition_and_insert() RETURNS trigger AS $$
    DECLARE
      partition_date TEXT;
      partition TEXT;
    BEGIN
      partition_date = to_char(to_timestamp(NEW.created_at)::date, 'YYYY_MM_DD');
      partition = TG_TABLE_NAME || '_' || partition_date;
      IF NOT EXISTS(SELECT relname FROM pg_class WHERE relname=partition) THEN
        EXECUTE 'CREATE TABLE ' || partition || ' (check (to_timestamp(created_at)::date = ''' || to_timestamp(NEW.created_at)::date || ''')) INHERITS (' || TG_TABLE_NAME || ');';
        IF (TG_TABLE_NAME = LOWER('Layer')) THEN
            EXECUTE 'CREATE TRIGGER ' || partition || '_notify_event AFTER INSERT OR UPDATE OR DELETE ON ' || partition || ' FOR EACH ROW EXECUTE PROCEDURE notify_event();';
        ELSIF (TG_TABLE_NAME = LOWER('Task')) THEN
            EXECUTE 'CREATE TRIGGER ' || partition || '_notify_event AFTER INSERT OR UPDATE OR DELETE ON ' || partition || ' FOR EACH ROW EXECUTE PROCEDURE notify_event();';
        END IF;
      END IF;
      EXECUTE 'INSERT INTO ' || partition || ' SELECT(' || TG_TABLE_NAME || ' ' || quote_literal(NEW) || ').*;';
      RETURN NULL;
    END;
$$ LANGUAGE plpgsql VOLATILE COST 101;

CREATE TABLE IF NOT EXISTS Job (
	    name VARCHAR(255),
	    id VARCHAR(36) PRIMARY KEY,
	    status VARCHAR(12) NOT NULL DEFAULT 'PENDING',
	    etag VARCHAR(36) NOT NULL,
	    created_at BIGINT NOT NULL DEFAULT 0,
	    updated_at BIGINT NOT NULL DEFAULT 0,
            UNIQUE (id, created_at)
) WITH (OIDS=FALSE);
CREATE OR REPLACE TRIGGER Job_partition_trigger BEFORE INSERT ON Job FOR EACH ROW EXECUTE PROCEDURE create_partition_and_insert();

-- Layer table has no FK to Job as we can only ever create Layers in CreateJob API call, in which we create
-- the Job and it's Layers in a transaction (and optionally Tasks too)
CREATE TABLE IF NOT EXISTS Layer (
	    name VARCHAR(255),
	    paused_at BIGINT NOT NULL DEFAULT 0,
	    priority INT NOT NULL DEFAULT 0,
	    id VARCHAR(36) PRIMARY KEY,
	    status VARCHAR(12) NOT NULL DEFAULT 'PENDING',
	    etag VARCHAR(36) NOT NULL,
	    job_id VARCHAR(36) NOT NULL,
	    created_at BIGINT NOT NULL DEFAULT 0,
	    updated_at BIGINT NOT NULL DEFAULT 0,
            UNIQUE (id, created_at)
) WITH (OIDS=FALSE);
CREATE OR REPLACE TRIGGER Layer_notify_event AFTER INSERT OR UPDATE OR DELETE ON Layer FOR EACH ROW EXECUTE PROCEDURE notify_event();
CREATE OR REPLACE TRIGGER Layer_partition_trigger BEFORE INSERT ON Layer FOR EACH ROW EXECUTE PROCEDURE create_partition_and_insert();

CREATE TABLE IF NOT EXISTS Task (
	    type VARCHAR(255) NOT NULL, 
            args BYTEA,
	    name VARCHAR(255),
	    paused_at BIGINT NOT NULL DEFAULT 0,
            retries INT NOT NULL DEFAULT 0,
	    id VARCHAR(36) PRIMARY KEY,
	    status VARCHAR(12) NOT NULL DEFAULT 'PENDING',
	    etag VARCHAR(36) NOT NULL,
	    job_id VARCHAR(36) NOT NULL,
	    layer_id VARCHAR(36) NOT NULL,
	    queue_task_id VARCHAR(255) NOT NULL,
	    message TEXT,
	    created_at BIGINT NOT NULL DEFAULT 0,
	    updated_at BIGINT NOT NULL DEFAULT 0,
            CONSTRAINT fk_layer_id FOREIGN KEY (layer_id) REFERENCES Layer(id) ON DELETE CASCADE,
            UNIQUE (id, created_at)
) WITH (OIDS=FALSE);
CREATE OR REPLACE TRIGGER Task_notify_event AFTER INSERT OR UPDATE OR DELETE ON Task FOR EACH ROW EXECUTE PROCEDURE notify_event();
CREATE OR REPLACE TRIGGER Task_partition_trigger BEFORE INSERT ON Task FOR EACH ROW EXECUTE PROCEDURE create_partition_and_insert();
