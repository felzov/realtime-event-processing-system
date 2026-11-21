-- Runs once, when the postgres container starts with an empty data volume (the image's
-- /docker-entrypoint-initdb.d convention). An existing volume keeps whatever databases it has.
-- Integration tests use only this database; test code refuses a name not ending in `_test`.
CREATE DATABASE event_pipeline_test OWNER event_pipeline;
