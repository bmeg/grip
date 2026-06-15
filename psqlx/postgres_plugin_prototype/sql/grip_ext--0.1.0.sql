-- Prototype extension SQL contract for delegated GripQL execution

CREATE SCHEMA IF NOT EXISTS grip_ext;

-- Expected runtime symbol is provided by the Rust extension shared object.
CREATE OR REPLACE FUNCTION grip_ext.grip_exec(graph text, query jsonb)
RETURNS SETOF jsonb
AS 'MODULE_PATHNAME', 'grip_exec'
LANGUAGE C
STABLE
PARALLEL SAFE;

CREATE OR REPLACE FUNCTION grip_ext.grip_ping()
RETURNS text
AS 'MODULE_PATHNAME', 'grip_ping'
LANGUAGE C
STABLE
PARALLEL SAFE;
