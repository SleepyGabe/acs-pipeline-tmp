-- ---------------------------------------------------------------------------
-- Target Postgres database bootstrap.
--
-- The database itself ("target_db") is created from POSTGRES_DB. The notebook
-- creates and loads every table into PG_SCHEMA (default "public"), dropping
-- first when DROP_TARGET_BEFORE_LOAD=True, so the target starts empty.
--
-- This file just guarantees the target schema exists and is writable. Change
-- the schema name here if you set the notebook's PG_SCHEMA to something else.
-- ---------------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS public AUTHORIZATION postgres_user;

-- pgcrypto provides gen_random_uuid(), which the converter maps SYS_GUID() to.
-- (Postgres 13+ also exposes gen_random_uuid() in core, but enabling pgcrypto
--  keeps older targets working.)
CREATE EXTENSION IF NOT EXISTS pgcrypto;
