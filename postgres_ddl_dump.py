# Databricks notebook source
# MAGIC %md
# MAGIC # Postgres Server DDL Snapshot (schema-only ➜ `.sql` file)
# MAGIC
# MAGIC This notebook dumps the **DDL (structure) of an entire Postgres server** — every database the
# MAGIC connecting role can reach — **without any table data**, and writes it to a single `.sql` file in a
# MAGIC **user-specified directory** (e.g. a Databricks **Volume**). It is **best-effort by design**: anything
# MAGIC the role has **no permission** to read is **skipped and logged**, never fatal.
# MAGIC
# MAGIC **What it captures**
# MAGIC - Cluster **globals** — roles & tablespaces (`pg_dumpall --globals-only`, no passwords).
# MAGIC - For each accessible database: `CREATE SCHEMA / TABLE / VIEW / SEQUENCE / FUNCTION / TYPE / INDEX /
# MAGIC   CONSTRAINT ...` — i.e. the full structure, **schema-only** (`pg_dump --schema-only`, no rows).
# MAGIC
# MAGIC **What it skips (and reports)**
# MAGIC - Databases the role can't `CONNECT` to.
# MAGIC - Schemas the role lacks `USAGE` on; tables it lacks `SELECT` on.
# MAGIC - Any object that still errors — the database falls back to a per-schema dump so one bad schema
# MAGIC   can't sink the rest.
# MAGIC
# MAGIC **Notes**
# MAGIC - Uses the **same `PG_*` connection config** as the other notebooks. **No widgets** — plain variables only.
# MAGIC - Output is one `.sql` file with `\connect` markers per database, so `psql -f file.sql` replays the DDL
# MAGIC   into each database (the databases must already exist on the target, or enable `ADD_CREATE_DATABASE`).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Install the Postgres client tools (`pg_dump` / `pg_dumpall`)
# MAGIC Same as the backup notebook: `postgresql-client` is an OS package, not a Python lib. `pg_dump`'s major
# MAGIC version must be **>= the server's**; installs from PGDG pinned to `PG_CLIENT_MAJOR`.

# COMMAND ----------

# Major version of the postgresql-client to install. MUST be >= the server's major
# version. The reference docker stack runs postgres:16, so 16 is a safe default.
PG_CLIENT_MAJOR = "16"

import shutil
import subprocess

def _sh(cmd):
    """Run a shell command, streaming output; raise on failure."""
    print(f"$ {cmd}")
    res = subprocess.run(cmd, shell=True, text=True,
                         stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    print(res.stdout)
    if res.returncode != 0:
        raise RuntimeError(f"command failed ({res.returncode}): {cmd}")

existing = shutil.which("pg_dump")
if existing:
    print(f"pg_dump already present at {existing}")
    _sh("pg_dump --version")
else:
    # Add the PGDG repo so we can install the exact client major version, then install it.
    try:
        _sh("apt-get update -qq")
        _sh("apt-get install -y -qq curl ca-certificates gnupg lsb-release")
        _sh("install -d /usr/share/postgresql-common/pgdg")
        _sh("curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc "
            "-o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc")
        _sh('echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] '
            'http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" '
            '> /etc/apt/sources.list.d/pgdg.list')
        _sh("apt-get update -qq")
        _sh(f"apt-get install -y -qq postgresql-client-{PG_CLIENT_MAJOR}")
    except RuntimeError:
        print("PGDG install failed; falling back to the distro's default postgresql-client")
        _sh("apt-get install -y -qq postgresql-client")
    _sh("pg_dump --version")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration — connection, scope & output (NO WIDGETS)

# COMMAND ----------

import os

# ----------------------------------------------------------------------------
# Postgres connection details (same variables as the other notebooks). PG_DATABASE
# is the "bootstrap" database we connect to first to enumerate the server's databases;
# it just needs to be one the role can CONNECT to (e.g. "postgres" or "target_db").
# ----------------------------------------------------------------------------
PG_HOST = os.getenv("PG_HOST", "postgres-host.example.com")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DATABASE = os.getenv("PG_DATABASE", "postgres")
PG_USER = os.getenv("PG_USER", "postgres_user")
PG_PASSWORD = os.getenv("PG_PASSWORD", "postgres_password")          # e.g. dbutils.secrets.get("scope", "pg_pw")
PG_SSLMODE = os.getenv("PG_SSLMODE", "prefer")                        # disable | allow | prefer | require | verify-ca | verify-full

# ----------------------------------------------------------------------------
# Output location  ***  USER-SPECIFIED DIRECTORY  ***  (a Databricks Volume persists it)
# ----------------------------------------------------------------------------
OUTPUT_DIR = os.getenv("DDL_OUTPUT_DIR", "/Volumes/main/default/pg_ddl")
OUTPUT_FILENAME = os.getenv("DDL_OUTPUT_FILENAME") or None            # None -> "<host>_ddl_<UTC-timestamp>.sql"

# ----------------------------------------------------------------------------
# Scope
# ----------------------------------------------------------------------------
# [] = every database on the server the role can CONNECT to (skips templates and
# no-connect DBs). Or list specific databases, e.g. ["target_db", "reporting"].
DATABASES = []

INCLUDE_GLOBALS = True         # dump cluster roles + tablespaces (pg_dumpall --globals-only --no-role-passwords)
EXCLUDE_SCHEMAS = []           # extra schemas to skip beyond the system ones, e.g. ["pg_temp*", "cron"]

# ----------------------------------------------------------------------------
# DDL flavour
# ----------------------------------------------------------------------------
NO_OWNER = True                # drop ALTER ... OWNER TO (restore adopts the connecting role) — portable
NO_ACL = False                 # False = keep GRANT/REVOKE (they ARE part of the DDL); True = strip them (--no-privileges)
ADD_CONNECT_LINES = True       # inject \connect "<db>" so `psql -f` routes each section to its database
ADD_CREATE_DATABASE = False    # also emit CREATE DATABASE per db (restore onto a fresh server where the DBs don't exist yet)

# Any extra raw pg_dump flags to pass through, e.g. ["--no-comments"].
EXTRA_PG_DUMP_ARGS = []

# ----------------------------------------------------------------------------
# Example of sourcing the password from a secret instead of hard-coding (recommended):
# ----------------------------------------------------------------------------
# PG_PASSWORD = dbutils.secrets.get(scope="migration", key="pg_password")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Resolve & verify the output directory, build the file path

# COMMAND ----------

import datetime

os.makedirs(OUTPUT_DIR, exist_ok=True)
_probe = os.path.join(OUTPUT_DIR, ".write_test")
try:
    with open(_probe, "w") as fh:
        fh.write("ok")
    os.remove(_probe)
except OSError as exc:
    raise PermissionError(
        f"OUTPUT_DIR is not writable: {OUTPUT_DIR!r} ({exc}). On Databricks, point it at a "
        f"Volume you can write to, e.g. /Volumes/<catalog>/<schema>/<volume>/pg_ddl."
    )

if OUTPUT_FILENAME:
    _name = OUTPUT_FILENAME if OUTPUT_FILENAME.endswith(".sql") else OUTPUT_FILENAME + ".sql"
else:
    _ts = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    _safe_host = PG_HOST.replace(":", "_").replace("/", "_")
    _name = f"{_safe_host}_ddl_{_ts}.sql"

OUTPUT_PATH = os.path.join(OUTPUT_DIR, _name)
print(f"Output directory : {OUTPUT_DIR}")
print(f"DDL file         : {OUTPUT_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Pre-flight: version check + enumerate the databases we're allowed into

# COMMAND ----------

import re
import shutil
import subprocess
import psycopg2

if shutil.which("pg_dump") is None:
    raise RuntimeError("pg_dump not found on PATH — run the install cell (§0) first.")


def pg_connect(dbname):
    return psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=dbname,
                            user=PG_USER, password=PG_PASSWORD, sslmode=PG_SSLMODE)


# --- version gate (Ubuntu build appends a "(Ubuntu ...)" suffix, so don't anchor to end) ---
_boot = pg_connect(PG_DATABASE)
try:
    with _boot.cursor() as cur:
        cur.execute("SHOW server_version_num;")
        server_num = int(cur.fetchone()[0])
        cur.execute("SHOW server_version;")
        server_version = cur.fetchone()[0]
        cur.execute("SELECT current_user;")
        connected_role = cur.fetchone()[0]

        # Databases we may enter: connectable, non-template, and CONNECT-privileged.
        cur.execute(
            """
            SELECT d.datname
            FROM pg_database d
            WHERE d.datallowconn
              AND NOT d.datistemplate
              AND has_database_privilege(current_user, d.datname, 'CONNECT')
            ORDER BY d.datname;
            """
        )
        connectable = [r[0] for r in cur.fetchall()]
finally:
    _boot.close()
server_major = server_num // 10000

_out = subprocess.run(["pg_dump", "--version"], text=True, capture_output=True).stdout
_m = re.search(r"PostgreSQL\)\s+(\d+)", _out) or re.search(r"\b(\d+)\.\d+", _out)
client_major = int(_m.group(1)) if _m else -1
print(f"Server        : {server_version} (major {server_major})")
print(f"pg_dump       : {_out.strip()} (major {client_major})")
print(f"Connected as  : {connected_role}")
if client_major < server_major:
    raise RuntimeError(
        f"pg_dump major {client_major} is OLDER than the server major {server_major}. "
        f"Set PG_CLIENT_MAJOR='{server_major}' in §0 and re-run the install cell."
    )

# Resolve the target database list.
if DATABASES:
    TARGET_DBS = [d for d in DATABASES if d in connectable]
    denied = [d for d in DATABASES if d not in connectable]
    if denied:
        print(f"\nSKIP (not connectable / no CONNECT privilege): {denied}")
else:
    TARGET_DBS = connectable

if not TARGET_DBS:
    raise RuntimeError("No databases are reachable for this role — nothing to dump.")
print(f"\nDatabases to snapshot ({len(TARGET_DBS)}): {TARGET_DBS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Dump the DDL (schema-only, best-effort, skip what we can't read)
# MAGIC For each database we enumerate the schemas the role can `USAGE` and the tables it lacks `SELECT` on,
# MAGIC scope `pg_dump --schema-only` accordingly, and — if the whole-database dump still errors — retry
# MAGIC **one schema at a time** so a single unreadable schema can't lose the rest. Everything skipped is logged.

# COMMAND ----------

_SYS_SCHEMAS = ("pg_catalog", "information_schema")


def _base_pg_dump_args():
    args = ["--schema-only", "--no-password", "--verbose"]
    if NO_OWNER:
        args.append("--no-owner")
    if NO_ACL:
        args.append("--no-privileges")
    args += list(EXTRA_PG_DUMP_ARGS)
    return args


def run_pg_dump(dbname, schemas, exclude_tables):
    """Run pg_dump --schema-only for `dbname`, scoped to `schemas`, excluding `exclude_tables`.
    Returns (ok, sql_text, stderr_text)."""
    cmd = ["pg_dump", "--host", PG_HOST, "--port", str(PG_PORT),
           "--username", PG_USER, "--dbname", dbname] + _base_pg_dump_args()
    if ADD_CREATE_DATABASE:
        cmd.append("--create")
    for s in schemas:
        cmd += ["--schema", s]
    for t in exclude_tables:
        cmd += ["--exclude-table", t]
    env = dict(os.environ); env["PGPASSWORD"] = PG_PASSWORD; env["PGSSLMODE"] = PG_SSLMODE
    proc = subprocess.run(cmd, env=env, text=True,
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return proc.returncode == 0, proc.stdout, proc.stderr


def accessible_objects(dbname):
    """Return (schemas_with_usage, tables_without_select) for `dbname`."""
    conn = pg_connect(dbname)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT n.nspname
                FROM pg_namespace n
                WHERE has_schema_privilege(current_user, n.oid, 'USAGE')
                  AND n.nspname NOT IN %s
                  AND n.nspname NOT LIKE 'pg\\_toast%%'
                  AND n.nspname NOT LIKE 'pg\\_temp%%'
                ORDER BY n.nspname;
                """,
                (_SYS_SCHEMAS,),
            )
            schemas = [r[0] for r in cur.fetchall()]
            # Tables/matviews/foreign tables in those schemas the role can't SELECT -> exclude them.
            cur.execute(
                """
                SELECT n.nspname, c.relname
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relkind IN ('r', 'p', 'm', 'f')
                  AND has_schema_privilege(current_user, n.oid, 'USAGE')
                  AND n.nspname NOT IN %s
                  AND n.nspname NOT LIKE 'pg\\_toast%%'
                  AND n.nspname NOT LIKE 'pg\\_temp%%'
                  AND NOT has_table_privilege(current_user, c.oid, 'SELECT');
                """,
                (_SYS_SCHEMAS,),
            )
            no_select = [(r[0], r[1]) for r in cur.fetchall()]
    finally:
        conn.close()
    # apply user EXCLUDE_SCHEMAS (fnmatch-style)
    import fnmatch
    schemas = [s for s in schemas if not any(fnmatch.fnmatchcase(s, p) for p in EXCLUDE_SCHEMAS)]
    return schemas, no_select


sections = []            # (header, sql) pieces written to the final file, in order
skipped = []             # human-readable notes about anything we skipped

# --- globals (roles + tablespaces) ---
if INCLUDE_GLOBALS:
    gcmd = ["pg_dumpall", "--host", PG_HOST, "--port", str(PG_PORT), "--username", PG_USER,
            "--globals-only", "--no-role-passwords", "--no-password", "-l", PG_DATABASE]
    if NO_OWNER:
        gcmd.append("--no-owner")
    genv = dict(os.environ); genv["PGPASSWORD"] = PG_PASSWORD; genv["PGSSLMODE"] = PG_SSLMODE
    gproc = subprocess.run(gcmd, env=genv, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if gproc.returncode == 0:
        sections.append(("-- GLOBALS: roles & tablespaces (no passwords)", gproc.stdout))
        print("globals: OK")
    else:
        skipped.append(f"globals (roles/tablespaces): {gproc.stderr.strip().splitlines()[-1] if gproc.stderr.strip() else 'failed'}")
        print(f"globals: SKIPPED — {skipped[-1]}")

# --- per-database schema-only DDL ---
for db in TARGET_DBS:
    schemas, no_select = accessible_objects(db)
    excl = [f"{s}.{t}" for (s, t) in no_select]
    if no_select:
        skipped.append(f"{db}: {len(no_select)} table(s) excluded (no SELECT): "
                       + ", ".join(excl[:8]) + (" ..." if len(excl) > 8 else ""))
    if not schemas:
        skipped.append(f"{db}: no schemas with USAGE for this role — database skipped")
        print(f"{db}: SKIPPED (no accessible schemas)")
        continue

    ok, sql, err = run_pg_dump(db, schemas, excl)
    if ok:
        sections.append((f"-- DATABASE: {db}  ({len(schemas)} schema(s))", sql))
        print(f"{db}: OK  ({len(schemas)} schemas, {len(no_select)} tables excluded)")
    else:
        # Fall back to per-schema so one unreadable schema doesn't lose the whole DB.
        print(f"{db}: whole-db dump failed, retrying per-schema...")
        good_parts = []
        for s in schemas:
            s_excl = [f"{ss}.{t}" for (ss, t) in no_select if ss == s]
            sok, ssql, serr = run_pg_dump(db, [s], s_excl)
            if sok:
                good_parts.append(ssql)
                print(f"   {db}.{s}: OK")
            else:
                last = serr.strip().splitlines()[-1] if serr.strip() else "failed"
                skipped.append(f"{db}.{s}: {last}")
                print(f"   {db}.{s}: SKIPPED — {last}")
        if good_parts:
            sections.append((f"-- DATABASE: {db}  (per-schema, {len(good_parts)}/{len(schemas)} schemas)",
                             "\n".join(good_parts)))

if not sections:
    raise RuntimeError("Nothing could be dumped (no accessible objects). See the skip log above.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Assemble & write the `.sql` file

# COMMAND ----------

_header = [
    "--",
    "-- Postgres server DDL snapshot (schema-only, no data)",
    f"-- Source server : {PG_HOST}:{PG_PORT}",
    f"-- Server version: {server_version}",
    f"-- Connected role: {connected_role}",
    f"-- Generated (UTC): {datetime.datetime.utcnow().isoformat(timespec='seconds')}Z",
    f"-- Databases     : {', '.join(TARGET_DBS)}",
    "--",
    "",
]

with open(OUTPUT_PATH, "w") as fh:
    fh.write("\n".join(_header))
    for header, sql in sections:
        fh.write(f"\n\n{header}\n")
        # Route this section to its database when replaying with psql -f. Skip when
        # ADD_CREATE_DATABASE is on: pg_dump --create emits its own CREATE DATABASE +
        # \connect, and injecting our \connect first would target a not-yet-created DB.
        if ADD_CONNECT_LINES and not ADD_CREATE_DATABASE and header.startswith("-- DATABASE: "):
            dbname = header.split("-- DATABASE: ", 1)[1].split("  ")[0].strip()
            fh.write(f'\\connect "{dbname}"\n')
        fh.write(sql)
        if not sql.endswith("\n"):
            fh.write("\n")
    if skipped:
        fh.write("\n\n-- ============================================================\n")
        fh.write("-- SKIPPED (no permission / errored) — NOT present in this file:\n")
        for note in skipped:
            fh.write(f"--   {note}\n")

print(f"Wrote {OUTPUT_PATH}")
if skipped:
    print(f"\n{len(skipped)} item(s) were skipped (also recorded at the end of the file):")
    for note in skipped:
        print(f"   - {note}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Verify the DDL file (structure present, no data)

# COMMAND ----------

size = os.path.getsize(OUTPUT_PATH)
with open(OUTPUT_PATH, "r", errors="replace") as fh:
    text = fh.read()

n_create = len(re.findall(r"(?m)^\s*CREATE\s+", text))
# schema-only must not carry row data: no COPY ... FROM stdin and no INSERT INTO.
data_lines = [ln for ln in text.splitlines()
              if re.match(r"^\s*(COPY\s+.+\bFROM stdin|INSERT INTO)\b", ln)]

print(f"File   : {OUTPUT_PATH}")
print(f"Size   : {size:,} bytes ({size / 1024:.1f} KiB)")
print(f"CREATE statements: {n_create}")
if size == 0 or n_create == 0:
    raise RuntimeError("DDL file has no CREATE statements — something went wrong.")
if data_lines:
    raise RuntimeError(f"Unexpected DATA found ({len(data_lines)} line(s)) — this should be schema-only!")
print("OK: file contains DDL and NO table data.")

print("\n--- restore (into a server where the databases already exist) ---")
print(f"export PGPASSWORD='<password>'")
print(f"psql -h <host> -p <port> -U {PG_USER} -d {PG_DATABASE} -f '{OUTPUT_PATH}'")
print("# (the \\connect lines route each section to its database; enable ADD_CREATE_DATABASE")
print("#  if the target server does not have those databases yet.)")
