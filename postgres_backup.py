# Databricks notebook source
# MAGIC %md
# MAGIC # Postgres Database Backup (`pg_dump` ➜ `.sql` file)
# MAGIC
# MAGIC This notebook takes a **logical backup** of the Postgres database configured below and writes it
# MAGIC to a single `.sql` file in a **user-specified output directory** (e.g. a Databricks **Volume**).
# MAGIC The resulting file is a plain-SQL dump produced by `pg_dump`, so it can be replayed later with
# MAGIC `psql -f <file>.sql` to **restore** the database on any Postgres server of the same (or newer) major version.
# MAGIC
# MAGIC **Flow**
# MAGIC ```
# MAGIC  ┌────────────┐   pg_dump (-Fp)   ┌──────────────────────────────┐   psql -f (later)   ┌────────────┐
# MAGIC  │ Postgres DB│ ────────────────► │ <OUTPUT_DIR>/<db>_backup.sql │ ──────────────────► │ Postgres DB│
# MAGIC  └────────────┘                   └──────────────────────────────┘                     └────────────┘
# MAGIC ```
# MAGIC
# MAGIC **Notes**
# MAGIC - Uses the **same Postgres connection config** (the `PG_*` variables) as `oracle_to_postgres_migration`.
# MAGIC - **No widgets.** Every input is a plain Python variable in the *Configuration* cell, overridable by an
# MAGIC   environment variable of the same name (that's how the Docker / Jupyter stack points it at the containers).
# MAGIC - The output directory is **user-specified** via `BACKUP_OUTPUT_DIR` — point it at a Databricks Volume
# MAGIC   (e.g. `/Volumes/<catalog>/<schema>/<volume>/...`) so the dump persists beyond the cluster.
# MAGIC - Prefer pulling the password from `dbutils.secrets` rather than hard-coding it (example shown, commented out).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Install the Postgres client tools (`pg_dump`)
# MAGIC `pg_dump` / `psql` come from the `postgresql-client` OS package — they are **not** Python libraries,
# MAGIC so `%pip` can't provide them. Databricks cluster nodes are Ubuntu and run as `root`, so `apt-get` works.
# MAGIC
# MAGIC **Important:** `pg_dump`'s major version must be **>= the server's major version**. The cell installs
# MAGIC from the official PostgreSQL Apt repository (PGDG) pinned to `PG_CLIENT_MAJOR`; set it to your server's
# MAGIC major version. If the cluster already has a recent-enough `pg_dump`, you can skip this cell.

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
        # Fall back to whatever the distro ships; the version check later will flag it if too old.
        print("PGDG install failed; falling back to the distro's default postgresql-client")
        _sh("apt-get install -y -qq postgresql-client")
    _sh("pg_dump --version")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration — connection details & backup options (NO WIDGETS)

# COMMAND ----------

import os

# ----------------------------------------------------------------------------
# Postgres (SOURCE of the backup) connection details
#   Same variables as the migration notebook. Each value falls back to the
#   literal below but can be overridden by an environment variable of the same
#   name (that's what lets the Docker / Jupyter stack point this at the container).
# ----------------------------------------------------------------------------
PG_HOST = os.getenv("PG_HOST", "postgres-host.example.com")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DATABASE = os.getenv("PG_DATABASE", "target_db")
PG_USER = os.getenv("PG_USER", "postgres_user")
PG_PASSWORD = os.getenv("PG_PASSWORD", "postgres_password")          # e.g. dbutils.secrets.get("scope", "pg_pw")
PG_SCHEMA = os.getenv("PG_SCHEMA", "public")                          # default schema (used by the INCLUDE_SCHEMAS example below)
PG_SSLMODE = os.getenv("PG_SSLMODE", "prefer")                        # disable | allow | prefer | require | verify-ca | verify-full

# ----------------------------------------------------------------------------
# Output location  ***  THIS IS THE USER-SPECIFIED DIRECTORY  ***
#   Point BACKUP_OUTPUT_DIR at wherever the .sql dump should land. On Databricks
#   use a Volume so the file survives the cluster, e.g.:
#       /Volumes/<catalog>/<schema>/<volume>/pg_backups
#   The directory is created if missing and verified writable in the next cell.
# ----------------------------------------------------------------------------
BACKUP_OUTPUT_DIR = os.getenv("BACKUP_OUTPUT_DIR", "/Volumes/main/default/pg_backups")

# Leave as None to auto-name "<db>_<full|schema>_backup_<UTC-timestamp>.sql".
# Set an explicit name to control it (extension is added automatically per BACKUP_FORMAT).
BACKUP_FILENAME = os.getenv("BACKUP_FILENAME") or None

# ----------------------------------------------------------------------------
# Backup behaviour
# ----------------------------------------------------------------------------
# Output format:
#   "plain"  -> a single .sql text file, restored with `psql -f file.sql`        (default; what was requested)
#   "custom" -> a compressed .dump, restored with `pg_restore` (selective/parallel restore)
BACKUP_FORMAT = "plain"

SCHEMA_ONLY = False           # True = DDL only, no row data (pg_dump --schema-only)
DATA_ONLY = False             # True = row data only, no DDL    (pg_dump --data-only)

# ----------------------------------------------------------------------------
# SCOPE — back up only PART of the database. This is how you keep a multi-TB DB's
# dump small: you only ever dump what you list here. Leave ALL FOUR empty to back
# up the whole database. Patterns use pg_dump's syntax ('*' and '?' wildcards);
# an entry with no '.' matches a table in any schema. These map 1:1 to pg_dump
# object-selection flags, and the §3 size estimate reflects exactly this selection.
# ----------------------------------------------------------------------------
INCLUDE_SCHEMAS    = []        # whole schemas to dump, e.g. ["reporting", "public"]      (pg_dump --schema)
INCLUDE_TABLES     = []        # specific tables to dump, e.g. ["public.customers", "public.orders"]  (pg_dump --table)
EXCLUDE_TABLES     = []        # tables to drop ENTIRELY (DDL + data), e.g. ["public.*_tmp"]  (pg_dump --exclude-table)
EXCLUDE_TABLE_DATA = []        # tables to keep as EMPTY (DDL only, ROWS dropped), e.g. ["public.audit_*", "*_log"]  (pg_dump --exclude-table-data)

# ----------------------------------------------------------------------------
# DISK GUARD — refuse to start (nothing is written) unless the selection fits.
#   The §3 preflight estimates the on-disk size of the selected data and aborts
#   BEFORE pg_dump runs if it would blow the budget or the destination's free
#   space. This is what prevents a runaway dump from filling your volume.
# ----------------------------------------------------------------------------
MAX_DUMP_BYTES    = 50 * 1024**3   # hard ceiling for the estimated dump. Set to None to disable the budget check.
FREE_SPACE_SAFETY = 1.5            # require estimate * this much free space at BACKUP_OUTPUT_DIR before starting.

NO_OWNER = True               # strip ownership (restore as the connecting role) — portable across servers
NO_PRIVILEGES = True          # strip GRANT/REVOKE (ACLs) — portable across servers
INCLUDE_DROP = True           # emit DROP ... IF EXISTS before each CREATE (--clean --if-exists) — clean re-restore
INCLUDE_CREATE_DATABASE = False  # emit CREATE DATABASE + \connect (--create); restore into a maintenance db (e.g. postgres)

# Any extra raw pg_dump flags you want to pass through, e.g. ["--no-comments"].
EXTRA_PG_DUMP_ARGS = []

# ----------------------------------------------------------------------------
# Example of sourcing the password from a secret instead of hard-coding (recommended):
# ----------------------------------------------------------------------------
# PG_PASSWORD = dbutils.secrets.get(scope="migration", key="pg_password")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Resolve & verify the output directory, build the dump file path

# COMMAND ----------

import datetime

# The user explicitly chose BACKUP_OUTPUT_DIR, so honour it exactly: create it if
# missing and prove it's writable. Fail LOUDLY rather than silently dumping elsewhere
# (a backup written to the wrong place is worse than a backup that didn't run).
os.makedirs(BACKUP_OUTPUT_DIR, exist_ok=True)
_probe = os.path.join(BACKUP_OUTPUT_DIR, ".write_test")
try:
    with open(_probe, "w") as fh:
        fh.write("ok")
    os.remove(_probe)
except OSError as exc:
    raise PermissionError(
        f"BACKUP_OUTPUT_DIR is not writable: {BACKUP_OUTPUT_DIR!r} ({exc}). "
        f"On Databricks, point it at a Volume you can write to, e.g. "
        f"/Volumes/<catalog>/<schema>/<volume>/pg_backups."
    )

_EXT = {"plain": ".sql", "custom": ".dump"}[BACKUP_FORMAT]
if BACKUP_FILENAME:
    _name = BACKUP_FILENAME if BACKUP_FILENAME.endswith(_EXT) else BACKUP_FILENAME + _EXT
else:
    _ts = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    _kind = "schema" if SCHEMA_ONLY else ("data" if DATA_ONLY else "full")
    _name = f"{PG_DATABASE}_{_kind}_backup_{_ts}{_EXT}"

BACKUP_PATH = os.path.join(BACKUP_OUTPUT_DIR, _name)
print(f"Output directory : {BACKUP_OUTPUT_DIR}")
print(f"Backup file      : {BACKUP_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Pre-flight: version check, **scope resolution, size estimate & disk guard**
# MAGIC This cell is the safety gate. It:
# MAGIC 1. verifies `pg_dump` is new enough for the server (it refuses to dump from a *newer* server);
# MAGIC 2. resolves exactly which tables your SCOPE selection will dump, and sums their on-disk size;
# MAGIC 3. **aborts here — before a single byte is written** — if that estimate blows `MAX_DUMP_BYTES`
# MAGIC    or won't fit the destination's free space (so a runaway dump can't fill your volume);
# MAGIC 4. warns about any foreign key from a selected table to a table you're *not* backing up (would break restore into an empty DB).

# COMMAND ----------

import re
import fnmatch
import shutil
import subprocess
import psycopg2

if shutil.which("pg_dump") is None:
    raise RuntimeError("pg_dump not found on PATH — run the install cell (§0) first.")


def _human(n):
    for unit in ("B", "KiB", "MiB", "GiB", "TiB", "PiB"):
        if abs(n) < 1024 or unit == "PiB":
            return f"{n:,.2f} {unit}"
        n /= 1024


def _matches(schema, name, patterns):
    """True if 'schema.name' matches any pg_dump-style pattern in `patterns`.
    A pattern with no '.' matches the table name in any schema."""
    fq = f"{schema}.{name}"
    for p in patterns:
        if "." in p:
            if fnmatch.fnmatchcase(fq, p):
                return True
        elif fnmatch.fnmatchcase(name, p):
            return True
    return False


# ---- one connection for the whole preflight ----
_conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DATABASE,
                         user=PG_USER, password=PG_PASSWORD, sslmode=PG_SSLMODE)
try:
    with _conn.cursor() as cur:
        cur.execute("SHOW server_version_num;")   # e.g. 160004 -> major 16
        server_num = int(cur.fetchone()[0])
        cur.execute("SHOW server_version;")
        server_version = cur.fetchone()[0]

        # Every data-bearing user relation with its on-disk sizes.
        cur.execute(
            """
            SELECT n.nspname, c.relname,
                   pg_total_relation_size(c.oid) AS total_bytes,
                   pg_table_size(c.oid)          AS data_bytes
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind IN ('r', 'p', 'm')            -- table, partitioned table, matview
              AND n.nspname NOT IN ('pg_catalog', 'information_schema')
              AND n.nspname NOT LIKE 'pg\\_toast%'
              AND n.nspname NOT LIKE 'pg\\_temp%';
            """
        )
        all_tables = [(s, t, int(tot), int(dat)) for s, t, tot, dat in cur.fetchall()]

        # Foreign keys: (child_schema, child, parent_schema, parent).
        cur.execute(
            """
            SELECT ns.nspname, cl.relname, fns.nspname, fcl.relname
            FROM pg_constraint con
            JOIN pg_class cl      ON cl.oid  = con.conrelid
            JOIN pg_namespace ns  ON ns.oid  = cl.relnamespace
            JOIN pg_class fcl     ON fcl.oid = con.confrelid
            JOIN pg_namespace fns ON fns.oid = fcl.relnamespace
            WHERE con.contype = 'f';
            """
        )
        fkeys = cur.fetchall()
finally:
    _conn.close()
server_major = server_num // 10000

# --- version gate ---
# `pg_dump --version` prints e.g. "pg_dump (PostgreSQL) 16.13 (Ubuntu 16.13-0ubuntu0.24.04.1)"
# — take the number right after "(PostgreSQL)", falling back to the first "<major>.<minor>".
_out = subprocess.run(["pg_dump", "--version"], text=True, capture_output=True).stdout
_m = re.search(r"PostgreSQL\)\s+(\d+)", _out) or re.search(r"\b(\d+)\.\d+", _out)
client_major = int(_m.group(1)) if _m else -1
print(f"Server : {server_version} (major {server_major})")
print(f"pg_dump: {_out.strip()} (major {client_major})")
if client_major < server_major:
    raise RuntimeError(
        f"pg_dump major {client_major} is OLDER than the server major {server_major}. "
        f"Set PG_CLIENT_MAJOR='{server_major}' in §0 and re-run the install cell."
    )

# --- resolve the SCOPE selection (mirrors what pg_dump will actually dump) ---
_no_include = not (INCLUDE_SCHEMAS or INCLUDE_TABLES)


def _selected(schema, name):
    if _no_include:
        included = True                                   # whole DB
    else:
        included = (any(fnmatch.fnmatchcase(schema, p) for p in INCLUDE_SCHEMAS)
                    or _matches(schema, name, INCLUDE_TABLES))
    if not included or _matches(schema, name, EXCLUDE_TABLES):
        return False
    return True


selected = [(s, t, tot, dat) for (s, t, tot, dat) in all_tables if _selected(s, t)]
selected_names = {(s, t) for (s, t, _, _) in selected}

# --- foreign-key closure check (would the restore break?) ---
dangling = [(cs, ct, ps, pt) for (cs, ct, ps, pt) in fkeys
            if (cs, ct) in selected_names and (ps, pt) not in selected_names]

# --- size estimate: data of selected tables, MINUS tables whose data we exclude ---
_data_excluded = {(s, t) for (s, t, _, _) in selected if _matches(s, t, EXCLUDE_TABLE_DATA)}
if SCHEMA_ONLY:
    est_bytes = 0                                          # DDL only — negligible
else:
    est_bytes = sum(dat for (s, t, tot, dat) in selected if (s, t) not in _data_excluded)

print(f"\nScope: {'WHOLE DATABASE' if _no_include else 'PARTIAL'} — "
      f"{len(selected)} of {len(all_tables)} tables selected"
      + (f", {len(_data_excluded)} kept as empty (data excluded)" if _data_excluded else ""))
for (s, t, tot, dat) in sorted(selected, key=lambda r: -r[3])[:15]:
    tag = "  [data excluded]" if (s, t) in _data_excluded else ""
    print(f"   {s}.{t:<40} data={_human(dat):>12}{tag}")
if len(selected) > 15:
    print(f"   ... and {len(selected) - 15} more")
print(f"\nEstimated dump data size (uncompressed, approx): {_human(est_bytes)}")

# --- disk guard: abort BEFORE writing anything ---
if MAX_DUMP_BYTES is not None and est_bytes > MAX_DUMP_BYTES:
    raise RuntimeError(
        f"ABORT: estimated {_human(est_bytes)} exceeds MAX_DUMP_BYTES={_human(MAX_DUMP_BYTES)}. "
        f"Narrow the SCOPE (INCLUDE_* / EXCLUDE_TABLE_DATA) or raise MAX_DUMP_BYTES. "
        f"Nothing was written."
    )
try:
    free = shutil.disk_usage(BACKUP_OUTPUT_DIR).free
    need = est_bytes * FREE_SPACE_SAFETY
    print(f"Free at destination: {_human(free)} (need ~{_human(need)} incl. {FREE_SPACE_SAFETY}x safety)")
    if est_bytes and free < need:
        raise RuntimeError(
            f"ABORT: only {_human(free)} free at {BACKUP_OUTPUT_DIR}, need ~{_human(need)}. "
            f"Nothing was written."
        )
except OSError:
    print(f"NOTE: could not read free space for {BACKUP_OUTPUT_DIR} "
          f"(common for FUSE-mounted Volumes) — relying on the MAX_DUMP_BYTES budget instead.")

# --- FK warning (only when we did NOT auto-include) ---
if dangling:
    print("\nWARNING: these foreign keys point at tables OUTSIDE the backup scope. The dump is valid,")
    print("but restoring into an EMPTY database will fail these FKs unless the parent tables already")
    print("exist there. To make it self-contained, add the parents (right-hand side) to INCLUDE_TABLES")
    print("— but note pg_dump does not emit CREATE SCHEMA for --table-only dumps, so a parent in a")
    print("non-'public' schema also needs that schema created on the target first:")
    for (cs, ct, ps, pt) in dangling[:20]:
        print(f"   {cs}.{ct}  ->  {ps}.{pt}")
    if len(dangling) > 20:
        print(f"   ... and {len(dangling) - 20} more")

# --- canonical pg_dump object-selection args, consumed verbatim by §4 ---
# Pass the user's include/exclude patterns straight through to pg_dump; §3's estimate
# above mirrors exactly this selection.
SCOPE_ARGS = []
for s in INCLUDE_SCHEMAS:
    SCOPE_ARGS += ["--schema", s]
for t in INCLUDE_TABLES:
    SCOPE_ARGS += ["--table", t]
for t in EXCLUDE_TABLES:
    SCOPE_ARGS += ["--exclude-table", t]
for t in EXCLUDE_TABLE_DATA:
    SCOPE_ARGS += ["--exclude-table-data", t]

print("\nOK: pre-flight passed — proceeding to dump.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Run the backup (`pg_dump`)
# MAGIC The password is passed to `pg_dump` via the `PGPASSWORD` environment variable (never on the command
# MAGIC line, where it would show up in the process list). `pg_dump` writes the file itself via `--file`.

# COMMAND ----------

import subprocess

if SCHEMA_ONLY and DATA_ONLY:
    raise ValueError("SCHEMA_ONLY and DATA_ONLY are mutually exclusive — set at most one to True.")

cmd = [
    "pg_dump",
    "--host", PG_HOST,
    "--port", str(PG_PORT),
    "--username", PG_USER,
    "--dbname", PG_DATABASE,
    "--file", BACKUP_PATH,
    "--format", {"plain": "p", "custom": "c"}[BACKUP_FORMAT],
    "--no-password",   # never prompt interactively; rely on PGPASSWORD below
    "--verbose",       # progress goes to stderr
]
cmd += SCOPE_ARGS                       # scope resolved & size-checked in §3 (INCLUDE_*/EXCLUDE_*)
if SCHEMA_ONLY:
    cmd += ["--schema-only"]
if DATA_ONLY:
    cmd += ["--data-only"]
if NO_OWNER:
    cmd += ["--no-owner"]
if NO_PRIVILEGES:
    cmd += ["--no-privileges"]
if INCLUDE_DROP:
    cmd += ["--clean", "--if-exists"]
if INCLUDE_CREATE_DATABASE:
    cmd += ["--create"]
cmd += list(EXTRA_PG_DUMP_ARGS)

# Sanitised echo of the command (no secrets are in argv — the password is in the env).
print("Running:", " ".join(cmd))

env = dict(os.environ)
env["PGPASSWORD"] = PG_PASSWORD
env["PGSSLMODE"] = PG_SSLMODE

proc = subprocess.run(cmd, env=env, text=True,
                      stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
print(proc.stdout)
if proc.returncode != 0:
    raise RuntimeError(f"pg_dump failed with exit code {proc.returncode} — see output above.")
print(f"\nBackup complete: {BACKUP_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Verify the dump file

# COMMAND ----------

size = os.path.getsize(BACKUP_PATH)
print(f"File : {BACKUP_PATH}")
print(f"Size : {size:,} bytes ({size / (1024 * 1024):.2f} MiB)")
if size == 0:
    raise RuntimeError("Backup file is empty — the dump did not produce any output.")

if BACKUP_FORMAT == "plain":
    # Show the header and a short preview so you can eyeball that it's a real dump.
    with open(BACKUP_PATH, "r", errors="replace") as fh:
        head = [next(fh, "") for _ in range(15)]
    print("\n--- first lines ---")
    print("".join(head).rstrip())
    if not any("PostgreSQL database dump" in ln for ln in head):
        print("\nWARNING: expected 'PostgreSQL database dump' banner not found in the header.")
else:
    print("Custom-format archive written (binary). List its contents with: "
          f"pg_restore --list '{BACKUP_PATH}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. How to restore from this backup (reference)
# MAGIC The cell below only **prints** ready-to-run commands — it does not execute anything.

# COMMAND ----------

target_db = PG_DATABASE  # change to the database you want to restore INTO

print("# Set the password for the restore session (same as the source, or your target's):")
print(f"export PGPASSWORD='<password>'\n")

if BACKUP_FORMAT == "plain":
    if INCLUDE_CREATE_DATABASE:
        print("# This dump includes CREATE DATABASE — restore by connecting to a maintenance DB:")
        print(f"psql -h <host> -p <port> -U {PG_USER} -d postgres -f '{BACKUP_PATH}'")
    else:
        print("# Restore into an existing (empty) target database:")
        print(f"createdb -h <host> -p <port> -U {PG_USER} {target_db}   # if it doesn't exist yet")
        print(f"psql -h <host> -p <port> -U {PG_USER} -d {target_db} -f '{BACKUP_PATH}'")
else:
    print("# Custom-format archive — restore with pg_restore (supports --jobs for parallelism):")
    print(f"pg_restore -h <host> -p <port> -U {PG_USER} -d {target_db} "
          f"--clean --if-exists --no-owner '{BACKUP_PATH}'")
