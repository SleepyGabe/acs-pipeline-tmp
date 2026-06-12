# Databricks notebook source
# MAGIC %md
# MAGIC # Oracle ➜ Postgres Table Migration
# MAGIC
# MAGIC This notebook migrates **400+ tables** from an Oracle database to a Postgres database by
# MAGIC reproducing the following flow for every table in the configured list:
# MAGIC
# MAGIC 1. **Dump** the entire table (DDL + data) from the source Oracle DB into an Oracle-flavoured `.sql` file.
# MAGIC 2. **Convert** that Oracle SQL file into a Postgres-flavoured `.sql` file (syntax conversion).
# MAGIC 3. **Load** the converted Postgres SQL file into the target Postgres DB table.
# MAGIC
# MAGIC **Architecture**
# MAGIC ```
# MAGIC  ┌────────────┐   1. dump    ┌──────────────┐   2. convert   ┌────────────────┐   3. load   ┌────────────┐
# MAGIC  │  Oracle DB │ ───────────► │ oracle/*.sql │ ─────────────► │ postgres/*.sql │ ──────────► │ Postgres DB│
# MAGIC  └────────────┘              └──────────────┘                └────────────────┘             └────────────┘
# MAGIC ```
# MAGIC
# MAGIC **Notes**
# MAGIC - Connection details live in plain variables in the *Configuration* cell (no widgets).
# MAGIC - Replace the placeholder values / `TABLE_NAMES` list with your real values. Prefer pulling
# MAGIC   secrets from `dbutils.secrets` rather than hard-coding passwords (an example is shown, commented out).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Install drivers
# MAGIC `python-oracledb` (thin mode, no Oracle client needed) for the source and `psycopg2` for the target.

# COMMAND ----------

# MAGIC %pip install python-oracledb psycopg2-binary
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration — connection details & table list (NO WIDGETS)

# COMMAND ----------

import os

# ----------------------------------------------------------------------------
# Oracle (SOURCE) connection details
# ----------------------------------------------------------------------------
ORACLE_HOST = "oracle-host.example.com"
ORACLE_PORT = 1521
ORACLE_SERVICE_NAME = "ORCLPDB1"          # use service name ...
ORACLE_SID = None                          # ... OR sid (leave one of them None)
ORACLE_USER = "oracle_user"
ORACLE_PASSWORD = "oracle_password"        # e.g. dbutils.secrets.get("scope", "oracle_pw")
ORACLE_SCHEMA = "ORACLE_USER"              # schema that owns the tables (often == user, uppercase)

# ----------------------------------------------------------------------------
# Postgres (TARGET) connection details
# ----------------------------------------------------------------------------
PG_HOST = "postgres-host.example.com"
PG_PORT = 5432
PG_DATABASE = "target_db"
PG_USER = "postgres_user"
PG_PASSWORD = "postgres_password"          # e.g. dbutils.secrets.get("scope", "pg_pw")
PG_SCHEMA = "public"                        # target schema in Postgres
PG_SSLMODE = "prefer"                       # disable | allow | prefer | require | verify-ca | verify-full

# ----------------------------------------------------------------------------
# Migration behaviour
# ----------------------------------------------------------------------------
WORK_DIR = "/dbfs/tmp/ora2pg"               # where the .sql dump files are written
ORACLE_DUMP_DIR = os.path.join(WORK_DIR, "oracle")
POSTGRES_DUMP_DIR = os.path.join(WORK_DIR, "postgres")

BATCH_SIZE = 5_000                          # rows fetched per batch from Oracle
MAX_PARALLEL_TABLES = 8                      # how many tables to migrate concurrently (thread pool).
                                            # Each worker uses its own Oracle + Postgres connection,
                                            # so keep this <= the connection limits on BOTH databases.
                                            # Set to 1 for fully sequential migration.
# How to handle the target table on the Postgres side:
#   "recreate" — DROP (CASCADE) + CREATE from the Oracle DDL, then load. Runs ALL the structural
#                passes below (constraints/indexes/identity/sequences/FKs). Full migration. (default)
#   "truncate" — table must ALREADY EXIST. TRUNCATE it, then load data only. Skips CREATE and ALL
#                structural passes (your schema already has them). Use for "replace the data".
#   "append"   — table must ALREADY EXIST. Load data only (no drop/truncate/create). Skips ALL
#                structural passes. Use for "add data to what's there" (no dedup — may duplicate).
LOAD_MODE = "recreate"

CONTINUE_ON_ERROR = True                    # keep migrating remaining tables if one fails
KEEP_SQL_FILES = True                       # keep intermediate .sql files for auditing

# Constraint / index migration (second pass). Only applied when LOAD_MODE == "recreate".
MIGRATE_PK_UNIQUE_CHECK = True              # add PRIMARY KEY / UNIQUE / CHECK constraints
MIGRATE_INDEXES = True                      # re-create non-constraint indexes
MIGRATE_FOREIGN_KEYS = True                 # add FOREIGN KEYs (applied last, after all tables load)
MIGRATE_SEQUENCES = True                    # re-create standalone Oracle sequences in Postgres
MIGRATE_IDENTITY_COLUMNS = True             # convert Oracle IDENTITY columns to Postgres IDENTITY

# Load ordering.
RESPECT_LOAD_ORDER = False                  # True  = load tables exactly in TABLE_NAMES order
                                            # False = auto-sort by FK dependency (parents first)

# ----------------------------------------------------------------------------
# Example of sourcing secrets instead of hard-coding (recommended):
# ----------------------------------------------------------------------------
# ORACLE_PASSWORD = dbutils.secrets.get(scope="migration", key="oracle_password")
# PG_PASSWORD     = dbutils.secrets.get(scope="migration", key="pg_password")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table list (400+ tables)
# MAGIC Provide the table names as an array of strings. Two options are shown — pick one.

# COMMAND ----------

# Option A — explicit, hard-coded list of table names.
TABLE_NAMES = [
    "CUSTOMERS",
    "ORDERS",
    "ORDER_ITEMS",
    "PRODUCTS",
    # ... add the rest of your 400+ tables here ...
]

# Option B — discover every table in the Oracle schema automatically.
# Set DISCOVER_TABLES = True to ignore the list above and pull all tables in ORACLE_SCHEMA.
DISCOVER_TABLES = False

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Database connection helpers

# COMMAND ----------

import oracledb
import psycopg2


def get_oracle_connection():
    """Open a connection to the source Oracle database (python-oracledb thin mode)."""
    if ORACLE_SERVICE_NAME:
        dsn = oracledb.makedsn(ORACLE_HOST, ORACLE_PORT, service_name=ORACLE_SERVICE_NAME)
    elif ORACLE_SID:
        dsn = oracledb.makedsn(ORACLE_HOST, ORACLE_PORT, sid=ORACLE_SID)
    else:
        raise ValueError("Either ORACLE_SERVICE_NAME or ORACLE_SID must be set.")
    return oracledb.connect(user=ORACLE_USER, password=ORACLE_PASSWORD, dsn=dsn)


def get_postgres_connection():
    """Open a connection to the target Postgres database."""
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD,
        sslmode=PG_SSLMODE,
    )


# Make sure the working directories exist.
os.makedirs(ORACLE_DUMP_DIR, exist_ok=True)
os.makedirs(POSTGRES_DUMP_DIR, exist_ok=True)
print(f"Oracle dump dir   : {ORACLE_DUMP_DIR}")
print(f"Postgres dump dir : {POSTGRES_DUMP_DIR}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. STEP 1 — Dump an Oracle table to a DDL `.sql` file + a COPY-ready CSV
# MAGIC
# MAGIC For speed, data is written as a **CSV** (Postgres `COPY` format) rather than per-row
# MAGIC `INSERT`s — `COPY` is dramatically faster on large tables. The `CREATE TABLE` DDL is
# MAGIC written to a separate `.sql` file (built from `all_tab_columns` for accurate Oracle types)
# MAGIC and is the only thing the Step 2 converter has to touch. Values are formatted directly into
# MAGIC Postgres-friendly text in Python, so the data needs no SQL-literal conversion at all.

# COMMAND ----------

import csv
import datetime
import decimal


def _oracle_quote_ident(name: str) -> str:
    """Quote an Oracle identifier."""
    return '"' + name.replace('"', '""') + '"'


def _oracle_output_type_handler(cursor, name, default_type, size, precision, scale):
    """
    Fetch LOBs as plain values instead of locators: CLOB/NCLOB -> str, BLOB -> bytes.
    This avoids a per-row round trip per LOB and is much faster for dumping.
    """
    if default_type in (oracledb.DB_TYPE_CLOB, oracledb.DB_TYPE_NCLOB):
        return cursor.var(oracledb.DB_TYPE_LONG, arraysize=cursor.arraysize)
    if default_type == oracledb.DB_TYPE_BLOB:
        return cursor.var(oracledb.DB_TYPE_LONG_RAW, arraysize=cursor.arraysize)
    return None


def _oracle_column_type(data_type, length, precision, scale) -> str:
    """Build an Oracle column type string from all_tab_columns metadata."""
    dt = (data_type or "").upper()
    if dt in ("VARCHAR2", "VARCHAR", "NVARCHAR2", "CHAR", "NCHAR"):
        return f"{dt}({length or 4000})"
    if dt == "NUMBER":
        if precision:
            return f"NUMBER({precision},{scale or 0})"
        return "NUMBER"
    if dt.startswith("TIMESTAMP"):
        return dt  # preserve precision / WITH TIME ZONE, the converter normalises it
    if dt in ("DATE", "CLOB", "NCLOB", "BLOB", "LONG", "FLOAT", "BINARY_FLOAT",
              "BINARY_DOUBLE", "ROWID"):
        return dt
    if dt == "RAW":
        return f"RAW({length or 2000})"
    # Fallback for anything unusual.
    return "VARCHAR2(4000)"


def _build_create_table_ddl(ora_conn, table_name: str) -> str:
    """Build an Oracle CREATE TABLE statement from the data dictionary (accurate types)."""
    cur = ora_conn.cursor()
    cur.execute(
        """
        SELECT column_name, data_type, data_length, data_precision, data_scale, nullable
        FROM all_tab_columns
        WHERE owner = :owner AND table_name = :tname
        ORDER BY column_id
        """,
        owner=ORACLE_SCHEMA.upper(), tname=table_name,
    )
    cols = cur.fetchall()
    cur.close()
    if not cols:
        raise ValueError(f"No columns found for {ORACLE_SCHEMA}.{table_name}")

    col_defs = []
    for col_name, data_type, length, precision, scale, nullable in cols:
        col_type = _oracle_column_type(data_type, length, precision, scale)
        not_null = "" if nullable == "Y" else " NOT NULL"
        col_defs.append(f"    {_oracle_quote_ident(col_name)} {col_type}{not_null}")

    return (
        f"-- Oracle DDL for {ORACLE_SCHEMA}.{table_name}\n"
        f"CREATE TABLE {_oracle_quote_ident(table_name)} (\n"
        + ",\n".join(col_defs)
        + "\n);\n"
    )


def _csv_value(value):
    """Format a Python value as Postgres COPY-CSV text. None -> '' (loaded as NULL)."""
    if value is None:
        return None  # csv.writer emits an empty field; COPY ... NULL '' reads it as NULL
    if isinstance(value, datetime.datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S.%f")
    if isinstance(value, datetime.date):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, (bytes, bytearray)):
        return "\\x" + value.hex()           # Postgres bytea hex input (literal in CSV mode)
    if isinstance(value, bool):
        return "t" if value else "f"
    return value                              # int / float / Decimal / str — csv.writer stringifies


def dump_oracle_table(ora_conn, table_name: str):
    """
    STEP 1: Dump a single Oracle table to (DDL .sql, data .csv).
    Returns (ddl_path, csv_path). The CSV's first row is the column header.
    """
    ddl_path = os.path.join(ORACLE_DUMP_DIR, f"{table_name}.sql")
    csv_path = os.path.join(ORACLE_DUMP_DIR, f"{table_name}.csv")
    qualified = f"{_oracle_quote_ident(ORACLE_SCHEMA)}.{_oracle_quote_ident(table_name)}"

    # DDL from the dictionary (decoupled from the data cursor's LOB handler).
    with open(ddl_path, "w", encoding="utf-8") as f:
        f.write(_build_create_table_ddl(ora_conn, table_name))

    # Data as COPY-ready CSV.
    cursor = ora_conn.cursor()
    cursor.arraysize = BATCH_SIZE
    cursor.outputtypehandler = _oracle_output_type_handler
    cursor.execute(f"SELECT * FROM {qualified}")
    col_names = [c[0] for c in cursor.description]

    row_count = 0
    with open(csv_path, "w", encoding="utf-8", newline="") as cf:
        writer = csv.writer(cf, lineterminator="\n")
        writer.writerow(col_names)                      # header row
        while True:
            rows = cursor.fetchmany(BATCH_SIZE)
            if not rows:
                break
            writer.writerows([_csv_value(v) for v in row] for row in rows)
            row_count += len(rows)
    cursor.close()

    print(f"  [dump]    {table_name}: {row_count} rows -> {csv_path}")
    return ddl_path, csv_path


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. STEP 2 — Convert Oracle `.sql` to Postgres `.sql` (syntax conversion)
# MAGIC
# MAGIC A line/regex based converter covering the most common Oracle ➜ Postgres differences:
# MAGIC data types, function names, sequence/DUAL idioms, identifier quoting and date literals.
# MAGIC Extend `_TYPE_RULES` / `_FUNC_RULES` for any project-specific syntax you hit.
# MAGIC
# MAGIC > **Scope note:** This is a **syntax-only, regex-based** converter — not a SQL parser/transpiler.
# MAGIC > It is designed to convert the **table data dumps generated by Step 1 of this notebook**
# MAGIC > (structured `CREATE TABLE` DDL + `INSERT` statements with predictable literals), which is the
# MAGIC > only input it will ever receive here. It is **not** intended for arbitrary hand-written Oracle
# MAGIC > SQL or PL/SQL (e.g. `CONNECT BY`, `(+)` joins, `DECODE`, `MERGE`, procedures/packages/triggers).
# MAGIC > For those, use a dedicated transpiler such as `ora2pg`.

# COMMAND ----------

import re

# NOTE: Syntax-only converter. Input is always the Step 1 table-data dump
# (CREATE TABLE + INSERTs), never free-form Oracle SQL / PL/SQL. See the scope
# note above before extending these rules for any other use.

# Data-type rewrites (applied to DDL). Order matters — longest/most specific first.
_TYPE_RULES = [
    (re.compile(r"\bVARCHAR2\s*\(\s*(\d+)\s*(?:CHAR|BYTE)?\s*\)", re.IGNORECASE), r"VARCHAR(\1)"),
    (re.compile(r"\bVARCHAR2\b", re.IGNORECASE), "VARCHAR"),
    (re.compile(r"\bNVARCHAR2\s*\(\s*(\d+)\s*\)", re.IGNORECASE), r"VARCHAR(\1)"),
    (re.compile(r"\bNCHAR\b", re.IGNORECASE), "CHAR"),
    (re.compile(r"\bCLOB\b", re.IGNORECASE), "TEXT"),
    (re.compile(r"\bNCLOB\b", re.IGNORECASE), "TEXT"),
    (re.compile(r"\bBLOB\b", re.IGNORECASE), "BYTEA"),
    (re.compile(r"\bLONG\s+RAW\b", re.IGNORECASE), "BYTEA"),
    (re.compile(r"\bRAW\s*\(\s*\d+\s*\)", re.IGNORECASE), "BYTEA"),
    (re.compile(r"\bLONG\b", re.IGNORECASE), "TEXT"),
    # NUMBER(p,0) -> integer-ish, NUMBER(p,s) -> NUMERIC(p,s), bare NUMBER -> NUMERIC
    (re.compile(r"\bNUMBER\s*\(\s*(\d+)\s*,\s*0\s*\)", re.IGNORECASE), r"NUMERIC(\1)"),
    (re.compile(r"\bNUMBER\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)", re.IGNORECASE), r"NUMERIC(\1,\2)"),
    (re.compile(r"\bNUMBER\b", re.IGNORECASE), "NUMERIC"),
    (re.compile(r"\bBINARY_DOUBLE\b", re.IGNORECASE), "DOUBLE PRECISION"),
    (re.compile(r"\bBINARY_FLOAT\b", re.IGNORECASE), "REAL"),
    (re.compile(r"\bFLOAT\b", re.IGNORECASE), "DOUBLE PRECISION"),
    # Oracle DATE carries a time component -> map to TIMESTAMP in Postgres.
    (re.compile(r"\bDATE\b", re.IGNORECASE), "TIMESTAMP"),
]

# Function / expression rewrites (applied to data + DDL).
_FUNC_RULES = [
    (re.compile(r"\bSYSDATE\b", re.IGNORECASE), "CURRENT_TIMESTAMP"),
    (re.compile(r"\bSYSTIMESTAMP\b", re.IGNORECASE), "CURRENT_TIMESTAMP"),
    (re.compile(r"\bNVL\s*\(", re.IGNORECASE), "COALESCE("),
    (re.compile(r"\bSYS_GUID\s*\(\s*\)", re.IGNORECASE), "gen_random_uuid()"),
    # Oracle string concat is the same (||) so nothing to do there.
    # FROM DUAL is meaningless in Postgres.
    (re.compile(r"\bFROM\s+DUAL\b", re.IGNORECASE), ""),
    # HEXTORAW('AABB') -> '\xAABB'  (Postgres bytea hex literal)
    (re.compile(r"HEXTORAW\s*\(\s*'([0-9A-Fa-f]*)'\s*\)"), r"'\\x\1'"),
]

# TO_DATE / TO_TIMESTAMP with the formats we emit in step 1 -> Postgres casts.
_TO_TIMESTAMP_RE = re.compile(
    r"TO_TIMESTAMP\s*\(\s*'([^']*)'\s*,\s*'[^']*'\s*\)", re.IGNORECASE
)
_TO_DATE_RE = re.compile(
    r"TO_DATE\s*\(\s*'([^']*)'\s*,\s*'[^']*'\s*\)", re.IGNORECASE
)


def _convert_line(line: str, in_ddl: bool) -> str:
    """Apply the conversion rules to a single line of SQL."""
    # Date/time literal conversions first (they contain quoted strings).
    line = _TO_TIMESTAMP_RE.sub(r"TIMESTAMP '\1'", line)
    line = _TO_DATE_RE.sub(r"DATE '\1'", line)

    # Function / expression rewrites.
    for pattern, repl in _FUNC_RULES:
        line = pattern.sub(repl, line)

    # Data-type rewrites only matter inside DDL.
    if in_ddl:
        for pattern, repl in _TYPE_RULES:
            line = pattern.sub(repl, line)

    return line


def convert_oracle_sql_to_postgres(oracle_sql_path: str, table_name: str) -> str:
    """
    STEP 2: Convert an Oracle .sql file into a Postgres .sql file.
    Returns the path to the converted file.
    """
    out_path = os.path.join(POSTGRES_DUMP_DIR, f"{table_name}.sql")

    with open(oracle_sql_path, "r", encoding="utf-8") as src, \
         open(out_path, "w", encoding="utf-8") as dst:

        dst.write(f"-- Converted from Oracle dump: {os.path.basename(oracle_sql_path)}\n")
        dst.write(f"SET search_path TO {PG_SCHEMA};\n\n")

        in_ddl = False
        for raw_line in src:
            line = raw_line

            # Track whether we are inside a CREATE TABLE block so type rules apply there.
            stripped = line.strip().upper()
            if stripped.startswith("CREATE TABLE"):
                in_ddl = True
            line = _convert_line(line, in_ddl)
            if in_ddl and ");" in line:
                in_ddl = False

            # Oracle uses "" for quoting; Postgres uses "" too, so identifiers pass through.
            dst.write(line)

    print(f"  [convert] {table_name}: {oracle_sql_path} -> {out_path}")
    return out_path


def convert_oracle_sql_text(oracle_sql_text: str) -> str:
    """
    Convert a chunk of Oracle SQL text (e.g. constraint / index DDL) to Postgres.
    Used by the constraints pass. Type rules are skipped (no column type defs here);
    function rules (NVL, SYSDATE, ...) still apply, which matters for CHECK conditions.
    """
    return "".join(_convert_line(line, in_ddl=False) for line in oracle_sql_text.splitlines(keepends=True))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. STEP 3 — Prepare the target per `LOAD_MODE`, then bulk-load the CSV via `COPY`
# MAGIC
# MAGIC `COPY` is used instead of executing per-row `INSERT`s — it streams the whole CSV into
# MAGIC Postgres in one operation and is typically 10–100× faster on large tables.
# MAGIC
# MAGIC **`LOAD_MODE`** controls what happens to a pre-existing target table:
# MAGIC - `"recreate"` (default) — DROP (CASCADE) + CREATE from the converted DDL, then load. Runs
# MAGIC   the structural passes (constraints/indexes/identity/sequences/FKs).
# MAGIC - `"truncate"` — table must already exist; TRUNCATE it, then load **data only** (no DDL,
# MAGIC   no structural passes).
# MAGIC - `"append"` — table must already exist; load **data only**, no truncate (rows are added).
# MAGIC
# MAGIC In the data-only modes a preflight checks the table exists and its columns cover the data.

# COMMAND ----------

def _split_sql_statements(sql_text: str):
    """
    Split a SQL script into individual statements on semicolons,
    ignoring semicolons that appear inside single-quoted string literals.
    """
    statements = []
    buf = []
    in_string = False
    i = 0
    n = len(sql_text)
    while i < n:
        ch = sql_text[i]
        buf.append(ch)
        if ch == "'":
            # Handle escaped '' inside a string literal.
            if in_string and i + 1 < n and sql_text[i + 1] == "'":
                buf.append(sql_text[i + 1])
                i += 2
                continue
            in_string = not in_string
        elif ch == ";" and not in_string:
            stmt = "".join(buf).strip()
            if stmt and stmt != ";":
                statements.append(stmt.rstrip(";").strip())
            buf = []
        i += 1
    tail = "".join(buf).strip()
    if tail:
        statements.append(tail.rstrip(";").strip())
    return [s for s in statements if s]


def _assert_target_ready(cur, table_name: str, csv_columns):
    """For data-only modes: verify the target table exists and has the CSV's columns."""
    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = %s",
        (PG_SCHEMA, table_name),
    )
    existing = {r[0] for r in cur.fetchall()}
    if not existing:
        raise RuntimeError(
            f'LOAD_MODE="{LOAD_MODE}" but target table "{PG_SCHEMA}"."{table_name}" '
            f"does not exist. Create it first, or use LOAD_MODE=\"recreate\"."
        )
    missing = [c for c in csv_columns if c not in existing]
    if missing:
        raise RuntimeError(
            f'Target "{PG_SCHEMA}"."{table_name}" is missing column(s) {missing} '
            f"present in the Oracle data. Column names must match for COPY."
        )


def load_postgres_copy(pg_conn, postgres_ddl_path: str, csv_path: str, table_name: str):
    """
    STEP 3: Prepare the target table per LOAD_MODE, then bulk-load the CSV with COPY.

    LOAD_MODE:
      "recreate" — DROP (CASCADE) + CREATE from the converted DDL, then load.
      "truncate" — TRUNCATE the existing table, then load (data only).
      "append"   — load into the existing table as-is (data only).

    The CSV's first line is a header giving the column order.
    """
    cur = pg_conn.cursor()
    qualified = f'"{PG_SCHEMA}"."{table_name}"'
    try:
        if LOAD_MODE == "recreate":
            cur.execute(f"DROP TABLE IF EXISTS {qualified} CASCADE;")
            with open(postgres_ddl_path, "r", encoding="utf-8") as f:
                for stmt in _split_sql_statements(f.read()):
                    cur.execute(stmt)

        # Bulk-load the data. Read the header to pin the column order, then COPY the rest.
        with open(csv_path, "r", encoding="utf-8", newline="") as f:
            header = next(csv.reader([f.readline()]))

            if LOAD_MODE in ("truncate", "append"):
                _assert_target_ready(cur, table_name, header)
            if LOAD_MODE == "truncate":
                cur.execute(f"TRUNCATE TABLE {qualified};")

            col_list = ", ".join('"' + c.replace('"', '""') + '"' for c in header)
            copy_sql = (
                f"COPY {qualified} ({col_list}) "
                f"FROM STDIN WITH (FORMAT csv, HEADER false, NULL '')"
            )
            cur.copy_expert(copy_sql, f)      # f is now positioned just after the header
            row_count = cur.rowcount

        pg_conn.commit()
        print(f"  [load]    {table_name}: COPY {row_count} rows ({LOAD_MODE})")
    except Exception:
        pg_conn.rollback()
        raise
    finally:
        cur.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5b. SECOND PASS — Primary keys, unique/check constraints, indexes & foreign keys
# MAGIC
# MAGIC The first pass only carries column definitions + data. This pass reads the Oracle data
# MAGIC dictionary (`all_constraints`, `all_cons_columns`, `all_indexes`, `all_ind_columns`) and
# MAGIC reproduces the relational structure on Postgres:
# MAGIC
# MAGIC - **PK / UNIQUE / CHECK** and **indexes** are applied per-table (they only reference one table).
# MAGIC - **FOREIGN KEYs** are collected across all tables and applied **last**, after every table and
# MAGIC   its data exist, so referential integrity doesn't fail on table/row ordering.
# MAGIC
# MAGIC As with the data pass, DDL is dumped Oracle-flavoured ➜ converted ➜ loaded.

# COMMAND ----------

def _pg_ident(name: str) -> str:
    """Quote an identifier for Postgres."""
    return '"' + name.replace('"', '""') + '"'


def _pg_qualified(table_name: str) -> str:
    return f"{_pg_ident(PG_SCHEMA)}.{_pg_ident(table_name)}"


def _constraint_columns(ora_conn, owner: str, constraint_name: str):
    """Ordered column list for a given constraint."""
    cur = ora_conn.cursor()
    cur.execute(
        """
        SELECT column_name
        FROM all_cons_columns
        WHERE owner = :owner AND constraint_name = :cname
        ORDER BY position
        """,
        owner=owner, cname=constraint_name,
    )
    cols = [r[0] for r in cur.fetchall()]
    cur.close()
    return cols


def build_oracle_constraint_ddl(ora_conn, table_name: str):
    """
    Read PK / UNIQUE / CHECK constraints and indexes for a table and return
    Oracle-flavoured DDL (str). Foreign keys are intentionally excluded here.
    """
    owner = ORACLE_SCHEMA.upper()
    qualified = f'"{table_name}"'
    lines = [f"-- constraints & indexes for {table_name}"]

    cur = ora_conn.cursor()

    # --- PK / UNIQUE / CHECK constraints ---
    if MIGRATE_PK_UNIQUE_CHECK:
        cur.execute(
            """
            SELECT constraint_name, constraint_type, search_condition
            FROM all_constraints
            WHERE owner = :owner AND table_name = :tname
              AND constraint_type IN ('P', 'U', 'C')
              AND status = 'ENABLED'
            ORDER BY constraint_type, constraint_name
            """,
            owner=owner, tname=table_name,
        )
        for cname, ctype, search_cond in cur.fetchall():
            if ctype in ("P", "U"):
                cols = _constraint_columns(ora_conn, owner, cname)
                if not cols:
                    continue
                kind = "PRIMARY KEY" if ctype == "P" else "UNIQUE"
                col_list = ", ".join(f'"{c}"' for c in cols)
                lines.append(
                    f'ALTER TABLE {qualified} ADD CONSTRAINT "{cname}" {kind} ({col_list});'
                )
            elif ctype == "C":
                cond = (search_cond or "").strip()
                # Skip the system-generated "COL" IS NOT NULL checks (already NOT NULL in DDL).
                if not cond or re.match(r'^"?\w+"?\s+IS\s+NOT\s+NULL$', cond, re.IGNORECASE):
                    continue
                lines.append(
                    f'ALTER TABLE {qualified} ADD CONSTRAINT "{cname}" CHECK ({cond});'
                )

    # --- Indexes (excluding those backing PK / UNIQUE constraints) ---
    if MIGRATE_INDEXES:
        cur.execute(
            """
            SELECT index_name, uniqueness
            FROM all_indexes
            WHERE table_owner = :owner AND table_name = :tname
              AND index_type = 'NORMAL'
              AND index_name NOT IN (
                  SELECT constraint_name FROM all_constraints
                  WHERE owner = :owner AND table_name = :tname
                    AND constraint_type IN ('P', 'U')
              )
            ORDER BY index_name
            """,
            owner=owner, tname=table_name,
        )
        index_rows = cur.fetchall()
        for index_name, uniqueness in index_rows:
            icur = ora_conn.cursor()
            icur.execute(
                """
                SELECT column_name
                FROM all_ind_columns
                WHERE index_owner = :owner AND index_name = :iname
                ORDER BY column_position
                """,
                owner=owner, iname=index_name,
            )
            icols = [r[0] for r in icur.fetchall()]
            icur.close()
            if not icols:
                continue
            unique = "UNIQUE " if uniqueness == "UNIQUE" else ""
            col_list = ", ".join(f'"{c}"' for c in icols)
            lines.append(
                f'CREATE {unique}INDEX "{index_name}" ON {qualified} ({col_list});'
            )

    cur.close()
    return "\n".join(lines) + "\n"


def build_oracle_foreign_key_ddl(ora_conn, table_name: str):
    """Return Oracle-flavoured FOREIGN KEY DDL (str) for a single table."""
    owner = ORACLE_SCHEMA.upper()
    qualified = f'"{table_name}"'
    lines = []

    cur = ora_conn.cursor()
    cur.execute(
        """
        SELECT c.constraint_name, c.r_owner, c.r_constraint_name,
               c.delete_rule, rc.table_name AS ref_table
        FROM all_constraints c
        JOIN all_constraints rc
          ON rc.owner = c.r_owner AND rc.constraint_name = c.r_constraint_name
        WHERE c.owner = :owner AND c.table_name = :tname
          AND c.constraint_type = 'R' AND c.status = 'ENABLED'
        ORDER BY c.constraint_name
        """,
        owner=owner, tname=table_name,
    )
    for cname, r_owner, r_cname, delete_rule, ref_table in cur.fetchall():
        local_cols = _constraint_columns(ora_conn, owner, cname)
        ref_cols = _constraint_columns(ora_conn, r_owner, r_cname)
        if not local_cols or not ref_cols:
            continue
        local_list = ", ".join(f'"{c}"' for c in local_cols)
        ref_list = ", ".join(f'"{c}"' for c in ref_cols)
        # Referenced table is migrated into PG_SCHEMA as well.
        on_delete = ""
        if delete_rule and delete_rule.upper() in ("CASCADE", "SET NULL"):
            on_delete = f" ON DELETE {delete_rule.upper()}"
        lines.append(
            f'ALTER TABLE {qualified} ADD CONSTRAINT "{cname}" '
            f'FOREIGN KEY ({local_list}) '
            f'REFERENCES "{ref_table}" ({ref_list}){on_delete};'
        )
    cur.close()
    return "\n".join(lines)


def migrate_table_constraints(ora_conn, pg_conn, table_name: str):
    """Dump ➜ convert ➜ load PK / UNIQUE / CHECK / indexes for one table."""
    if not (MIGRATE_PK_UNIQUE_CHECK or MIGRATE_INDEXES):
        return
    oracle_ddl = build_oracle_constraint_ddl(ora_conn, table_name)
    postgres_ddl = convert_oracle_sql_text(oracle_ddl)

    ora_path = os.path.join(ORACLE_DUMP_DIR, f"{table_name}_constraints.sql")
    pg_path = os.path.join(POSTGRES_DUMP_DIR, f"{table_name}_constraints.sql")
    with open(ora_path, "w", encoding="utf-8") as f:
        f.write(oracle_ddl)
    with open(pg_path, "w", encoding="utf-8") as f:
        f.write(f"SET search_path TO {PG_SCHEMA};\n")
        f.write(postgres_ddl)

    cur = pg_conn.cursor()
    try:
        executed = 0
        for stmt in _split_sql_statements(postgres_ddl):
            cur.execute(stmt)
            executed += 1
        pg_conn.commit()
        if executed:
            print(f"  [constr]  {table_name}: applied {executed} constraint/index statements")
    except Exception:
        pg_conn.rollback()
        raise
    finally:
        cur.close()
        if not KEEP_SQL_FILES:
            os.remove(ora_path)
            os.remove(pg_path)


def migrate_foreign_keys(ora_conn, pg_conn, table_names):
    """
    Final pass: dump ➜ convert ➜ load all FOREIGN KEYs once every table exists.
    Each FK is applied independently so one bad FK doesn't abort the rest.
    """
    if not MIGRATE_FOREIGN_KEYS:
        return 0

    all_fk_lines = []
    for table_name in table_names:
        ddl = build_oracle_foreign_key_ddl(ora_conn, table_name)
        if ddl.strip():
            all_fk_lines.append(ddl)

    if not all_fk_lines:
        print("No foreign keys to migrate.")
        return 0

    oracle_ddl = "-- foreign keys (all tables)\n" + "\n".join(all_fk_lines) + "\n"
    postgres_ddl = convert_oracle_sql_text(oracle_ddl)

    ora_path = os.path.join(ORACLE_DUMP_DIR, "_foreign_keys.sql")
    pg_path = os.path.join(POSTGRES_DUMP_DIR, "_foreign_keys.sql")
    with open(ora_path, "w", encoding="utf-8") as f:
        f.write(oracle_ddl)
    with open(pg_path, "w", encoding="utf-8") as f:
        f.write(f"SET search_path TO {PG_SCHEMA};\n")
        f.write(postgres_ddl)

    applied, failed = 0, 0
    cur = pg_conn.cursor()
    for stmt in _split_sql_statements(postgres_ddl):
        try:
            cur.execute(stmt)
            pg_conn.commit()
            applied += 1
        except Exception as exc:  # noqa: BLE001
            pg_conn.rollback()
            failed += 1
            print(f"  [fk ERROR] {exc}")
    cur.close()
    print(f"Foreign keys: applied {applied}, failed {failed}")
    if not KEEP_SQL_FILES:
        os.remove(ora_path)
        os.remove(pg_path)
    return applied

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5c. THIRD PASS — Sequences & identity columns
# MAGIC
# MAGIC Auto-increment behaviour in Oracle comes in two shapes, both handled here:
# MAGIC
# MAGIC - **Standalone sequences** (`all_sequences`) ➜ Postgres `CREATE SEQUENCE`, started at the
# MAGIC   sequence's current high-water value. Oracle's enormous default `MAXVALUE` (28 nines)
# MAGIC   exceeds Postgres `bigint`, so it is clamped to `NO MAXVALUE` when out of range.
# MAGIC - **Identity columns** (`all_tab_identity_cols`, Oracle 12c+) ➜ the column is altered to
# MAGIC   `GENERATED {ALWAYS|BY DEFAULT} AS IDENTITY` *after* its data is loaded, then `RESTART WITH
# MAGIC   MAX(col)+1` so future inserts don't collide with migrated rows.
# MAGIC
# MAGIC > Pre-12c "sequence + BEFORE INSERT trigger" auto-increment is **not** auto-detected — convert
# MAGIC > those columns to identity manually or rely on the migrated standalone sequence.

# COMMAND ----------

# Postgres bigint bounds — Oracle sequences can exceed these.
_PG_BIGINT_MAX = 9223372036854775807
_PG_BIGINT_MIN = -9223372036854775808


def migrate_sequences(ora_conn, pg_conn):
    """Re-create every standalone Oracle sequence in Postgres (run once)."""
    if not MIGRATE_SEQUENCES:
        return 0

    cur = ora_conn.cursor()
    cur.execute(
        """
        SELECT sequence_name, min_value, max_value, increment_by,
               cycle_flag, cache_size, last_number
        FROM all_sequences
        WHERE sequence_owner = :owner
        ORDER BY sequence_name
        """,
        owner=ORACLE_SCHEMA.upper(),
    )
    rows = cur.fetchall()
    cur.close()

    if not rows:
        print("No sequences to migrate.")
        return 0

    statements = []
    for name, min_v, max_v, incr, cycle_flag, cache, last_number in rows:
        min_v = int(min_v)
        max_v = int(max_v)
        incr = int(incr or 1)
        start = int(last_number or min_v)

        minclause = f"MINVALUE {min_v}" if min_v >= _PG_BIGINT_MIN else "NO MINVALUE"
        maxclause = f"MAXVALUE {max_v}" if max_v <= _PG_BIGINT_MAX else "NO MAXVALUE"
        # Clamp the start value into the representable range as well.
        start = max(min(start, _PG_BIGINT_MAX), _PG_BIGINT_MIN)
        cacheclause = f"CACHE {int(cache)}" if cache and int(cache) > 1 else "CACHE 1"
        cycleclause = "CYCLE" if (cycle_flag or "N").upper() == "Y" else "NO CYCLE"

        statements.append(
            f'CREATE SEQUENCE IF NOT EXISTS {_pg_qualified(name)} '
            f'INCREMENT BY {incr} {minclause} {maxclause} '
            f'START WITH {start} {cacheclause} {cycleclause};'
        )

    # Dump ➜ (no conversion needed, already Postgres) ➜ load.
    sql_text = f"SET search_path TO {PG_SCHEMA};\n" + "\n".join(statements) + "\n"
    pg_path = os.path.join(POSTGRES_DUMP_DIR, "_sequences.sql")
    with open(pg_path, "w", encoding="utf-8") as f:
        f.write(sql_text)

    applied = 0
    cur = pg_conn.cursor()
    try:
        for stmt in statements:
            cur.execute(stmt)
            applied += 1
        pg_conn.commit()
    except Exception:
        pg_conn.rollback()
        raise
    finally:
        cur.close()
        if not KEEP_SQL_FILES:
            os.remove(pg_path)
    print(f"Sequences: created {applied}")
    return applied


def migrate_identity_columns(ora_conn, pg_conn, table_name: str):
    """Convert Oracle IDENTITY columns on one table to Postgres IDENTITY columns."""
    if not MIGRATE_IDENTITY_COLUMNS:
        return

    cur = ora_conn.cursor()
    cur.execute(
        """
        SELECT column_name, generation_type
        FROM all_tab_identity_cols
        WHERE owner = :owner AND table_name = :tname
        """,
        owner=ORACLE_SCHEMA.upper(), tname=table_name,
    )
    identity_cols = cur.fetchall()
    cur.close()
    if not identity_cols:
        return

    qualified = _pg_qualified(table_name)
    pgcur = pg_conn.cursor()
    try:
        for column_name, generation_type in identity_cols:
            col = _pg_ident(column_name)
            gen = "ALWAYS" if (generation_type or "").upper() == "ALWAYS" else "BY DEFAULT"

            # Find the current max so the identity sequence resumes past migrated data.
            pgcur.execute(f"SELECT COALESCE(MAX({col}), 0) FROM {qualified}")
            current_max = pgcur.fetchone()[0] or 0

            pgcur.execute(
                f"ALTER TABLE {qualified} ALTER COLUMN {col} "
                f"ADD GENERATED {gen} AS IDENTITY;"
            )
            pgcur.execute(
                f"ALTER TABLE {qualified} ALTER COLUMN {col} "
                f"RESTART WITH {int(current_max) + 1};"
            )
            print(f"  [ident]   {table_name}.{column_name}: GENERATED {gen} AS IDENTITY "
                  f"(restart {int(current_max) + 1})")
        pg_conn.commit()
    except Exception:
        pg_conn.rollback()
        raise
    finally:
        pgcur.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5d. Dependency-aware load ordering
# MAGIC
# MAGIC Some tables must load before others (parent before child). We read the foreign-key graph
# MAGIC from `all_constraints` (type `R`) — **restricted to the tables being migrated** — and
# MAGIC topologically sort it so every parent loads before its children (Kahn's algorithm).
# MAGIC
# MAGIC Robustness details:
# MAGIC - **Self-references** (a table FK'ing itself) are ignored for ordering — a single table is
# MAGIC   always loadable on its own; the FK is satisfied later by the deferred FK pass.
# MAGIC - **Cycles** (e.g. A→B→A) cannot be fully ordered. They are detected, logged, and broken by
# MAGIC   emitting the lowest-remaining-dependency tables in a stable order; those rows rely on the
# MAGIC   deferred FK pass for integrity.
# MAGIC - **Deterministic**: ties broken alphabetically, so the same input always yields the same order.
# MAGIC - FKs pointing at tables **outside** the migration set are ignored for ordering (we can't load
# MAGIC   what we're not migrating), but are reported so you know they exist.

# COMMAND ----------

def build_fk_dependency_graph(ora_conn, table_names):
    """
    Build the FK dependency graph for the given tables.

    Returns (deps, external_refs) where:
      deps[child] = set(parents)  — parents that must load before `child`
                                    (self-refs removed; only in-scope parents kept)
      external_refs[child] = set(parents outside the migration set)
    """
    in_scope = {t.upper() for t in table_names}
    owner = ORACLE_SCHEMA.upper()

    deps = {t: set() for t in table_names}
    # Map uppercase -> original spelling so we preserve the caller's casing.
    canonical = {t.upper(): t for t in table_names}
    external_refs = {}

    cur = ora_conn.cursor()
    cur.execute(
        """
        SELECT c.table_name AS child_table, rc.table_name AS parent_table
        FROM all_constraints c
        JOIN all_constraints rc
          ON rc.owner = c.r_owner AND rc.constraint_name = c.r_constraint_name
        WHERE c.owner = :owner AND c.constraint_type = 'R' AND c.status = 'ENABLED'
        """,
        owner=owner,
    )
    rows = cur.fetchall()
    cur.close()

    for child_raw, parent_raw in rows:
        child_u, parent_u = child_raw.upper(), parent_raw.upper()
        if child_u not in in_scope:
            continue  # the dependent table isn't part of this migration
        if parent_u == child_u:
            continue  # self-reference — not an ordering constraint
        if parent_u in in_scope:
            deps[canonical[child_u]].add(canonical[parent_u])
        else:
            external_refs.setdefault(canonical[child_u], set()).add(parent_raw)

    return deps, external_refs


def order_tables_by_dependency(ora_conn, table_names):
    """
    Return table_names reordered so FK parents load before children (Kahn's algorithm).

    Robust to cycles: when no zero-dependency table remains, the cycle is broken by
    choosing the remaining table with the fewest unmet dependencies (ties alphabetical),
    which is logged. Output always contains exactly the input tables, once each.
    """
    deps, external_refs = build_fk_dependency_graph(ora_conn, table_names)

    if external_refs:
        print("Note: FKs referencing tables OUTSIDE the migration set (ignored for ordering):")
        for child, parents in sorted(external_refs.items()):
            print(f"  - {child} -> {', '.join(sorted(parents))}")

    # Work on a mutable copy of the dependency sets.
    remaining = {t: set(parents) for t, parents in deps.items()}
    ordered = []
    placed = set()
    cycles_broken = []

    while remaining:
        # Tables whose parents are all already placed.
        ready = sorted(t for t, parents in remaining.items() if not (parents - placed))
        if not ready:
            # Cycle (or mutual dependency): break it deterministically.
            unmet = lambda t: len(remaining[t] - placed)
            victim = sorted(remaining, key=lambda t: (unmet(t), t))[0]
            cycles_broken.append(victim)
            ready = [victim]

        for t in ready:
            ordered.append(t)
            placed.add(t)
            del remaining[t]

    if cycles_broken:
        print("WARNING: FK dependency cycle(s) detected. Order forced for these tables "
              "(they rely on the deferred FK pass for integrity):")
        for t in cycles_broken:
            print(f"  - {t}")

    return ordered


def resolve_load_order(ora_conn, table_names):
    """Apply RESPECT_LOAD_ORDER: keep the given order, or sort by FK dependency."""
    if RESPECT_LOAD_ORDER:
        print("Load order: using TABLE_NAMES order as-is (RESPECT_LOAD_ORDER=True).")
        return list(table_names)
    print("Load order: resolving FK dependencies (parents before children)...")
    ordered = order_tables_by_dependency(ora_conn, table_names)
    print(f"Load order resolved for {len(ordered)} tables.")
    return ordered

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Orchestration — parallel fan-out (thread pool) with bracketing sequence/FK passes
# MAGIC
# MAGIC Tables are migrated concurrently with a thread pool of `MAX_PARALLEL_TABLES` workers. Each
# MAGIC worker runs the full per-table pipeline (dump ➜ convert ➜ COPY ➜ constraints ➜ identity)
# MAGIC on its **own** Oracle + Postgres connections — the drivers are not safe to share across
# MAGIC threads. The work is I/O-bound (DB + file I/O releases the GIL), so threads give real
# MAGIC concurrency and overlap Oracle reads with Postgres writes across different tables.
# MAGIC
# MAGIC Sequences run once **before** the pool; foreign keys run once **after** it (deferred-FK
# MAGIC design), so load order is irrelevant to correctness and tables can finish in any order.

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor, as_completed


def discover_oracle_tables(ora_conn):
    """Return all table names owned by ORACLE_SCHEMA."""
    cur = ora_conn.cursor()
    cur.execute(
        "SELECT table_name FROM all_tables WHERE owner = :owner ORDER BY table_name",
        owner=ORACLE_SCHEMA.upper(),
    )
    names = [r[0] for r in cur.fetchall()]
    cur.close()
    return names


def migrate_one_table(table_name: str):
    """
    Full per-table pipeline on dedicated connections (safe to run in a worker thread).
    Returns (table_name, ok: bool, error: str | None, seconds: float).
    """
    t0 = datetime.datetime.now()
    ora_conn = get_oracle_connection()
    pg_conn = get_postgres_connection()
    try:
        ddl_path, csv_path = dump_oracle_table(ora_conn, table_name)        # STEP 1
        pg_ddl_path = convert_oracle_sql_to_postgres(ddl_path, table_name)  # STEP 2
        load_postgres_copy(pg_conn, pg_ddl_path, csv_path, table_name)      # STEP 3
        if LOAD_MODE == "recreate":
            # Structural passes only when we built the table; in data-only modes
            # (truncate/append) the target schema already has these.
            migrate_table_constraints(ora_conn, pg_conn, table_name)       # PK/UNIQUE/CHECK/index
            migrate_identity_columns(ora_conn, pg_conn, table_name)        # identity columns

        if not KEEP_SQL_FILES:
            for p in (ddl_path, csv_path, pg_ddl_path):
                try:
                    os.remove(p)
                except OSError:
                    pass

        elapsed = (datetime.datetime.now() - t0).total_seconds()
        return (table_name, True, None, elapsed)
    except Exception as exc:  # noqa: BLE001
        elapsed = (datetime.datetime.now() - t0).total_seconds()
        return (table_name, False, str(exc), elapsed)
    finally:
        ora_conn.close()
        pg_conn.close()


def migrate_all_tables():
    """Run the full dump ➜ convert ➜ COPY pipeline for every table, in parallel."""
    if LOAD_MODE not in ("recreate", "truncate", "append"):
        raise ValueError(f'LOAD_MODE must be "recreate", "truncate" or "append", got "{LOAD_MODE}"')

    setup_ora = get_oracle_connection()
    setup_pg = get_postgres_connection()

    try:
        table_names = discover_oracle_tables(setup_ora) if DISCOVER_TABLES else list(TABLE_NAMES)
        # Ordering is computed for logging/visibility; with parallel fan-out + deferred FKs it is
        # not required for correctness (tables may finish in any order).
        table_names = resolve_load_order(setup_ora, table_names)
        total = len(table_names)
        workers = max(1, int(MAX_PARALLEL_TABLES))
        print(f"Migrating {total} tables with {workers} parallel worker(s) [LOAD_MODE={LOAD_MODE}]\n"
              + "=" * 60)

        results = {"ok": [], "failed": []}
        start_all = datetime.datetime.now()

        # Standalone sequences: create up front (only when building schema, not in data-only modes).
        if LOAD_MODE == "recreate":
            migrate_sequences(setup_ora, setup_pg)
            print("-" * 60)

        # Parallel fan-out: each table is migrated end-to-end on its own connections.
        done = 0
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(migrate_one_table, t): t for t in table_names}
            for fut in as_completed(futures):
                table_name, ok, err, elapsed = fut.result()
                done += 1
                if ok:
                    print(f"[{done}/{total}] [done]  {table_name} in {elapsed:.1f}s")
                    results["ok"].append(table_name)
                else:
                    print(f"[{done}/{total}] [ERROR] {table_name}: {err}")
                    results["failed"].append((table_name, err))

        # FINAL PASS: foreign keys, once every table + its data exists.
        # Only when building schema; data-only modes leave existing FKs untouched.
        if LOAD_MODE == "recreate":
            print("\n" + "-" * 60)
            ok_in_order = [t for t in table_names if t in set(results["ok"])]
            migrate_foreign_keys(setup_ora, setup_pg, ok_in_order)

        total_elapsed = (datetime.datetime.now() - start_all).total_seconds()
        print("\n" + "=" * 60)
        print(f"Finished in {total_elapsed:.1f}s — "
              f"{len(results['ok'])} ok, {len(results['failed'])} failed")
        if results["failed"]:
            print("\nFailed tables:")
            for name, err in results["failed"]:
                print(f"  - {name}: {err}")
            if not CONTINUE_ON_ERROR:
                raise RuntimeError(f"{len(results['failed'])} table(s) failed to migrate")
        return results
    finally:
        setup_ora.close()
        setup_pg.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Run the migration

# COMMAND ----------

results = migrate_all_tables()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. (Optional) Summary as a Spark DataFrame for easy inspection

# COMMAND ----------

summary_rows = (
    [(t, "OK", "") for t in results["ok"]]
    + [(t, "FAILED", err) for t, err in results["failed"]]
)
summary_df = spark.createDataFrame(summary_rows, ["table_name", "status", "error"])
display(summary_df)
