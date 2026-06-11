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

BATCH_SIZE = 5_000                          # rows fetched / inserted per batch
DROP_TARGET_BEFORE_LOAD = True              # DROP TABLE IF EXISTS on the Postgres side first
CREATE_TARGET_TABLE = True                  # emit / run CREATE TABLE DDL on the Postgres side
CONTINUE_ON_ERROR = True                    # keep migrating remaining tables if one fails
KEEP_SQL_FILES = True                       # keep intermediate .sql files for auditing

# Constraint / index migration (second pass).
MIGRATE_PK_UNIQUE_CHECK = True              # add PRIMARY KEY / UNIQUE / CHECK constraints
MIGRATE_INDEXES = True                      # re-create non-constraint indexes
MIGRATE_FOREIGN_KEYS = True                 # add FOREIGN KEYs (applied last, after all tables load)

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
# MAGIC ## 3. STEP 1 — Dump an Oracle table (DDL + data) to an Oracle `.sql` file
# MAGIC
# MAGIC We read the column metadata to build a `CREATE TABLE`, then stream the rows in batches
# MAGIC and emit `INSERT` statements with Oracle-flavoured literals (e.g. `TO_TIMESTAMP(...)`).

# COMMAND ----------

import datetime
import decimal


def _oracle_quote_ident(name: str) -> str:
    """Quote an Oracle identifier."""
    return '"' + name.replace('"', '""') + '"'


def _oracle_literal(value) -> str:
    """Render a Python value as an Oracle SQL literal."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float, decimal.Decimal)):
        return str(value)
    if isinstance(value, datetime.datetime):
        return "TO_TIMESTAMP('" + value.strftime("%Y-%m-%d %H:%M:%S.%f") + "', 'YYYY-MM-DD HH24:MI:SS.FF6')"
    if isinstance(value, datetime.date):
        return "TO_DATE('" + value.strftime("%Y-%m-%d") + "', 'YYYY-MM-DD')"
    if isinstance(value, (bytes, bytearray)):
        return "HEXTORAW('" + value.hex().upper() + "')"
    # Default: string. Escape single quotes by doubling them.
    return "'" + str(value).replace("'", "''") + "'"


def _oracle_type_from_cursor(col) -> str:
    """Map a python-oracledb cursor description entry to an Oracle column type string."""
    name, type_obj, display_size, internal_size, precision, scale, null_ok = col
    tname = getattr(type_obj, "name", str(type_obj)).upper()

    if "VARCHAR" in tname or tname in ("DB_TYPE_VARCHAR", "STRING"):
        return f"VARCHAR2({internal_size or 4000})"
    if "CHAR" in tname:
        return f"CHAR({internal_size or 1})"
    if "CLOB" in tname:
        return "CLOB"
    if "BLOB" in tname:
        return "BLOB"
    if "RAW" in tname:
        return f"RAW({internal_size or 2000})"
    if "TIMESTAMP" in tname:
        return "TIMESTAMP"
    if "DATE" in tname:
        return "DATE"
    if "NUMBER" in tname or "NUMERIC" in tname or "DECIMAL" in tname:
        if precision:
            return f"NUMBER({precision},{scale or 0})"
        return "NUMBER"
    if "FLOAT" in tname or "DOUBLE" in tname or "BINARY_DOUBLE" in tname:
        return "FLOAT"
    if "INT" in tname:
        return "NUMBER(38,0)"
    return "VARCHAR2(4000)"


def dump_oracle_table(ora_conn, table_name: str) -> str:
    """
    STEP 1: Dump a single Oracle table (DDL + data) to an Oracle-flavoured .sql file.
    Returns the path to the written file.
    """
    out_path = os.path.join(ORACLE_DUMP_DIR, f"{table_name}.sql")
    qualified = f"{_oracle_quote_ident(ORACLE_SCHEMA)}.{_oracle_quote_ident(table_name)}"

    cursor = ora_conn.cursor()
    cursor.arraysize = BATCH_SIZE
    cursor.execute(f"SELECT * FROM {qualified}")

    columns = cursor.description
    col_names = [c[0] for c in columns]

    with open(out_path, "w", encoding="utf-8") as f:
        f.write(f"-- Oracle dump of {ORACLE_SCHEMA}.{table_name}\n")
        f.write(f"-- generated {datetime.datetime.utcnow().isoformat()}Z\n\n")

        # --- DDL ---
        f.write(f"CREATE TABLE {_oracle_quote_ident(table_name)} (\n")
        col_defs = []
        for col in columns:
            col_name, _, _, _, _, _, null_ok = col
            col_type = _oracle_type_from_cursor(col)
            nullable = "" if null_ok else " NOT NULL"
            col_defs.append(f"    {_oracle_quote_ident(col_name)} {col_type}{nullable}")
        f.write(",\n".join(col_defs))
        f.write("\n);\n\n")

        # --- DATA ---
        insert_prefix = (
            f"INSERT INTO {_oracle_quote_ident(table_name)} "
            f"({', '.join(_oracle_quote_ident(c) for c in col_names)}) VALUES "
        )
        row_count = 0
        while True:
            rows = cursor.fetchmany(BATCH_SIZE)
            if not rows:
                break
            for row in rows:
                literals = ", ".join(_oracle_literal(v) for v in row)
                f.write(f"{insert_prefix}({literals});\n")
            row_count += len(rows)
        f.write(f"\nCOMMIT;\n")

    cursor.close()
    print(f"  [dump]    {table_name}: {row_count} rows -> {out_path}")
    return out_path

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. STEP 2 — Convert Oracle `.sql` to Postgres `.sql` (syntax conversion)
# MAGIC
# MAGIC A line/regex based converter covering the most common Oracle ➜ Postgres differences:
# MAGIC data types, function names, sequence/DUAL idioms, identifier quoting and date literals.
# MAGIC Extend `_TYPE_RULES` / `_FUNC_RULES` for any project-specific syntax you hit.

# COMMAND ----------

import re

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
# MAGIC ## 5. STEP 3 — Load the converted Postgres `.sql` file into Postgres

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


def load_postgres_sql(pg_conn, postgres_sql_path: str, table_name: str):
    """
    STEP 3: Execute the converted Postgres .sql file against the target Postgres DB.
    """
    with open(postgres_sql_path, "r", encoding="utf-8") as f:
        sql_text = f.read()

    cur = pg_conn.cursor()
    qualified = f'"{PG_SCHEMA}"."{table_name}"'
    try:
        if DROP_TARGET_BEFORE_LOAD:
            cur.execute(f"DROP TABLE IF EXISTS {qualified} CASCADE;")

        executed = 0
        for stmt in _split_sql_statements(sql_text):
            upper = stmt.lstrip().upper()
            # Honour the create-table / commit flags.
            if upper.startswith("CREATE TABLE") and not CREATE_TARGET_TABLE:
                continue
            if upper.startswith("COMMIT"):
                continue  # we commit explicitly below
            cur.execute(stmt)
            executed += 1

        pg_conn.commit()
        print(f"  [load]    {table_name}: executed {executed} statements")
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
# MAGIC ## 6. Orchestration — loop over all tables (dump ➜ convert ➜ load ➜ constraints, then FKs)

# COMMAND ----------

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


def migrate_all_tables():
    """Run the full dump ➜ convert ➜ load pipeline for every table."""
    ora_conn = get_oracle_connection()
    pg_conn = get_postgres_connection()

    try:
        table_names = discover_oracle_tables(ora_conn) if DISCOVER_TABLES else list(TABLE_NAMES)
        total = len(table_names)
        print(f"Migrating {total} tables\n" + "=" * 60)

        results = {"ok": [], "failed": []}
        start_all = datetime.datetime.now()

        for idx, table_name in enumerate(table_names, start=1):
            print(f"\n[{idx}/{total}] {table_name}")
            t0 = datetime.datetime.now()
            try:
                # STEP 1: dump from Oracle
                oracle_sql = dump_oracle_table(ora_conn, table_name)
                # STEP 2: convert Oracle SQL -> Postgres SQL
                postgres_sql = convert_oracle_sql_to_postgres(oracle_sql, table_name)
                # STEP 3: load into Postgres
                load_postgres_sql(pg_conn, postgres_sql, table_name)
                # SECOND PASS (per-table): PK / UNIQUE / CHECK / indexes
                migrate_table_constraints(ora_conn, pg_conn, table_name)

                if not KEEP_SQL_FILES:
                    os.remove(oracle_sql)
                    os.remove(postgres_sql)

                elapsed = (datetime.datetime.now() - t0).total_seconds()
                print(f"  [done]    {table_name} in {elapsed:.1f}s")
                results["ok"].append(table_name)
            except Exception as exc:  # noqa: BLE001
                print(f"  [ERROR]   {table_name}: {exc}")
                results["failed"].append((table_name, str(exc)))
                if not CONTINUE_ON_ERROR:
                    raise

        # FINAL PASS: foreign keys, once every table + its data exists.
        print("\n" + "-" * 60)
        migrate_foreign_keys(ora_conn, pg_conn, [t for t in table_names if t in results["ok"]])

        total_elapsed = (datetime.datetime.now() - start_all).total_seconds()
        print("\n" + "=" * 60)
        print(f"Finished in {total_elapsed:.1f}s — "
              f"{len(results['ok'])} ok, {len(results['failed'])} failed")
        if results["failed"]:
            print("\nFailed tables:")
            for name, err in results["failed"]:
                print(f"  - {name}: {err}")
        return results
    finally:
        ora_conn.close()
        pg_conn.close()

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
