# Project Handover — Oracle ➜ Postgres Table Migration (Databricks Notebook)

> Purpose of this file: give another engineer (or another Claude instance) **complete
> context to pick this project up cold** — what it is, why each decision was made, how the
> code is structured, what's done, what's deliberately out of scope, and what to do next.

---

## 1. What this project is

A **single Databricks notebook** that migrates **400+ tables** from an Oracle database to a
Postgres database. It was built from a short pseudocode/architecture spec (see
*Original spec* below). The deliverable:

- **`oracle_to_postgres_migration.py`** — Databricks notebook in *source* format
  (`# Databricks notebook source` header, cells separated by `# COMMAND ----------`,
  markdown cells via `# MAGIC %md`). It imports straight into a Databricks workspace.
  ~1150 lines, pure-Python, no widgets. Speed-oriented: CSV `COPY` load + parallel fan-out.
- **Local Docker test environment** (`docker-compose.yml` + `docker/`) — Oracle + Postgres
  containers with a seeded source schema so the notebook can be test-run end to end.
  See §10 below and `docker/README.md`.

### Original spec (verbatim intent)
```
Oracle DB connection
tablenames in an array of strings
For each of the 400+ tables from array of names {
  1. Pull entire table data from direct oracle db via dump onto oracle sql file
  2. Convert Oracle SQL File to Postgres SQL File. Syntax Conversion
  3. Upload entire table data from Converted Postgres SQL File to direct postgres db table
}
Postgres DB Connection
```
Hard requirements from the user: **connection details in variables**, **NO WIDGETS**.

---

## 2. Repository & branch

- Repo: `sleepygabe/acs-pipeline-tmp` (started empty; this notebook is the first content).
- **Working branch: `claude/databricks-notebook-pseudocode-vcc1cs`** — all work commits/pushes here.
- Do **NOT** push to other branches without explicit permission. Do **NOT** open a PR unless asked.
- Commit message convention used: descriptive subject + body, ending with the session URL line.

### Commit history (newest last)
```
eb391ba  Add Databricks notebook for Oracle to Postgres table migration   (base flow: dump→convert→load)
1897281  Add second pass: PK/unique/check constraints, indexes and foreign keys
3676871  Add sequence and identity column migration
3063469  Add dependency-aware load ordering (FK topological sort)
2cb17fc  Document syntax-only converter scope (table data dumps only)
```

---

## 3. End-to-end flow (how it actually runs)

Entry point: **`migrate_all_tables()`** (cell §6). It runs tables **in parallel** via a thread
pool (`MAX_PARALLEL_TABLES` workers) for speed; each worker runs `migrate_one_table()` on its
**own** Oracle + Postgres connections.

```
resolve_load_order()           ── compute order (for logging; not required under deferred FKs)
migrate_sequences()            ── ONCE, before the pool (standalone sequences)
╔═ ThreadPoolExecutor(MAX_PARALLEL_TABLES) — migrate_one_table per table, concurrently ═╗
║ 1. dump_oracle_table              Oracle ─► oracle/<t>.sql (DDL) + oracle/<t>.csv (data) ║
║ 2. convert_oracle_sql_to_postgres oracle/<t>.sql ─► postgres/<t>.sql  (DDL only)         ║
║ 3. load_postgres_copy             CREATE from DDL, then COPY <t>.csv ─► Postgres (commit) ║
║ 4. migrate_table_constraints      PK/UNIQUE/CHECK/indexes (commit)                        ║
║ 5. migrate_identity_columns       identity columns (commit)                               ║
╚═ each table runs end-to-end on its own connections; tables finish in any order ══════════╝
migrate_foreign_keys()         ── ONCE, after the pool (deferred FKs)
```

**Speed design (current):** data is bulk-loaded with Postgres **`COPY`** from a CSV (not per-row
INSERTs — that was the v1 approach and was the big bottleneck), and tables are migrated
**concurrently** (fan-out). Both were chosen because the only goal stated was raw speed. The work
is I/O-bound, so threads give real overlap (Oracle reads ‖ Postgres writes across tables).

**Per-table atomicity:** each table is still fully finished and committed independently (its own
transaction). Two concerns are hoisted out of the pool: **sequences** (once, before) and
**foreign keys** (once, after). With deferred FKs, parallel/any-order loading is safe.

### Why FKs are deferred (key design decision)
A child's FK can't be created until its parent exists. Deferring all FKs to a final pass
guarantees referential integrity regardless of load order and **survives circular FKs**
(which are mathematically unorderable). Dependency-aware ordering (below) gets
parents-before-children for the *data load*, but FKs still land last for safety.

---

## 4. Notebook structure (cell by cell)

| Cell | Title | Key functions |
|------|-------|---------------|
| §0 | Install drivers | `%pip install python-oracledb psycopg2-binary` + `%restart_python` |
| §1 | Configuration (NO WIDGETS) | all connection vars + behaviour flags + `TABLE_NAMES` |
| §2 | Connection helpers | `get_oracle_connection`, `get_postgres_connection` |
| §3 | **STEP 1** dump (DDL + CSV) | `dump_oracle_table` (→ `(ddl_path, csv_path)`), `_build_create_table_ddl`, `_oracle_column_type`, `_csv_value`, `_oracle_output_type_handler`, `_oracle_quote_ident` |
| §4 | **STEP 2** convert | `convert_oracle_sql_to_postgres`, `convert_oracle_sql_text`, `_convert_line`, `_TYPE_RULES`, `_FUNC_RULES` |
| §5 | **STEP 3** load (COPY) | `load_postgres_copy`, `_split_sql_statements` |
| §5b | 2nd pass: constraints/indexes/FKs | `build_oracle_constraint_ddl`, `build_oracle_foreign_key_ddl`, `migrate_table_constraints`, `migrate_foreign_keys`, `_pg_ident`, `_pg_qualified`, `_constraint_columns` |
| §5c | 3rd pass: sequences/identity | `migrate_sequences`, `migrate_identity_columns` |
| §5d | Dependency-aware load ordering | `build_fk_dependency_graph`, `order_tables_by_dependency`, `resolve_load_order` |
| §6 | Orchestration (parallel) | `discover_oracle_tables`, `migrate_one_table`, `migrate_all_tables` (ThreadPoolExecutor) |
| §7 | Run | `results = migrate_all_tables()` |
| §8 | Summary | builds a Spark DataFrame of OK/FAILED per table |

---

## 5. Configuration (cell §1) — what to set before running

**Oracle (source):** `ORACLE_HOST`, `ORACLE_PORT`, `ORACLE_SERVICE_NAME` *or* `ORACLE_SID`
(set one, leave the other `None`), `ORACLE_USER`, `ORACLE_PASSWORD`, `ORACLE_SCHEMA`
(owner of the tables, usually uppercase).

**Postgres (target):** `PG_HOST`, `PG_PORT`, `PG_DATABASE`, `PG_USER`, `PG_PASSWORD`,
`PG_SCHEMA`, `PG_SSLMODE`.

> Secrets: passwords are plain vars per the spec, but there's a commented
> `dbutils.secrets.get(...)` example — prefer that in real use.

**Behaviour flags:**

| Flag | Default | Meaning |
|------|---------|---------|
| `WORK_DIR` | `/dbfs/tmp/ora2pg` | where intermediate `.sql` (DDL) + `.csv` (data) files go (`oracle/`, `postgres/` subdirs) |
| `BATCH_SIZE` | 5000 | rows fetched per batch from Oracle (`arraysize`/`fetchmany`) |
| `MAX_PARALLEL_TABLES` | 8 | **tables migrated concurrently** (thread-pool workers). Each worker uses its own Oracle + Postgres connection → keep ≤ connection limits on BOTH DBs. Set 1 = sequential |
| `DROP_TARGET_BEFORE_LOAD` | True | `DROP TABLE IF EXISTS` on target first |
| `CREATE_TARGET_TABLE` | True | run the generated `CREATE TABLE` DDL |
| `CONTINUE_ON_ERROR` | True | keep going if one table fails (failure logged + in summary) |
| `KEEP_SQL_FILES` | True | keep intermediate `.sql` for auditing |
| `MIGRATE_PK_UNIQUE_CHECK` | True | 2nd pass: PK/UNIQUE/CHECK constraints |
| `MIGRATE_INDEXES` | True | 2nd pass: non-constraint indexes |
| `MIGRATE_FOREIGN_KEYS` | True | final pass: FKs |
| `MIGRATE_SEQUENCES` | True | standalone sequences |
| `MIGRATE_IDENTITY_COLUMNS` | True | Oracle identity → Postgres identity |
| `RESPECT_LOAD_ORDER` | False | False = auto FK-dependency sort; True = use `TABLE_NAMES` order verbatim |

**Table list:** `TABLE_NAMES` = array of strings (Option A). Or set `DISCOVER_TABLES = True`
to pull every table in `ORACLE_SCHEMA` from `all_tables` (Option B).

---

## 6. How each step works (important details)

### STEP 1 — dump (`dump_oracle_table` → `(ddl_path, csv_path)`)
- Writes **two files**: `oracle/<t>.sql` (CREATE TABLE DDL, columns + NOT NULL only — no PK/FK
  inline) and `oracle/<t>.csv` (data, first row = header).
- DDL is built from **`all_tab_columns`** (`_build_create_table_ddl` / `_oracle_column_type`) for
  accurate Oracle types — decoupled from the data cursor.
- Data: streams `fetchmany(BATCH_SIZE)` rows and writes **COPY-ready CSV** via `_csv_value`:
  `None`→empty field (NULL), datetime→`YYYY-MM-DD HH:MM:SS.ffffff`, date→ISO, bytes→`\xHEX`
  (Postgres bytea), bool→`t`/`f`; numbers/strings stringified by `csv.writer` (handles
  comma/quote/newline escaping). **No SQL-literal conversion needed for data.**
- `_oracle_output_type_handler` fetches CLOB/NCLOB→str and BLOB→bytes (avoids per-row LOB round
  trips; faster).

### STEP 2 — convert (`convert_oracle_sql_to_postgres`)
- Now only converts the **DDL** file (`oracle/<t>.sql` → `postgres/<t>.sql`). Data bypasses the
  converter entirely (it's already Postgres-friendly CSV). Still **syntax-only** regex, not AST.
- Type rewrites (`_TYPE_RULES`, DDL only): `VARCHAR2→VARCHAR`, `NUMBER(p,s)→NUMERIC`,
  `DATE→TIMESTAMP`, `CLOB→TEXT`, `BLOB→BYTEA`, `RAW→BYTEA`, etc.
- Function/idiom rewrites (`_FUNC_RULES`): `NVL→COALESCE`, `SYSDATE→CURRENT_TIMESTAMP`,
  `SYS_GUID()→gen_random_uuid()`, `FROM DUAL` removal, `HEXTORAW('AA')→'\xAA'`,
  `TO_TIMESTAMP/TO_DATE(...)→TIMESTAMP/DATE '...'` (latter rules now mostly exercised only by
  the constraint/CHECK converter `convert_oracle_sql_text()`).
- **SCOPE (documented in-cell):** only ever fed the Step-1 DDL. NOT a general Oracle→Postgres
  transpiler — no `CONNECT BY`, `(+)`, `DECODE`, `MERGE`, PL/SQL. Use `ora2pg` if that changes.

### STEP 3 — load (`load_postgres_copy`)
- DROP (if `DROP_TARGET_BEFORE_LOAD`) → CREATE from the converted DDL (if `CREATE_TARGET_TABLE`,
  via the quote-aware `_split_sql_statements`) → **`COPY`** the CSV with `cursor.copy_expert`.
- Reads the CSV header to pin the `COPY (col, …)` column order, then streams the rest:
  `FORMAT csv, HEADER false, NULL ''`. Commits per table; rolls back on error.
- **This replaced per-row INSERTs** — the single biggest speed win for large tables.

### 2nd pass — constraints/indexes/FKs (§5b)
- Reads `all_constraints`, `all_cons_columns`, `all_indexes`, `all_ind_columns`.
- PK/UNIQUE/CHECK + indexes applied **per table** after its data. Skips system-generated
  `"COL" IS NOT NULL` checks (already NOT NULL in DDL) and indexes backing PK/UNIQUE (dupes).
- FKs collected for all OK tables, applied **last, each independently** (one bad FK won't abort
  the rest — logs `[fk ERROR]`). `ON DELETE CASCADE/SET NULL` preserved from `delete_rule`.

### 3rd pass — sequences/identity (§5c)
- `migrate_sequences`: `all_sequences` → `CREATE SEQUENCE IF NOT EXISTS … START WITH <high-water>`.
  **Clamps** Oracle's oversized MIN/MAXVALUE (28 nines) to `NO MIN/MAXVALUE` when outside
  Postgres bigint range. Runs once, before the loop.
- `migrate_identity_columns`: `all_tab_identity_cols` → `ALTER COLUMN … ADD GENERATED
  {ALWAYS|BY DEFAULT} AS IDENTITY` **after** data load, then `RESTART WITH MAX(col)+1` so future
  inserts don't collide. Per table.
- **Known gap (documented):** pre-12c "sequence + BEFORE INSERT trigger" auto-increment is NOT
  auto-detected (no reliable dictionary link). Standalone sequence is migrated; convert the
  column to identity / point a default at `nextval()` manually.

### Dependency-aware load ordering (§5d)
- `build_fk_dependency_graph`: FK graph from `all_constraints` (type `R`), **scoped to migrated
  tables**. Self-refs dropped; out-of-scope parents reported but ignored for ordering.
- `order_tables_by_dependency`: **Kahn's algorithm**, parents before children.
  - **Cycles**: detected, logged (`WARNING`), broken deterministically (fewest unmet deps,
    alphabetical tie-break). Those tables rely on the deferred FK pass.
  - **Deterministic**: alphabetical tie-break; same input → same order. Output always contains
    every input table exactly once.
- `resolve_load_order`: toggles on `RESPECT_LOAD_ORDER`. **Under parallel fan-out + deferred FKs,
  order doesn't affect correctness** — it's computed for logging/visibility only.
- This logic was unit-tested with a standalone harness (linear, diamond, cycle, self-ref,
  determinism, completeness) — all passed. Tests were ad-hoc (run in bash), not committed.

### Parallel fan-out (§6) — the speed architecture
- `migrate_one_table(table_name)`: full per-table pipeline on its **own** Oracle + Postgres
  connections (drivers aren't thread-safe to share). Returns `(table, ok, error, seconds)`.
- `migrate_all_tables()`: opens one setup connection pair for the bracketing passes, runs
  `migrate_sequences` once, then a `ThreadPoolExecutor(MAX_PARALLEL_TABLES)` over the tables via
  `as_completed`, then `migrate_foreign_keys` once. Threads work because the load is I/O-bound.
- `CONTINUE_ON_ERROR=False` now raises *after* the pool drains (in-flight tables still finish) —
  there's no mid-flight cancellation.
- **Why this design:** user's sole stated goal was speed. We chose fan-out + COPY over the
  two-cluster producer/consumer job (which caps ~2× and is far more complex). See git history /
  the workflow-revision discussion.

---

## 7. State of the project

**Complete & working** (validated by `python3 -c "import ast; ast.parse(...)"` + stubbed-driver
checks after every change):
- dump→convert→load flow, now **DDL `.sql` + CSV** dump and **`COPY`** load.
- **Parallel fan-out** (thread pool, `MAX_PARALLEL_TABLES`) — the speed architecture.
- Constraints, indexes, FKs, sequences, identity columns.
- Dependency-aware ordering with cycle handling (now informational under parallel/deferred-FK).
- Converter scope documented. Docker test env + one-command `make test`.

**Not done / explicitly declined by user:**
- **Two-cluster producer/consumer job** (workflow revision) — *assessed; rejected in favour of
  fan-out + COPY because the goal was speed and that design caps ~2×.*
- **Spark JDBC direct-stream** (drop SQL files) — *offered as the fastest path; user chose to keep
  the file-based flow (fan-out + COPY) instead.*
- **Row-count validation pass** — *offered once, declined (but a row-count diff exists in the
  Docker test harness, `docker/test/run_migration_test.py`).*
- A `FK_PER_TABLE` mode — *offered, not requested. Deferred-FK design left as-is.*

**Known limitations (all intentional, documented in-notebook):**
- Syntax-only converter, dumps-only scope (not arbitrary SQL/PL-SQL).
- Pre-12c trigger-based auto-increment not auto-detected.
- Composite/partial/function-based indexes and exotic CHECK expressions may need manual tweaks.
- Everything migrates into a single target `PG_SCHEMA`.
- Views, materialized views, triggers, procedures/packages are out of scope (data + relational
  structure only).

---

## 8. Conventions & how to work on this

- **Validate after every edit:** `python3 -c "import ast; ast.parse(open('oracle_to_postgres_migration.py').read())"`.
  The file is a Databricks *source* `.py`; `# MAGIC`/`# COMMAND` lines are comments, so it parses
  as plain Python even though `%pip`/`display`/`spark`/`dbutils` only exist at Databricks runtime.
- Keep the cell structure intact: markdown via `# MAGIC %md`, cell breaks `# COMMAND ----------`.
- Keep NO WIDGETS — config stays as plain variables in §1.
- Commit + push to `claude/databricks-notebook-pseudocode-vcc1cs` after a coherent change.
- The user is iterative and detail-oriented: prefers honest answers about limitations, asks
  pointed questions (e.g. "is it incremental?", "syntax-only?"), and wants robustness.

### Likely next requests (be ready)
- Tuning throughput further: raise `MAX_PARALLEL_TABLES`, add a **connection pool** (currently a
  fresh connection per table), split **huge tables** across workers (range/rowid chunks), or move
  to Spark JDBC if file-based COPY isn't fast enough.
- Row-count / checksum validation pass inside the notebook (declined once; diff exists in tests).
- Handling views/sequences-via-triggers/PL-SQL, or swapping the converter for `ora2pg`.
- Restart/resume capability (skip already-migrated tables) — pairs well with a manifest table.

---

## 9. Quick start for a fresh instance

1. `git checkout claude/databricks-notebook-pseudocode-vcc1cs`
2. Read `oracle_to_postgres_migration.py` top-to-bottom (it's heavily commented + has markdown cells).
3. To run: import into Databricks, fill in cell §1 (connections + `TABLE_NAMES`), Run All.
4. Inspect results via the §8 summary DataFrame and the `.sql` files under `WORK_DIR`.

---

## 10. Local Docker test environment

Lets you test-run the notebook against real databases without Databricks/cloud.

**Files:**
- `docker-compose.yml` (repo root) — `oracle` (`gvenzl/oracle-xe:21-slim`) + `postgres:16`,
  with healthchecks and persistent volumes.
- `docker/oracle/init/01_schema.sql` — creates `CUSTOMERS` + `ORDERS` under schema
  `ORACLE_USER`. Deliberately covers every notebook feature: identity column, standalone
  sequence, PK/UNIQUE/CHECK, an index, and FK `ORDERS → CUSTOMERS` (so dependency ordering +
  deferred-FK pass are both exercised). Objects are **fully schema-qualified** so they're owned
  by `ORACLE_USER` regardless of which privileged user the image's init runner uses.
- `docker/oracle/init/02_seed.sql` — 3 customers + 3 orders, committed.
- `docker/postgres/init/01_init.sql` — ensures `public` schema + `pgcrypto` (for
  `gen_random_uuid()`, the `SYS_GUID()` target).
- `docker/README.md` — full usage + the exact cell §1 values + verify commands + arm64 note.
- `Makefile` — `make test` / `up` / `down` / `clean` / `logs`.
- `docker/test/run_test.sh` — boots the stack, waits for healthy, makes a throwaway venv,
  runs the migration, diffs row counts, tears down (`KEEP_UP=1` to keep DBs up). Exit 0 = pass.
- `docker/test/run_migration_test.py` — **reuses the notebook's real functions** by exec'ing its
  code cells into a namespace (skips markdown/`%pip`/run/Spark-summary cells, applies localhost +
  temp-dir overrides), runs `migrate_all_tables()`, then compares `COUNT(*)` per table both sides.
  Supports `--dry` (load only, no DB). The cell-loader + converter were validated locally with
  stubbed drivers; the loader pattern is sensitive to how cells are split — see notes below.

**Use:** `make test` (one command). Or manually: `docker compose up -d`, wait for Oracle's
first-run init (a few min; `make logs`), then run the notebook with `ORACLE_HOST=localhost`,
`ORACLE_SERVICE_NAME=XEPDB1`, `ORACLE_SCHEMA=ORACLE_USER`, `TABLE_NAMES=["CUSTOMERS","ORDERS"]`
(Postgres/credentials already match the notebook defaults). Expect 3+3 rows on both sides.

**Loader caveats (for whoever maintains the test):** `run_migration_test.py` splits the notebook
on `\n# COMMAND ----------\n` and skips any cell containing `# MAGIC`, the `migrate_all_tables()`
run cell (kept only if it also has `def `), and the `spark`/`display` summary cell. If you add a
runnable cell that imports something unavailable locally, or rename those markers, update the
loader. Overrides are re-applied after every cell so `WORK_DIR` is set before the connection
cell's `makedirs` runs.

**Notes / gotchas:**
- Compose connection values are aligned with the notebook's §1 placeholder defaults
  (`oracle_user`/`oracle_password`, `postgres_user`/`postgres_password`/`target_db`).
- Oracle image is amd64; Apple Silicon → add `platform: linux/amd64` or switch to
  `gvenzl/oracle-free:23-slim` (service name becomes `FREEPDB1`).
- Databricks can't reach `localhost`; run the logic locally or expose the DBs to a routable host.
- Validate compose changes with `docker compose config -q`.
