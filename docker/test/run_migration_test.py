#!/usr/bin/env python3
"""
Automated end-to-end test for the Oracle -> Postgres migration notebook.

It loads the *real* functions from `oracle_to_postgres_migration.py` (no copy/paste),
points them at the local Docker databases, runs the full migration, then verifies that
every migrated table has the same row count on both sides.

Run via `docker/test/run_test.sh` (which boots the containers first), or directly once
the containers are healthy:

    ORACLE_HOST=localhost PG_HOST=localhost python docker/test/run_migration_test.py

Flags:
    --dry   Load the notebook namespace and report the functions found, but do NOT
            connect or migrate. Used to sanity-check the loader without databases.
"""
import os
import sys
import tempfile

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
NOTEBOOK = os.path.join(REPO_ROOT, "oracle_to_postgres_migration.py")

# Tables we expect to exist in the seeded source schema.
EXPECTED_TABLES = ["CUSTOMERS", "ORDERS"]


def build_overrides():
    """Connection + behaviour overrides pointing the notebook at the Docker stack."""
    tmp = tempfile.mkdtemp(prefix="ora2pg_test_")
    # Create the oracle/ and postgres/ subdirs we override below. The notebook
    # normally creates them itself (via _resolve_work_dir at config time), but it
    # does so for its OWN env-derived WORK_DIR — and we then override WORK_DIR /
    # *_DUMP_DIR to `tmp` *after* that cell has run, so nothing creates tmp/oracle
    # or tmp/postgres. Without this, the first file write (migrate_sequences ->
    # _sequences.sql) dies with FileNotFoundError.
    os.makedirs(os.path.join(tmp, "oracle"), exist_ok=True)
    os.makedirs(os.path.join(tmp, "postgres"), exist_ok=True)
    return {
        # Oracle (source)
        "ORACLE_HOST": os.environ.get("ORACLE_HOST", "localhost"),
        "ORACLE_PORT": int(os.environ.get("ORACLE_PORT", "1521")),
        "ORACLE_SERVICE_NAME": os.environ.get("ORACLE_SERVICE_NAME", "XEPDB1"),
        "ORACLE_SID": None,
        "ORACLE_USER": os.environ.get("ORACLE_USER", "oracle_user"),
        "ORACLE_PASSWORD": os.environ.get("ORACLE_PASSWORD", "oracle_password"),
        "ORACLE_SCHEMA": os.environ.get("ORACLE_SCHEMA", "ORACLE_USER"),
        # Postgres (target)
        "PG_HOST": os.environ.get("PG_HOST", "localhost"),
        "PG_PORT": int(os.environ.get("PG_PORT", "5432")),
        "PG_DATABASE": os.environ.get("PG_DATABASE", "target_db"),
        "PG_USER": os.environ.get("PG_USER", "postgres_user"),
        "PG_PASSWORD": os.environ.get("PG_PASSWORD", "postgres_password"),
        "PG_SCHEMA": os.environ.get("PG_SCHEMA", "public"),
        "PG_SSLMODE": "disable",
        # Work dirs -> a local temp dir (not /dbfs)
        "WORK_DIR": tmp,
        "ORACLE_DUMP_DIR": os.path.join(tmp, "oracle"),
        "POSTGRES_DUMP_DIR": os.path.join(tmp, "postgres"),
        # Tables / behaviour
        "TABLE_NAMES": list(EXPECTED_TABLES),
        "DISCOVER_TABLES": False,
        "RESPECT_LOAD_ORDER": False,
    }


def load_notebook_namespace(overrides):
    """
    Exec the notebook's code cells into a namespace, reusing its real functions.

    Skips: markdown / %pip / %restart_python cells, the `migrate_all_tables()` run cell,
    and the Spark summary cell. Overrides are re-applied after every cell so the config
    values (and WORK_DIR, used by makedirs in the connection cell) take effect.
    """
    with open(NOTEBOOK, encoding="utf-8") as f:
        src = f.read()

    cells = src.split("\n# COMMAND ----------\n")
    ns = {}
    for cell in cells:
        if "# MAGIC" in cell:
            continue  # markdown, %pip, %restart_python
        if "migrate_all_tables()" in cell and "def " not in cell:
            continue  # the run cell
        if "spark.createDataFrame" in cell or "display(" in cell:
            continue  # the Spark summary cell
        exec(compile(cell, NOTEBOOK, "exec"), ns)
        ns.update(overrides)
    return ns


def verify_row_counts(ns):
    """Compare COUNT(*) for each table between Oracle and Postgres."""
    tables = ns["TABLE_NAMES"]
    ora_schema = ns["ORACLE_SCHEMA"]
    pg_schema = ns["PG_SCHEMA"]

    ora = ns["get_oracle_connection"]()
    pg = ns["get_postgres_connection"]()

    print("\n" + "=" * 52)
    print(f"{'TABLE':<22}{'ORACLE':>9}{'POSTGRES':>11}  RESULT")
    print("-" * 52)
    all_ok = True
    try:
        for t in tables:
            oc = ora.cursor()
            oc.execute(f'SELECT COUNT(*) FROM "{ora_schema}"."{t}"')
            o_count = oc.fetchone()[0]
            oc.close()

            pc = pg.cursor()
            pc.execute(f'SELECT COUNT(*) FROM "{pg_schema}"."{t}"')
            p_count = pc.fetchone()[0]
            pc.close()

            match = (o_count == p_count)
            all_ok = all_ok and match
            print(f"{t:<22}{o_count:>9}{p_count:>11}  {'PASS' if match else 'FAIL'}")
    finally:
        ora.close()
        pg.close()
    print("=" * 52)
    return all_ok


def main():
    dry = "--dry" in sys.argv
    overrides = build_overrides()
    print(f"Loading notebook functions from: {NOTEBOOK}")
    ns = load_notebook_namespace(overrides)

    expected_fns = ["migrate_all_tables", "migrate_one_table", "dump_oracle_table",
                    "convert_oracle_sql_to_postgres", "load_postgres_copy",
                    "get_oracle_connection", "get_postgres_connection"]
    missing = [fn for fn in expected_fns if fn not in ns or not callable(ns[fn])]
    if missing:
        print(f"ERROR: expected functions not loaded: {missing}")
        sys.exit(2)
    print(f"Loaded OK. Key functions present: {', '.join(expected_fns)}")

    if dry:
        print("--dry: skipping migration and verification.")
        return

    print("\n>>> Running migration...\n")
    ns["migrate_all_tables"]()

    ok = verify_row_counts(ns)
    if ok:
        print("\nRESULT: PASS — all tables match.")
        sys.exit(0)
    print("\nRESULT: FAIL — row counts differ.")
    sys.exit(1)


if __name__ == "__main__":
    main()
