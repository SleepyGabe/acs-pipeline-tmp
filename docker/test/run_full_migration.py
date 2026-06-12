#!/usr/bin/env python3
"""
Full end-to-end migration against the docker Oracle + Postgres containers.

Unlike run_migration_test.py (2 fixed tables) this DISCOVERS every table in the
source Oracle schema, wipes the Postgres target for a true from-scratch load,
runs the real migrate_all_tables() pipeline (sequences -> parallel per-table
dump/convert/COPY/constraints/identity -> deferred FK pass), then verifies
COUNT(*) for every discovered table on both sides.

It seeds the rich EDGE_* fixture set first (identity, composite/deferrable FKs,
CHECKs, DESC/function indexes, virtual columns, LOB/RAW, long identifiers, ...)
so the run exercises the full converter, on top of the CUSTOMERS/ORDERS seed.

Run: docker exec ora2pg-jupyter bash -lc 'cd /home/jovyan/work && python docker/test/run_full_migration.py'
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import run_migration_test as R
import run_edgecase_test as E


def pg_clean(ns):
    """Drop every table + standalone sequence in the target schema (fresh start)."""
    sch = ns["PG_SCHEMA"]
    pg = ns["get_postgres_connection"]()
    cur = pg.cursor()
    cur.execute("SELECT tablename FROM pg_tables WHERE schemaname=%s", (sch,))
    tables = [r[0] for r in cur.fetchall()]
    for t in tables:
        cur.execute(f'DROP TABLE IF EXISTS "{sch}"."{t}" CASCADE')
    cur.execute("SELECT sequencename FROM pg_sequences WHERE schemaname=%s", (sch,))
    seqs = [r[0] for r in cur.fetchall()]
    for s in seqs:
        cur.execute(f'DROP SEQUENCE IF EXISTS "{sch}"."{s}" CASCADE')
    pg.commit()
    cur.close()
    pg.close()
    print(f"  dropped {len(tables)} table(s), {len(seqs)} sequence(s) from {sch}")


def verify(ns):
    osch, psch = ns["ORACLE_SCHEMA"], ns["PG_SCHEMA"]
    ora = ns["get_oracle_connection"]()
    pg = ns["get_postgres_connection"]()
    names = ns["discover_oracle_tables"](ora)

    print("\n" + "=" * 60)
    print(f"{'TABLE':<34}{'ORACLE':>9}{'POSTGRES':>11}  RESULT")
    print("-" * 60)
    all_ok = True
    try:
        for t in names:
            oc = ora.cursor()
            oc.execute(f'SELECT COUNT(*) FROM "{osch}"."{t}"')
            o = oc.fetchone()[0]
            oc.close()
            pc = pg.cursor()
            try:
                pc.execute(f'SELECT COUNT(*) FROM "{psch}"."{t}"')
                p = pc.fetchone()[0]
            except Exception:
                p = "MISSING"
            pc.close()
            ok = (p == o)
            all_ok = all_ok and ok
            disp = t if len(t) <= 33 else t[:30] + "..."
            print(f"{disp:<34}{o:>9}{str(p):>11}  {'PASS' if ok else 'FAIL'}")
    finally:
        ora.close()
        pg.close()
    print("=" * 60)
    return all_ok, len(names)


def main():
    ov = R.build_overrides()
    ov["DISCOVER_TABLES"] = True
    ov["LOAD_MODE"] = "recreate"
    ov["RESPECT_LOAD_ORDER"] = False
    ns = R.load_notebook_namespace(ov)

    print("=== Seeding rich source fixture set into Oracle ===")
    E.setup_oracle(ns)

    print("\n=== Wiping Postgres target (true from-scratch load) ===")
    pg_clean(ns)

    print("\n=== FULL MIGRATION (discover ALL source tables) ===\n")
    results = ns["migrate_all_tables"]()

    ok, n = verify(ns)
    nfail = len(results["failed"])
    print(f"\nDiscovered/verified {n} tables. "
          f"Migration: {len(results['ok'])} ok, {nfail} failed.")
    if ok and nfail == 0:
        print("FULL MIGRATION RESULT: PASS — every table migrated and row counts match.")
        sys.exit(0)
    print("FULL MIGRATION RESULT: FAIL")
    sys.exit(1)


if __name__ == "__main__":
    main()
