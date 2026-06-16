#!/usr/bin/env python3
"""
Regression guard for the `SET search_path` schema-quoting bug.

THE INCIDENT: with PG_SCHEMA="acs-migration", every one of the 471 tables failed at
load time with:

    syntax error at or near "-"
    LINE 2: SET search_path TO acs-migration
                                  ^

because the schema name was interpolated into the SQL *raw* and a dash is not legal
in a bare (unquoted) SQL identifier. The same breaks on a leading digit, an embedded
space/dot, mixed case you need preserved, or a reserved word.

THE FIX: all four emission sites now route through `_pg_search_path()`, which quotes
PG_SCHEMA via `_pg_ident` — the same quoting every `_pg_qualified` table/sequence
reference already used.

This test needs NO database and NO Docker — it exec's the pure helper functions out
of the canonical .py and checks their output, plus a structural scan that fails if
anyone ever reintroduces a raw, unquoted schema interpolation. Run:

    python3 docker/test/run_search_path_test.py
"""
import hashlib
import os
import re
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SOURCE = os.path.join(REPO_ROOT, "oracle_to_postgres_migration.py")

PASS, FAIL = [], []


def check(name, ok, detail=""):
    (PASS if ok else FAIL).append(name)
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f" — {detail}" if detail else ""))


def _load_pure_helpers(src):
    """Exec just the pure identifier helpers (no DB, no module side effects)."""
    ns = {"hashlib": hashlib}
    for fn in ("_short_ident", "_pg_ident", "_pg_qualified", "_pg_search_path"):
        m = re.search(rf"^def {re.escape(fn)}\(.*?(?=\n(?:def |\S))", src, re.S | re.M)
        if not m:
            raise AssertionError(f"could not locate def {fn} in {SOURCE}")
        exec(compile(m.group(0), SOURCE, "exec"), ns)
    return ns


def main():
    with open(SOURCE, encoding="utf-8") as f:
        src = f.read()
    ns = _load_pure_helpers(src)

    # --- 1. The emitted search_path line is correctly quoted for every nasty schema.
    cases = [
        ("acs-migration", 'SET search_path TO "acs-migration";'),  # the dash that bit us
        ("public",        'SET search_path TO "public";'),         # ordinary bare name
        ("2024_data",     'SET search_path TO "2024_data";'),      # leading digit
        ("my schema",     'SET search_path TO "my schema";'),      # embedded space
        ("MixedCase",     'SET search_path TO "MixedCase";'),      # case must be preserved
        ("user",          'SET search_path TO "user";'),           # reserved word
        ('a"b',           'SET search_path TO "a""b";'),           # embedded quote -> doubled
    ]
    for schema, expect in cases:
        ns["PG_SCHEMA"] = schema
        got = ns["_pg_search_path"]()
        check(f"search_path quoted for {schema!r}", got == expect, f"got {got!r}")

    # --- 2. _pg_qualified quotes the schema half too (so DROP/COPY/TRUNCATE/etc. agree).
    ns["PG_SCHEMA"] = "acs-migration"
    qual = ns["_pg_qualified"]("AGENT_BANK")
    check("qualified name quotes the dashed schema",
          qual == '"acs-migration"."AGENT_BANK"', f"got {qual!r}")

    # --- 3. Structural guard: no raw, unquoted schema interpolation may EVER come back.
    # Match only actual f-string interpolation — `SET search_path TO {...}` — so prose
    # in docstrings (`... TO <schema>`, `... TO acs-migration`) is ignored. Every such
    # interpolation MUST quote through `_pg_ident(`, i.e. be the `_pg_search_path` helper.
    interpolations = re.findall(r"SET search_path TO \{[^}]*\}", src)
    raw = [o for o in interpolations if "_pg_ident(" not in o]
    check("every `SET search_path TO {...}` interpolation quotes via _pg_ident",
          not raw and interpolations, f"offending: {raw}; total found: {len(interpolations)}")
    # The exact raw pattern that caused the incident must never reappear.
    check("no `SET search_path TO {PG_SCHEMA}` raw f-string pattern in source",
          "SET search_path TO {PG_SCHEMA}" not in src)

    print("\n" + "=" * 52)
    print(f"SEARCH-PATH RESULT: {len(PASS)} passed, {len(FAIL)} failed")
    if FAIL:
        print("FAILURES: " + ", ".join(FAIL))
        sys.exit(1)
    print("ALL SEARCH-PATH ASSERTIONS PASSED")


if __name__ == "__main__":
    main()
