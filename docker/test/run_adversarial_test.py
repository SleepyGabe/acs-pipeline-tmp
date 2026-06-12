#!/usr/bin/env python3
"""
BRUTAL adversarial test for the Oracle->Postgres table-migration converter.

This is the RED TEAM. Every fixture is a valid, creatable Oracle object (table /
sequence / constraint / index / data) under schema ORACLE_USER that attacks a
specific assumption the regex/line-based converter in
`oracle_to_postgres_migration.py` makes about its Step-1-generated DDL + data.

It seeds the fixtures in the REAL Oracle container, runs the REAL
migrate_all_tables() against the docker DBs, then asserts on the REAL Postgres
side: table survived, row counts match, and values ROUND-TRIP exactly where
feasible. Where the converter is expected to FAIL or lose fidelity, the
assertion is still written (so we MEASURE it) and tagged [XFAIL-EXPECTED] in the
name. A converter failure is left as a failing assertion — NOT masked, NOT
patched.

Goes BEYOND run_edgecase_test.py (which only re-creates already-fixed cases).

Run: docker exec ora2pg-jupyter bash -lc 'cd /home/jovyan/work && python docker/test/run_adversarial_test.py'
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import run_migration_test as R


# ---------------------------------------------------------------------------
# Identifier torture helpers
# ---------------------------------------------------------------------------
# Two emoji column names identical through byte 63 (each emoji = 4 UTF-8 bytes).
# Postgres truncates at 63 bytes; _short_ident must hash them apart.
_EMOJI = "\U0001F600"          # grinning face, 4 bytes
_EMOJI_A = "C" + _EMOJI * 20 + "AAA"   # prefix collides for >63 bytes
_EMOJI_B = "C" + _EMOJI * 20 + "BBB"
# A column name with spaces + special chars (legal quoted Oracle id). NOTE: an
# embedded double-quote ("WE""IRD") is rejected by Oracle 21c XE here with
# ORA-03001, so we use a space/paren/case-mix name instead to still exercise
# the _pg_ident quoting path on a non-trivial identifier.
_DQ_COL = 'we IRD (x)'
# A 128-byte ASCII identifier (Oracle 12.2+ allows up to 128 bytes).
_LONG128 = "Z" * 128

ADV = "ADV_"   # fixture name prefix

# Every fixture table name (for discovery override + drop order).
FIXTURES = [
    "ADV_DDLEXIT",        # in_ddl ");"-in-DEFAULT premature exit bug
    "ADV_RESV",           # reserved-word table & column names
    "ADV_UNICODE_ID",     # emoji / CJK / RTL / embedded-quote identifiers
    "ADV_SELFCOL",        # a column named the same as its table
    "ADV_LONG128",        # 128-byte identifier
    "ADV_CSVHELL",        # control chars, CRLF, delimiters, NULL sentinel, bytea-look
    "ADV_UNIDATA",        # unicode data: BOM, zero-width, RTL, 4-byte, surrogate-ish
    "ADV_NUMTORT",        # NUMBER(38,38), NUMBER(1,0), 38-digit int, scientific, +/-0
    "ADV_FLOATS",         # BINARY_DOUBLE/FLOAT NaN/Inf/-Inf, FLOAT(126)
    "ADV_TEMPORAL",       # year 1 & 9999, 9-digit frac, half/quarter/+14 TZ
    "ADV_INTERVAL",       # INTERVAL YEAR TO MONTH / DAY TO SECOND
    "ADV_BIGCHAR",        # CHAR(2000) blank-pad, RAW(2000) max
    "ADV_WIDE",           # 200-column table
    "ADV_EMPTY",          # 0 rows
    "ADV_ONLYLOB",        # single LOB column table
    "ADV_DEFEXPR",        # default expressions: SYSTIMESTAMP, USER, special-char literal
    "ADV_CHECKMESS",      # deeply nested CHECK with reserved words + quotes + parens
    "ADV_UQNULL",         # multi-col UNIQUE on nullable cols, duplicate NULLs
    "ADV_PKORDER",        # composite PK whose column order differs from definition order
    "ADV_IDXOVL",         # overlapping index + unique constraint
    "ADV_ALLTYPES",       # one column of (almost) every Oracle type
    # FK cycle (both deferrable): A<->B
    "ADV_CYCA",
    "ADV_CYCB",
    # ON DELETE CASCADE / SET NULL
    "ADV_DELP",
    "ADV_DELC",
]

# Standalone sequence at the Oracle bigint-overflow boundary (legal sequence).
_SEQ_BIG = "ADV_SEQ_BIG"


def _ora_q(name):
    return '"' + name.replace('"', '""') + '"'


# ---------------------------------------------------------------------------
# Oracle DDL + seed. Parent-before-child. Built as a list of statements so we
# can create them and surface any non-creatable fixture immediately.
# ---------------------------------------------------------------------------
def oracle_ddl(S):
    Q = _ora_q
    wide_cols = ",\n".join(f'  "C{i:03d}" NUMBER(8,2)' for i in range(200))
    wide_vals_cols = ", ".join(f'"C{i:03d}"' for i in range(200))
    wide_vals = ", ".join(str(i + 0.5) for i in range(200))
    stmts = []

    # === ADV_DDLEXIT: the in_ddl ");"-in-DEFAULT premature-exit bug. ===
    # DEFAULT literal contains ');' on the FIRST column line; later columns need
    # NUMBER->NUMERIC and DATE->TIMESTAMP conversion. If in_ddl flips off, those
    # stay NUMBER/DATE (invalid PG types) -> CREATE fails -> table lost.
    stmts += [
        f'''CREATE TABLE {S}.ADV_DDLEXIT (
              "A" VARCHAR2(20) DEFAULT 'x);y',
              "B" NUMBER(10,2),
              "WHEN_DT" DATE
            )''',
        f"INSERT INTO {S}.ADV_DDLEXIT (\"B\",\"WHEN_DT\") VALUES (3.14, DATE '2024-03-15')",
    ]

    # === ADV_RESV: reserved-word table & column names ===
    stmts += [
        f'''CREATE TABLE {S}.ADV_RESV (
              "USER" NUMBER(10) PRIMARY KEY,
              "ORDER" VARCHAR2(20),
              "SELECT" NUMBER(5),
              "GROUP" VARCHAR2(20),
              "TABLE" VARCHAR2(20),
              "WHERE" VARCHAR2(20)
            )''',
        f'INSERT INTO {S}.ADV_RESV ("USER","ORDER","SELECT","GROUP","TABLE","WHERE") '
        f"VALUES (1,'ord',5,'grp','tbl','whr')",
    ]

    # === ADV_UNICODE_ID: emoji + CJK + embedded-quote identifiers ===
    stmts += [
        f'''CREATE TABLE {S}.ADV_UNICODE_ID (
              "ID" NUMBER(10) PRIMARY KEY,
              "姓名" VARCHAR2(50),
              {Q(_DQ_COL)} VARCHAR2(50),
              {Q(_EMOJI_A)} NUMBER(5),
              {Q(_EMOJI_B)} NUMBER(5)
            )''',
        f'INSERT INTO {S}.ADV_UNICODE_ID ("ID","姓名",{Q(_DQ_COL)},{Q(_EMOJI_A)},{Q(_EMOJI_B)}) '
        f"VALUES (1,'张三','q\"q',10,20)",
    ]

    # === ADV_SELFCOL: a column named the same as its table ===
    stmts += [
        f'CREATE TABLE {S}.ADV_SELFCOL ("ADV_SELFCOL" NUMBER(10) PRIMARY KEY, "V" VARCHAR2(10))',
        f'INSERT INTO {S}.ADV_SELFCOL ("ADV_SELFCOL","V") VALUES (1, \'a\')',
    ]

    # === ADV_LONG128: a 128-byte identifier (Oracle max) ===
    stmts += [
        f'CREATE TABLE {S}.ADV_LONG128 ("ID" NUMBER(10) PRIMARY KEY, {Q(_LONG128)} VARCHAR2(20))',
        f'INSERT INTO {S}.ADV_LONG128 ("ID",{Q(_LONG128)}) VALUES (1, \'v\')',
    ]

    # === ADV_CSVHELL: control chars / CRLF / delimiters / NULL sentinel / bytea-look ===
    stmts += [
        f'''CREATE TABLE {S}.ADV_CSVHELL (
              "ID" NUMBER(10) PRIMARY KEY,
              "TXT" VARCHAR2(200),
              "TAG" VARCHAR2(50)
            )''',
        # tab, newline, CR via CHR(); comma+quote+backslash; CRLF
        f"INSERT INTO {S}.ADV_CSVHELL VALUES (1, 'a'||CHR(9)||'b'||CHR(10)||CHR(13)||'c', 't1')",
        f"INSERT INTO {S}.ADV_CSVHELL VALUES (2, 'a,b,\"c\"\\d', 't2')",
        # value that LOOKS like a bytea hex literal in a TEXT column
        f"INSERT INTO {S}.ADV_CSVHELL VALUES (3, '\\x4869', 't3')",
        # leading/trailing spaces (Oracle keeps them in VARCHAR2)
        f"INSERT INTO {S}.ADV_CSVHELL VALUES (4, '  pad  ', 't4')",
        # every C0 control char 1..31 concatenated
        f"INSERT INTO {S}.ADV_CSVHELL VALUES (5, "
        + "||".join(f"CHR({i})" for i in range(1, 32)) + ", 't5')",
        # a lone single backslash
        f"INSERT INTO {S}.ADV_CSVHELL VALUES (6, '\\', 't6')",
    ]

    # === ADV_UNIDATA: hostile unicode DATA ===
    stmts += [
        f'CREATE TABLE {S}.ADV_UNIDATA ("ID" NUMBER(10) PRIMARY KEY, "TXT" VARCHAR2(400))',
        # BOM (FEFF) + zero-width space (200B) + zero-width joiner (200D)
        f"INSERT INTO {S}.ADV_UNIDATA VALUES (1, UNISTR('\\FEFFa\\200Bb\\200Dc'))",
        # RTL override (202E) + Arabic
        f"INSERT INTO {S}.ADV_UNIDATA VALUES (2, UNISTR('\\202Eabc'))",
        # 4-byte UTF-8 emoji (astral plane) via UNISTR surrogate pair
        f"INSERT INTO {S}.ADV_UNIDATA VALUES (3, UNISTR('x\\D83D\\DE00y'))",
        # combining diacritics
        f"INSERT INTO {S}.ADV_UNIDATA VALUES (4, UNISTR('e\\0301'))",
    ]

    # === ADV_NUMTORT: numeric torture ===
    stmts += [
        f'''CREATE TABLE {S}.ADV_NUMTORT (
              "ID" NUMBER(10) PRIMARY KEY,
              "TINY" NUMBER(1,0),
              "FULLSCALE" NUMBER(38,38),
              "BIGINT38" NUMBER(38,0),
              "SCI" NUMBER,
              "ZERO" NUMBER
            )''',
        f"INSERT INTO {S}.ADV_NUMTORT VALUES (1, 9, "
        f"0.12345678901234567890123456789012345678, "
        f"99999999999999999999999999999999999999, "
        f"1.23E30, -0.0)",
        f"INSERT INTO {S}.ADV_NUMTORT VALUES (2, -9, "
        f"-0.99999999999999999999999999999999999999, "
        f"-99999999999999999999999999999999999999, 0, 0)",
    ]

    # === ADV_FLOATS: NaN / Inf / -Inf / FLOAT(126) ===
    stmts += [
        f'''CREATE TABLE {S}.ADV_FLOATS (
              "ID" NUMBER(10) PRIMARY KEY,
              "BD" BINARY_DOUBLE,
              "BF" BINARY_FLOAT,
              "FL" FLOAT(126)
            )''',
        f"INSERT INTO {S}.ADV_FLOATS VALUES (1, BINARY_DOUBLE_INFINITY, BINARY_FLOAT_INFINITY, 1.5)",
        f"INSERT INTO {S}.ADV_FLOATS VALUES (2, -BINARY_DOUBLE_INFINITY, BINARY_FLOAT_NAN, 2.5)",
        f"INSERT INTO {S}.ADV_FLOATS VALUES (3, BINARY_DOUBLE_NAN, 1.25, 3.5)",
    ]

    # === ADV_TEMPORAL: year boundaries, 9-digit frac, exotic TZ offsets ===
    stmts += [
        f'''CREATE TABLE {S}.ADV_TEMPORAL (
              "ID" NUMBER(10) PRIMARY KEY,
              "EARLY_DT" DATE,
              "LATE_DT" DATE,
              "FRAC9" TIMESTAMP(9),
              "TZ_HALF" TIMESTAMP WITH TIME ZONE,
              "TZ_Q" TIMESTAMP WITH TIME ZONE,
              "TZ_14" TIMESTAMP WITH TIME ZONE
            )''',
        # year 1 (not 4712 BC — oracledb can't fetch BC) and year 9999
        f"INSERT INTO {S}.ADV_TEMPORAL VALUES (1, "
        f"DATE '0001-01-01', DATE '9999-12-31', "
        f"TIMESTAMP '2024-03-15 14:30:00.123456789', "
        f"FROM_TZ(TIMESTAMP '2024-01-15 10:30:00','+05:30'), "
        f"FROM_TZ(TIMESTAMP '2024-01-15 10:30:00','+05:45'), "
        f"FROM_TZ(TIMESTAMP '2024-01-15 10:30:00','+14:00'))",
    ]

    # === ADV_INTERVAL: INTERVAL types (no _csv_value branch -> measure) ===
    stmts += [
        f'''CREATE TABLE {S}.ADV_INTERVAL (
              "ID" NUMBER(10) PRIMARY KEY,
              "YM" INTERVAL YEAR(4) TO MONTH,
              "DS" INTERVAL DAY(4) TO SECOND(6)
            )''',
        f"INSERT INTO {S}.ADV_INTERVAL VALUES (1, INTERVAL '5-3' YEAR TO MONTH, "
        f"INTERVAL '4 05:06:07.891' DAY TO SECOND)",
    ]

    # === ADV_BIGCHAR: CHAR(2000) blank-pad + RAW(2000) max ===
    stmts += [
        f'''CREATE TABLE {S}.ADV_BIGCHAR (
              "ID" NUMBER(10) PRIMARY KEY,
              "FIXED" CHAR(2000),
              "RAWMAX" RAW(2000)
            )''',
        f"INSERT INTO {S}.ADV_BIGCHAR VALUES (1, 'hello', HEXTORAW('CAFE'))",
    ]

    # === ADV_WIDE: 200-column table ===
    stmts += [
        f'CREATE TABLE {S}.ADV_WIDE (\n{wide_cols}\n)',
        f'INSERT INTO {S}.ADV_WIDE ({wide_vals_cols}) VALUES ({wide_vals})',
    ]

    # === ADV_EMPTY: 0 rows ===
    stmts += [
        f'CREATE TABLE {S}.ADV_EMPTY ("ID" NUMBER(10) PRIMARY KEY, "V" VARCHAR2(10))',
    ]

    # === ADV_ONLYLOB: a single LOB column ===
    stmts += [
        f'CREATE TABLE {S}.ADV_ONLYLOB ("BODY" CLOB)',
        f"INSERT INTO {S}.ADV_ONLYLOB VALUES (RPAD('x', 2000, 'y'))",
        f"INSERT INTO {S}.ADV_ONLYLOB VALUES (NULL)",
    ]

    # === ADV_DEFEXPR: default expressions ===
    stmts += [
        f'''CREATE TABLE {S}.ADV_DEFEXPR (
              "ID" NUMBER(10) PRIMARY KEY,
              "MADE_TS" TIMESTAMP DEFAULT SYSTIMESTAMP,
              "WHO" VARCHAR2(40) DEFAULT USER,
              "WEIRD" VARCHAR2(40) DEFAULT 'a''b)(c',
              "N" NUMBER(5) DEFAULT 42
            )''',
        f"INSERT INTO {S}.ADV_DEFEXPR (\"ID\") VALUES (1)",
    ]

    # === ADV_CHECKMESS: deeply nested CHECK with reserved words + quotes + parens ===
    stmts += [
        f'''CREATE TABLE {S}.ADV_CHECKMESS (
              "ID" NUMBER(10) PRIMARY KEY,
              "STATUS" VARCHAR2(20),
              "AMT" NUMBER(10,2),
              CONSTRAINT CK_MESS CHECK (
                ("STATUS" IN ('OPEN','CLOSED','FROM DUAL','x)(y'))
                AND ("AMT" BETWEEN 0 AND 1000)
                AND ("STATUS" = 'OPEN' OR "AMT" > 0)
              )
            )''',
        f"INSERT INTO {S}.ADV_CHECKMESS VALUES (1, 'OPEN', 100)",
    ]

    # === ADV_UQNULL: multi-col UNIQUE on nullable cols, duplicate NULLs ===
    stmts += [
        f'''CREATE TABLE {S}.ADV_UQNULL (
              "ID" NUMBER(10) PRIMARY KEY,
              "A" NUMBER(5),
              "B" NUMBER(5),
              CONSTRAINT UQ_AB UNIQUE ("A","B")
            )''',
        f"INSERT INTO {S}.ADV_UQNULL VALUES (1, NULL, NULL)",
        f"INSERT INTO {S}.ADV_UQNULL VALUES (2, NULL, NULL)",  # dup NULLs allowed
        f"INSERT INTO {S}.ADV_UQNULL VALUES (3, 1, 2)",
    ]

    # === ADV_PKORDER: composite PK whose column order differs from definition order ===
    stmts += [
        f'''CREATE TABLE {S}.ADV_PKORDER (
              "FIRST_DEF" NUMBER(5),
              "SECOND_DEF" NUMBER(5),
              "THIRD_DEF" NUMBER(5),
              CONSTRAINT PK_ORDER PRIMARY KEY ("THIRD_DEF","FIRST_DEF")
            )''',
        f"INSERT INTO {S}.ADV_PKORDER VALUES (1, 2, 3)",
    ]

    # === ADV_IDXOVL: overlapping index + unique constraint on same columns ===
    stmts += [
        f'''CREATE TABLE {S}.ADV_IDXOVL (
              "ID" NUMBER(10) PRIMARY KEY,
              "K" NUMBER(5),
              CONSTRAINT UQ_K UNIQUE ("K")
            )''',
        f'CREATE INDEX IDX_K_EXTRA ON {S}.ADV_IDXOVL ("K","ID")',
        f"INSERT INTO {S}.ADV_IDXOVL VALUES (1, 100)",
    ]

    # === ADV_ALLTYPES: one column of (almost) every Oracle type ===
    stmts += [
        f'''CREATE TABLE {S}.ADV_ALLTYPES (
              "ID" NUMBER(10) PRIMARY KEY,
              "C_VC2" VARCHAR2(50),
              "C_NVC2" NVARCHAR2(50),
              "C_CHAR" CHAR(5),
              "C_NCHAR" NCHAR(5),
              "C_NUM" NUMBER(12,4),
              "C_INT" NUMBER(*,0),
              "C_FLOAT" FLOAT,
              "C_BD" BINARY_DOUBLE,
              "C_BF" BINARY_FLOAT,
              "C_DATE" DATE,
              "C_TS" TIMESTAMP(6),
              "C_TSTZ" TIMESTAMP WITH TIME ZONE,
              "C_TSLTZ" TIMESTAMP WITH LOCAL TIME ZONE,
              "C_CLOB" CLOB,
              "C_BLOB" BLOB,
              "C_RAW" RAW(16),
              "C_ROWID" ROWID
            )''',
        f"INSERT INTO {S}.ADV_ALLTYPES (\"ID\",\"C_VC2\",\"C_NVC2\",\"C_CHAR\",\"C_NCHAR\","
        f"\"C_NUM\",\"C_INT\",\"C_FLOAT\",\"C_BD\",\"C_BF\",\"C_DATE\",\"C_TS\",\"C_TSTZ\","
        f"\"C_TSLTZ\",\"C_CLOB\",\"C_BLOB\",\"C_RAW\") VALUES "
        f"(1,'vc','nvc','ab','cd',12.3456,7,1.5,2.5,3.5,"
        f"DATE '2024-01-02', TIMESTAMP '2024-01-02 03:04:05.678', "
        f"FROM_TZ(TIMESTAMP '2024-01-02 03:04:05','-08:00'), "
        f"TIMESTAMP '2024-01-02 03:04:05', 'clobtext', HEXTORAW('0102'), HEXTORAW('AABB'))",
    ]

    # === FK cycle (both deferrable): ADV_CYCA <-> ADV_CYCB ===
    stmts += [
        f'CREATE TABLE {S}.ADV_CYCA ("ID" NUMBER(10) PRIMARY KEY, "B_ID" NUMBER(10))',
        f'CREATE TABLE {S}.ADV_CYCB ("ID" NUMBER(10) PRIMARY KEY, "A_ID" NUMBER(10))',
        f'ALTER TABLE {S}.ADV_CYCA ADD CONSTRAINT FK_A_TO_B FOREIGN KEY ("B_ID") '
        f'REFERENCES {S}.ADV_CYCB ("ID") DEFERRABLE INITIALLY DEFERRED',
        f'ALTER TABLE {S}.ADV_CYCB ADD CONSTRAINT FK_B_TO_A FOREIGN KEY ("A_ID") '
        f'REFERENCES {S}.ADV_CYCA ("ID") DEFERRABLE INITIALLY DEFERRED',
        f"INSERT INTO {S}.ADV_CYCA VALUES (1, NULL)",
        f"INSERT INTO {S}.ADV_CYCB VALUES (1, 1)",
        f"UPDATE {S}.ADV_CYCA SET \"B_ID\" = 1 WHERE \"ID\" = 1",
    ]

    # === ON DELETE CASCADE / SET NULL ===
    stmts += [
        f'CREATE TABLE {S}.ADV_DELP ("ID" NUMBER(10) PRIMARY KEY)',
        f'''CREATE TABLE {S}.ADV_DELC (
              "C_ID" NUMBER(10) PRIMARY KEY,
              "P_CASC" NUMBER(10),
              "P_SETN" NUMBER(10),
              CONSTRAINT FK_CASC FOREIGN KEY ("P_CASC") REFERENCES {S}.ADV_DELP ("ID") ON DELETE CASCADE,
              CONSTRAINT FK_SETN FOREIGN KEY ("P_SETN") REFERENCES {S}.ADV_DELP ("ID") ON DELETE SET NULL
            )''',
        f"INSERT INTO {S}.ADV_DELP VALUES (1)",
        f"INSERT INTO {S}.ADV_DELP VALUES (2)",
        f"INSERT INTO {S}.ADV_DELC VALUES (10, 1, 2)",
    ]

    return stmts


# Drop children before parents for idempotent re-runs.
DROP_ORDER = [
    "ADV_DELC", "ADV_DELP", "ADV_CYCA", "ADV_CYCB",
    "ADV_DDLEXIT", "ADV_RESV", "ADV_UNICODE_ID", "ADV_SELFCOL", "ADV_LONG128",
    "ADV_CSVHELL", "ADV_UNIDATA", "ADV_NUMTORT", "ADV_FLOATS", "ADV_TEMPORAL",
    "ADV_INTERVAL", "ADV_BIGCHAR", "ADV_WIDE", "ADV_EMPTY", "ADV_ONLYLOB",
    "ADV_DEFEXPR", "ADV_CHECKMESS", "ADV_UQNULL", "ADV_PKORDER", "ADV_IDXOVL",
    "ADV_ALLTYPES",
]


def _drop_all(ora, schema):
    cur = ora.cursor()
    # Disable cyclic FKs first so the cycle tables drop cleanly.
    for t in DROP_ORDER:
        try:
            cur.execute(f'DROP TABLE "{schema}"."{t}" CASCADE CONSTRAINTS')
        except Exception:
            pass
    try:
        cur.execute(f'DROP SEQUENCE "{schema}"."{_SEQ_BIG}"')
    except Exception:
        pass
    cur.close()


def setup_oracle(ns):
    schema = ns["ORACLE_SCHEMA"]
    ora = ns["get_oracle_connection"]()
    try:
        _drop_all(ora, schema)
        cur = ora.cursor()
        created = 0
        for stmt in oracle_ddl(schema):
            try:
                cur.execute(stmt)
                created += 1
            except Exception as e:
                # Surface a non-creatable fixture loudly — it is a fixture bug to fix.
                gist = " ".join(stmt.split())[:90]
                print(f"  [SETUP-ERROR] {e}\n      stmt: {gist}")
                raise
        # A standalone sequence near the Oracle->bigint boundary.
        cur.execute(
            f'CREATE SEQUENCE "{schema}"."{_SEQ_BIG}" '
            f'START WITH 9223372036854775800 INCREMENT BY 1 '
            f'MAXVALUE 9999999999999999999999999999 NOCYCLE'
        )
        ora.commit()
        cur.close()
        print(f"  created {created} Oracle statement(s) + 1 sequence")
    finally:
        ora.close()


# ---------------------------------------------------------------------------
# Assertion plumbing
# ---------------------------------------------------------------------------
PASS, FAIL, XFAIL = [], [], []


def check(name, ok, detail=""):
    (PASS if ok else FAIL).append(name)
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f" - {detail}" if detail else ""))


def measure(name, ok, detail=""):
    """For cases the converter is EXPECTED to fail/lose fidelity. Records the
    observed result without counting an expected-failure as a hard FAIL."""
    if ok:
        PASS.append(name)
        print(f"  [PASS] {name}" + (f" - {detail}" if detail else ""))
    else:
        XFAIL.append(name)
        print(f"  [XFAIL] {name}" + (f" - {detail}" if detail else ""))


def verify_postgres(ns, migrated_ok):
    sch = ns["PG_SCHEMA"]
    pg = ns["get_postgres_connection"]()
    pg.autocommit = False
    cur = pg.cursor()

    def exists(table):
        cur.execute("SELECT to_regclass(%s)", (f'{sch}."{table}"',))
        return cur.fetchone()[0] is not None

    def col(table, column):
        cur.execute(
            "SELECT data_type, numeric_precision, numeric_scale, "
            "character_maximum_length, column_default "
            "FROM information_schema.columns "
            "WHERE table_schema=%s AND table_name=%s AND column_name=%s",
            (sch, table, column))
        return cur.fetchone()

    def rollback_safe(fn):
        try:
            return fn()
        except Exception as e:
            pg.rollback()
            return ("ERR", str(e))

    # ---- ADV_DDLEXIT: the headline bug ----
    print("\n--- ADV_DDLEXIT: in_ddl ');'-in-DEFAULT premature exit ---")
    ddlexit_ok = exists("ADV_DDLEXIT")
    measure("ADV_DDLEXIT table survived migration (DEFAULT 'x);y' did not break DDL)",
            ddlexit_ok,
            "table MISSING - converter flipped in_ddl off at ');' so NUMBER/DATE stayed unconverted"
            if not ddlexit_ok else "table created")
    if ddlexit_ok:
        b = col("ADV_DDLEXIT", "B")
        measure("ADV_DDLEXIT.B NUMBER(10,2) -> numeric (not left as NUMBER)",
                b and b[0] == "numeric", str(b))
        wd = col("ADV_DDLEXIT", "WHEN_DT")
        measure("ADV_DDLEXIT.WHEN_DT DATE -> timestamp (not left as DATE)",
                wd and wd[0].startswith("timestamp"), str(wd))
        cur.execute(f'SELECT "B","WHEN_DT" FROM "{sch}"."ADV_DDLEXIT"')
        row = cur.fetchone()
        check("ADV_DDLEXIT row round-trips (3.14, 2024-03-15)",
              row and str(row[0]) == "3.14" and str(row[1]).startswith("2024-03-15"), str(row))

    # ---- ADV_RESV: reserved-word identifiers ----
    print("\n--- ADV_RESV: reserved-word table & column names ---")
    check("ADV_RESV table exists", exists("ADV_RESV"))
    if exists("ADV_RESV"):
        cur.execute(f'SELECT "USER","ORDER","SELECT","GROUP","TABLE","WHERE" '
                    f'FROM "{sch}"."ADV_RESV"')
        row = cur.fetchone()
        check("ADV_RESV reserved-word columns round-trip",
              row == (1, 'ord', 5, 'grp', 'tbl', 'whr'), str(row))

    # ---- ADV_UNICODE_ID ----
    print("\n--- ADV_UNICODE_ID: emoji/CJK/embedded-quote identifiers ---")
    check("ADV_UNICODE_ID table exists", exists("ADV_UNICODE_ID"))
    if exists("ADV_UNICODE_ID"):
        cur.execute("SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema=%s AND table_name='ADV_UNICODE_ID'", (sch,))
        names = [r[0] for r in cur.fetchall()]
        check("ADV_UNICODE_ID has 5 distinct <=63-byte columns",
              len(names) == 5 and len(set(names)) == 5
              and all(len(n.encode()) <= 63 for n in names), str(names))
        cur.execute(f'SELECT count(*) FROM "{sch}"."ADV_UNICODE_ID"')
        check("ADV_UNICODE_ID row loaded", cur.fetchone()[0] == 1)
        # the CJK column value round-trips
        cur.execute(f'SELECT "姓名" FROM "{sch}"."ADV_UNICODE_ID" WHERE "ID"=1')
        v = rollback_safe(lambda: cur.fetchone())
        check("ADV_UNICODE_ID CJK column value round-trips (张三)",
              isinstance(v, tuple) and v and v[0] == '张三', repr(v))

    # ---- ADV_SELFCOL ----
    print("\n--- ADV_SELFCOL: column named same as table ---")
    check("ADV_SELFCOL table exists", exists("ADV_SELFCOL"))
    if exists("ADV_SELFCOL"):
        cur.execute(f'SELECT "ADV_SELFCOL","V" FROM "{sch}"."ADV_SELFCOL"')
        check("ADV_SELFCOL self-named column round-trips", cur.fetchone() == (1, 'a'))

    # ---- ADV_LONG128 ----
    print("\n--- ADV_LONG128: 128-byte identifier ---")
    check("ADV_LONG128 table exists", exists("ADV_LONG128"))
    if exists("ADV_LONG128"):
        cur.execute("SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema=%s AND table_name='ADV_LONG128' "
                    "AND column_name<>'ID'", (sch,))
        nm = cur.fetchone()
        check("ADV_LONG128 long column shortened to <=63 bytes",
              nm and len(nm[0].encode()) <= 63, nm and nm[0])
        cur.execute(f'SELECT count(*) FROM "{sch}"."ADV_LONG128"')
        check("ADV_LONG128 row loaded", cur.fetchone()[0] == 1)

    # ---- ADV_CSVHELL ----
    print("\n--- ADV_CSVHELL: control chars / delimiters / bytea-look / NULL sentinel ---")
    if exists("ADV_CSVHELL"):
        cur.execute(f'SELECT count(*) FROM "{sch}"."ADV_CSVHELL"')
        check("ADV_CSVHELL all 6 rows loaded (COPY not aborted)", cur.fetchone()[0] == 6)
        cur.execute(f'SELECT "TXT" FROM "{sch}"."ADV_CSVHELL" WHERE "ID"=1')
        v = cur.fetchone()[0]
        check("ADV_CSVHELL tab/newline/CR round-trip",
              v == 'a\tb\n\rc', repr(v))
        cur.execute(f'SELECT "TXT" FROM "{sch}"."ADV_CSVHELL" WHERE "ID"=2')
        v = cur.fetchone()[0]
        check("ADV_CSVHELL comma/quote/backslash round-trip",
              v == 'a,b,"c"\\d', repr(v))
        cur.execute(f'SELECT "TXT" FROM "{sch}"."ADV_CSVHELL" WHERE "ID"=3')
        v = cur.fetchone()[0]
        check("ADV_CSVHELL bytea-look '\\x4869' stays literal text",
              v == '\\x4869', repr(v))
        cur.execute(f'SELECT "TXT" FROM "{sch}"."ADV_CSVHELL" WHERE "ID"=4')
        v = cur.fetchone()[0]
        check("ADV_CSVHELL leading/trailing spaces preserved",
              v == '  pad  ', repr(v))
        cur.execute(f'SELECT "TXT" FROM "{sch}"."ADV_CSVHELL" WHERE "ID"=5')
        v = cur.fetchone()[0]
        expected_ctl = "".join(chr(i) for i in range(1, 32) if i != 0)
        measure("ADV_CSVHELL all C0 control chars (1..31) round-trip",
                v == expected_ctl, f"got {len(v)} chars, expected {len(expected_ctl)}")
        cur.execute(f'SELECT "TXT" FROM "{sch}"."ADV_CSVHELL" WHERE "ID"=6')
        v = cur.fetchone()[0]
        check("ADV_CSVHELL lone backslash round-trips", v == '\\', repr(v))
    else:
        check("ADV_CSVHELL table exists", False, "missing")

    # ---- ADV_UNIDATA ----
    print("\n--- ADV_UNIDATA: hostile unicode data ---")
    if exists("ADV_UNIDATA"):
        cur.execute(f'SELECT count(*) FROM "{sch}"."ADV_UNIDATA"')
        check("ADV_UNIDATA all 4 rows loaded", cur.fetchone()[0] == 4)
        cur.execute(f'SELECT "TXT" FROM "{sch}"."ADV_UNIDATA" WHERE "ID"=1')
        v = cur.fetchone()[0]
        check("ADV_UNIDATA BOM+zero-width chars round-trip",
              v == '﻿a​b‍c', repr(v))
        cur.execute(f'SELECT "TXT" FROM "{sch}"."ADV_UNIDATA" WHERE "ID"=3')
        v = cur.fetchone()[0]
        check("ADV_UNIDATA 4-byte astral emoji round-trips",
              v == 'x\U0001F600y', repr(v))
    else:
        check("ADV_UNIDATA table exists", False, "missing")

    # ---- ADV_NUMTORT ----
    # FINDING: the whole table fails to LOAD. oracledb returns a NUMBER(38,38)
    # value (e.g. -0.9999...e38) as a Python float, which rounds to -1.0; the
    # converter maps the column to NUMERIC(38,38) (max magnitude < 1), so COPY
    # aborts with "numeric field overflow" and the entire table is lost. A single
    # full-scale NUMBER column thus takes down every other (perfectly fine)
    # column in the same table. Measured, not masked.
    print("\n--- ADV_NUMTORT: numeric torture ---")
    numtort_loaded = exists("ADV_NUMTORT")
    measure("ADV_NUMTORT table survived (NUMBER(38,38) didn't overflow the load)",
            numtort_loaded,
            "table MISSING - oracledb float-rounds NUMBER(38,38) to -1.0 which "
            "overflows NUMERIC(38,38); COPY aborts the whole table"
            if not numtort_loaded else "loaded")
    if numtort_loaded:
        tiny = col("ADV_NUMTORT", "TINY")
        check("ADV_NUMTORT TINY NUMBER(1,0) -> numeric(1)",
              tiny and tiny[0] == "numeric" and tiny[1] == 1, str(tiny))
        fs = col("ADV_NUMTORT", "FULLSCALE")
        check("ADV_NUMTORT FULLSCALE NUMBER(38,38) -> numeric(38,38)",
              fs and fs[0] == "numeric" and fs[1] == 38 and fs[2] == 38, str(fs))
        cur.execute(f'SELECT "FULLSCALE" FROM "{sch}"."ADV_NUMTORT" WHERE "ID"=1')
        v = cur.fetchone()[0]
        full = "0.12345678901234567890123456789012345678"
        measure("ADV_NUMTORT NUMBER(38,38) FULL 38-digit precision preserved",
                str(v) == full, f"stored {v} (driver float-truncated)")
        cur.execute(f'SELECT "BIGINT38" FROM "{sch}"."ADV_NUMTORT" WHERE "ID"=1')
        v = cur.fetchone()[0]
        check("ADV_NUMTORT 38-digit integer round-trips exactly",
              str(v) == "9" * 38, str(v))
        cur.execute(f'SELECT "SCI" FROM "{sch}"."ADV_NUMTORT" WHERE "ID"=1')
        v = cur.fetchone()[0]
        check("ADV_NUMTORT scientific 1.23E30 round-trips",
              v == int("123" + "0" * 28), str(v))

    # ---- ADV_FLOATS ----
    print("\n--- ADV_FLOATS: NaN / Inf / -Inf ---")
    if exists("ADV_FLOATS"):
        cur.execute(f'SELECT "BD" FROM "{sch}"."ADV_FLOATS" WHERE "ID"=1')
        check("ADV_FLOATS +Inf round-trips", str(cur.fetchone()[0]) == "inf")
        cur.execute(f'SELECT "BD" FROM "{sch}"."ADV_FLOATS" WHERE "ID"=2')
        check("ADV_FLOATS -Inf round-trips", str(cur.fetchone()[0]) == "-inf")
        cur.execute(f'SELECT "BD" FROM "{sch}"."ADV_FLOATS" WHERE "ID"=3')
        check("ADV_FLOATS NaN round-trips", str(cur.fetchone()[0]) == "nan")
        bd = col("ADV_FLOATS", "BD")
        check("ADV_FLOATS BINARY_DOUBLE -> double precision",
              bd and bd[0] == "double precision", str(bd))
    else:
        check("ADV_FLOATS table exists", False, "missing")

    # ---- ADV_TEMPORAL ----
    print("\n--- ADV_TEMPORAL: year bounds, 9-digit frac, exotic TZ ---")
    if exists("ADV_TEMPORAL"):
        cur.execute("SET TIME ZONE 'UTC'")
        cur.execute(f'SELECT to_char("EARLY_DT",\'YYYY-MM-DD\'), '
                    f'to_char("LATE_DT",\'YYYY-MM-DD\') '
                    f'FROM "{sch}"."ADV_TEMPORAL" WHERE "ID"=1')
        early, late = cur.fetchone()
        # FINDING: _csv_value formats dates with datetime.strftime("%Y-%m-%d"),
        # which on glibc does NOT zero-pad years < 1000 -> "1-01-01". Postgres
        # then parses that as year 2001 (2-digit-year heuristic). Dates before
        # year 1000 are SILENTLY CORRUPTED. Measured, not masked.
        measure("ADV_TEMPORAL year 0001 DATE round-trips (no <1000 corruption)",
                early == "0001-01-01",
                f"stored {early!r} - strftime('%Y') doesn't zero-pad year<1000, "
                f"Postgres reads '1-01-01' as 2001")
        check("ADV_TEMPORAL year 9999 DATE round-trips", late == "9999-12-31", late)
        cur.execute(f'SELECT "FRAC9" FROM "{sch}"."ADV_TEMPORAL" WHERE "ID"=1')
        v = str(cur.fetchone()[0])
        measure("ADV_TEMPORAL TIMESTAMP(9) keeps all 9 fractional digits",
                v == "2024-03-15 14:30:00.123456789",
                f"stored {v} (oracledb truncates to microseconds before the converter sees it)")
        # FINDING: oracledb thin mode returns TIMESTAMP WITH TIME ZONE columns as
        # NAIVE datetimes (tzinfo=None) — the zone/offset is dropped at fetch — so
        # _csv_value emits no offset and Postgres reinterprets the wall-clock time
        # in the session TZ. The INSTANT is NOT preserved for any non-UTC offset
        # (here every value stays 10:30 instead of shifting). Note this is the
        # WITH TIME ZONE path; WITH LOCAL TIME ZONE happens to survive because it
        # is normalised to the session zone. Measured, not masked.
        cur.execute(f"SELECT to_char(\"TZ_HALF\" AT TIME ZONE 'UTC','HH24:MI'), "
                    f"to_char(\"TZ_Q\" AT TIME ZONE 'UTC','HH24:MI'), "
                    f"to_char(\"TZ_14\" AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI') "
                    f'FROM "{sch}"."ADV_TEMPORAL" WHERE "ID"=1')
        h, q, t14 = cur.fetchone()
        measure("ADV_TEMPORAL +05:30 instant preserved (expect 05:00 UTC)",
                h == "05:00", f"got {h} UTC - TSTZ offset dropped at fetch")
        measure("ADV_TEMPORAL +05:45 instant preserved (expect 04:45 UTC)",
                q == "04:45", f"got {q} UTC - TSTZ offset dropped at fetch")
        measure("ADV_TEMPORAL +14:00 instant preserved (expect prev-day 20:30 UTC)",
                t14 == "2024-01-14 20:30", f"got {t14} UTC - TSTZ offset dropped at fetch")
    else:
        check("ADV_TEMPORAL table exists", False, "missing")

    # ---- ADV_INTERVAL ----
    print("\n--- ADV_INTERVAL: interval types (no _csv_value branch) ---")
    if exists("ADV_INTERVAL"):
        ym = col("ADV_INTERVAL", "YM")
        ds = col("ADV_INTERVAL", "DS")
        # Type maps via fallback -> VARCHAR(4000) -> character varying
        measure("ADV_INTERVAL YEAR-TO-MONTH mapped to a real PG interval type",
                ym and ym[0] == "interval",
                f"mapped to {ym and ym[0]} (fallback VARCHAR, not interval)")
        cur.execute(f'SELECT count(*) FROM "{sch}"."ADV_INTERVAL"')
        check("ADV_INTERVAL row loaded (whatever the type)", cur.fetchone()[0] == 1)
        cur.execute(f'SELECT "YM"::text, "DS"::text FROM "{sch}"."ADV_INTERVAL"')
        ymv, dsv = cur.fetchone()
        print(f"      [info] interval stored as: YM={ymv!r} DS={dsv!r}")
    else:
        check("ADV_INTERVAL table exists", False, "missing")

    # ---- ADV_BIGCHAR ----
    print("\n--- ADV_BIGCHAR: CHAR(2000) blank-pad + RAW(2000) ---")
    if exists("ADV_BIGCHAR"):
        # PG length() strips trailing blanks on char(n), so use octet_length to
        # verify the value is genuinely stored blank-padded to 2000 like Oracle.
        cur.execute(f'SELECT octet_length("FIXED"), trim("FIXED"), "RAWMAX" '
                    f'FROM "{sch}"."ADV_BIGCHAR"')
        olen, trimmed, raw = cur.fetchone()
        ctype = col("ADV_BIGCHAR", "FIXED")
        check("ADV_BIGCHAR FIXED maps to character(2000)",
              ctype and ctype[0] == "character" and ctype[3] == 2000, str(ctype))
        check("ADV_BIGCHAR CHAR(2000) value blank-padded to 2000 octets",
              olen == 2000, f"octet_length={olen}")
        check("ADV_BIGCHAR CHAR content round-trips ('hello')",
              trimmed == "hello", repr(trimmed))
        check("ADV_BIGCHAR RAW->bytea round-trips (CAFE)",
              bytes(raw) == bytes.fromhex("CAFE"), repr(bytes(raw)))
    else:
        check("ADV_BIGCHAR table exists", False, "missing")

    # ---- ADV_WIDE ----
    print("\n--- ADV_WIDE: 200 columns ---")
    if exists("ADV_WIDE"):
        cur.execute("SELECT count(*) FROM information_schema.columns "
                    "WHERE table_schema=%s AND table_name='ADV_WIDE'", (sch,))
        check("ADV_WIDE has all 200 columns", cur.fetchone()[0] == 200)
        cur.execute(f'SELECT "C199" FROM "{sch}"."ADV_WIDE"')
        check("ADV_WIDE last column value round-trips (199.5)",
              str(cur.fetchone()[0]) == "199.50", None)
    else:
        check("ADV_WIDE table exists", False, "missing")

    # ---- ADV_EMPTY ----
    print("\n--- ADV_EMPTY: 0 rows ---")
    if exists("ADV_EMPTY"):
        cur.execute(f'SELECT count(*) FROM "{sch}"."ADV_EMPTY"')
        check("ADV_EMPTY exists with 0 rows", cur.fetchone()[0] == 0)
        cur.execute("SELECT count(*) FROM information_schema.table_constraints "
                    "WHERE table_schema=%s AND table_name='ADV_EMPTY' "
                    "AND constraint_type='PRIMARY KEY'", (sch,))
        check("ADV_EMPTY PK still applied on empty table", cur.fetchone()[0] == 1)
    else:
        check("ADV_EMPTY table exists", False, "missing")

    # ---- ADV_ONLYLOB ----
    print("\n--- ADV_ONLYLOB: single LOB column ---")
    if exists("ADV_ONLYLOB"):
        cur.execute(f'SELECT count(*) FROM "{sch}"."ADV_ONLYLOB"')
        check("ADV_ONLYLOB 2 rows loaded", cur.fetchone()[0] == 2)
        cur.execute(f'SELECT count(*) FROM "{sch}"."ADV_ONLYLOB" WHERE "BODY" IS NULL')
        check("ADV_ONLYLOB single-column NULL stays NULL (FORCE_NULL)",
              cur.fetchone()[0] == 1)
        cur.execute(f'SELECT length("BODY") FROM "{sch}"."ADV_ONLYLOB" '
                    f'WHERE "BODY" IS NOT NULL')
        check("ADV_ONLYLOB 2000-char CLOB round-trips", cur.fetchone()[0] == 2000)
    else:
        check("ADV_ONLYLOB table exists", False, "missing")

    # ---- ADV_DEFEXPR ----
    print("\n--- ADV_DEFEXPR: default expressions ---")
    if exists("ADV_DEFEXPR"):
        weird = col("ADV_DEFEXPR", "WEIRD")
        check("ADV_DEFEXPR special-char literal default preserved ('a''b)(c')",
              weird and weird[4] and "a''b)(c" in weird[4], str(weird and weird[4]))
        made = col("ADV_DEFEXPR", "MADE_TS")
        check("ADV_DEFEXPR SYSTIMESTAMP default -> CURRENT_TIMESTAMP",
              made and made[4] and "CURRENT_TIMESTAMP" in made[4].upper(),
              str(made and made[4]))
        n = col("ADV_DEFEXPR", "N")
        check("ADV_DEFEXPR numeric default 42 preserved",
              n and n[4] and "42" in n[4], str(n and n[4]))
        # fresh insert applies the special-char default
        def fresh():
            cur.execute(f'INSERT INTO "{sch}"."ADV_DEFEXPR"("ID") VALUES (2)')
            pg.commit()
            cur.execute(f'SELECT "WEIRD" FROM "{sch}"."ADV_DEFEXPR" WHERE "ID"=2')
            return cur.fetchone()[0]
        v = rollback_safe(fresh)
        check("ADV_DEFEXPR fresh insert applies special-char default",
              v == "a'b)(c", repr(v))
    else:
        check("ADV_DEFEXPR table exists", False, "missing")

    # ---- ADV_CHECKMESS ----
    print("\n--- ADV_CHECKMESS: deeply nested CHECK ---")
    if exists("ADV_CHECKMESS"):
        cur.execute("SELECT count(*) FROM information_schema.check_constraints "
                    "WHERE constraint_schema=%s AND constraint_name='CK_MESS'", (sch,))
        has_ck = cur.fetchone()[0] >= 1
        check("ADV_CHECKMESS nested CHECK migrated", has_ck)
        if has_ck:
            def reject_bad():
                cur.execute(f'INSERT INTO "{sch}"."ADV_CHECKMESS" VALUES (2, \'BOGUS\', 50)')
                pg.commit()
                return "accepted"
            r = rollback_safe(reject_bad)
            check("ADV_CHECKMESS CHECK rejects status not in IN-list",
                  r != "accepted", str(r))
            def accept_litparen():
                cur.execute(f'INSERT INTO "{sch}"."ADV_CHECKMESS" VALUES (3, \'x)(y\', 50)')
                pg.commit()
                return "accepted"
            r = rollback_safe(accept_litparen)
            check("ADV_CHECKMESS CHECK accepts literal 'x)(y' (paren-in-literal preserved)",
                  r == "accepted", str(r))
    else:
        check("ADV_CHECKMESS table exists", False, "missing")

    # ---- ADV_UQNULL ----
    print("\n--- ADV_UQNULL: multi-col UNIQUE on nullable cols ---")
    if exists("ADV_UQNULL"):
        cur.execute(f'SELECT count(*) FROM "{sch}"."ADV_UQNULL"')
        check("ADV_UQNULL all 3 rows loaded (duplicate NULLs allowed)",
              cur.fetchone()[0] == 3)
        cur.execute("SELECT count(*) FROM information_schema.table_constraints "
                    "WHERE table_schema=%s AND table_name='ADV_UQNULL' "
                    "AND constraint_type='UNIQUE'", (sch,))
        check("ADV_UQNULL UNIQUE constraint present", cur.fetchone()[0] >= 1)
        def reject_dup():
            cur.execute(f'INSERT INTO "{sch}"."ADV_UQNULL" VALUES (4, 1, 2)')
            pg.commit()
            return "accepted"
        r = rollback_safe(reject_dup)
        check("ADV_UQNULL UNIQUE rejects duplicate non-null (1,2)",
              r != "accepted", str(r))
    else:
        check("ADV_UQNULL table exists", False, "missing")

    # ---- ADV_PKORDER ----
    print("\n--- ADV_PKORDER: composite PK out of definition order ---")
    if exists("ADV_PKORDER"):
        cur.execute("""
            SELECT a.attname
            FROM pg_constraint c
            JOIN pg_class t ON t.oid=c.conrelid
            JOIN pg_namespace n ON n.oid=t.relnamespace
            JOIN unnest(c.conkey) WITH ORDINALITY AS k(attnum, ord) ON true
            JOIN pg_attribute a ON a.attrelid=t.oid AND a.attnum=k.attnum
            WHERE n.nspname=%s AND t.relname='ADV_PKORDER' AND c.contype='p'
            ORDER BY k.ord
        """, (sch,))
        pkcols = [r[0] for r in cur.fetchall()]
        check("ADV_PKORDER PK column order preserved (THIRD_DEF, FIRST_DEF)",
              pkcols == ["THIRD_DEF", "FIRST_DEF"], str(pkcols))
    else:
        check("ADV_PKORDER table exists", False, "missing")

    # ---- ADV_IDXOVL ----
    print("\n--- ADV_IDXOVL: overlapping index + unique constraint ---")
    if exists("ADV_IDXOVL"):
        cur.execute("SELECT count(*) FROM pg_indexes "
                    "WHERE schemaname=%s AND tablename='ADV_IDXOVL' "
                    "AND indexname='IDX_K_EXTRA'", (sch,))
        check("ADV_IDXOVL extra composite index created", cur.fetchone()[0] == 1)
        cur.execute("SELECT count(*) FROM information_schema.table_constraints "
                    "WHERE table_schema=%s AND table_name='ADV_IDXOVL' "
                    "AND constraint_type='UNIQUE'", (sch,))
        check("ADV_IDXOVL UNIQUE constraint also present", cur.fetchone()[0] >= 1)
    else:
        check("ADV_IDXOVL table exists", False, "missing")

    # ---- ADV_ALLTYPES ----
    print("\n--- ADV_ALLTYPES: one column of (almost) every type ---")
    if exists("ADV_ALLTYPES"):
        cur.execute(f'SELECT count(*) FROM "{sch}"."ADV_ALLTYPES"')
        check("ADV_ALLTYPES row loaded", cur.fetchone()[0] == 1)
        typemap = {
            "C_VC2": "character varying", "C_CHAR": "character",
            "C_NUM": "numeric", "C_BD": "double precision",
            "C_BF": "real", "C_DATE": "timestamp without time zone",
            "C_TSTZ": "timestamp with time zone", "C_CLOB": "text",
            "C_BLOB": "bytea", "C_RAW": "bytea",
        }
        for c, expected in typemap.items():
            got = col("ADV_ALLTYPES", c)
            check(f"ADV_ALLTYPES {c} -> {expected}",
                  got and got[0] == expected, str(got and got[0]))
    else:
        check("ADV_ALLTYPES table exists", False, "missing")

    # ---- FK cycle ----
    print("\n--- ADV_CYC*: deferrable FK cycle A<->B ---")
    if exists("ADV_CYCA") and exists("ADV_CYCB"):
        cur.execute("SELECT count(*) FROM pg_constraint "
                    "WHERE conname IN ('FK_A_TO_B','FK_B_TO_A') AND condeferrable")
        check("ADV_CYC both cyclic FKs created & deferrable", cur.fetchone()[0] == 2)
        cur.execute(f'SELECT count(*) FROM "{sch}"."ADV_CYCA"')
        ca = cur.fetchone()[0]
        cur.execute(f'SELECT count(*) FROM "{sch}"."ADV_CYCB"')
        cb = cur.fetchone()[0]
        check("ADV_CYC both rows loaded with mutual references", ca == 1 and cb == 1)
    else:
        check("ADV_CYC tables exist", False, "missing")

    # ---- ON DELETE CASCADE / SET NULL ----
    print("\n--- ADV_DEL*: ON DELETE CASCADE / SET NULL ---")
    if exists("ADV_DELC"):
        cur.execute("SELECT conname, confdeltype FROM pg_constraint "
                    "WHERE conname IN ('FK_CASC','FK_SETN') ORDER BY conname")
        rules = dict(cur.fetchall())
        check("ADV_DEL FK_CASC has ON DELETE CASCADE (c)",
              rules.get("FK_CASC") == "c", str(rules))
        check("ADV_DEL FK_SETN has ON DELETE SET NULL (n)",
              rules.get("FK_SETN") == "n", str(rules))
        # functional: delete parent 1 cascades the casc child; parent 2 nulls setn
        def del_test():
            cur.execute(f'DELETE FROM "{sch}"."ADV_DELP" WHERE "ID"=1')
            pg.commit()
            cur.execute(f'SELECT count(*) FROM "{sch}"."ADV_DELC" WHERE "C_ID"=10')
            return cur.fetchone()[0]
        r = rollback_safe(del_test)
        check("ADV_DEL ON DELETE CASCADE removes child row", r == 0, str(r))
    else:
        check("ADV_DELC table exists", False, "missing")

    # ---- ADV_SEQ_BIG ----
    print("\n--- ADV_SEQ_BIG: sequence near bigint overflow ---")
    cur.execute("SELECT count(*) FROM pg_sequences WHERE schemaname=%s "
                "AND sequencename=%s", (sch, _SEQ_BIG))
    check("ADV_SEQ_BIG sequence migrated (clamped, not crashed)",
          cur.fetchone()[0] == 1)

    cur.close()
    pg.close()


def main():
    overrides = R.build_overrides()
    overrides["TABLE_NAMES"] = list(FIXTURES)
    overrides["DISCOVER_TABLES"] = False
    overrides["RESPECT_LOAD_ORDER"] = False
    overrides["LOAD_MODE"] = "recreate"
    ns = R.load_notebook_namespace(overrides)

    print("=== Setting up Oracle adversarial fixtures ===")
    setup_oracle(ns)

    # Pre-clean Postgres of the fixture tables so a prior run can't mask a
    # "table missing" finding this run.
    pg = ns["get_postgres_connection"]()
    pgc = pg.cursor()
    for t in DROP_ORDER:
        pgc.execute(f'DROP TABLE IF EXISTS "{ns["PG_SCHEMA"]}"."{t}" CASCADE')
    pgc.execute(f'DROP SEQUENCE IF EXISTS "{ns["PG_SCHEMA"]}"."{_SEQ_BIG}" CASCADE')
    pg.commit()
    pgc.close()
    pg.close()

    print("\n=== Running REAL migration over adversarial fixtures ===")
    results = ns["migrate_all_tables"]()
    migrated_ok = set(results["ok"])

    print("\n=== Verifying Postgres side ===")
    verify_postgres(ns, migrated_ok)

    print("\n" + "=" * 60)
    print(f"ADVERSARIAL RESULT: {len(PASS)} passed, {len(FAIL)} failed, "
          f"{len(XFAIL)} expected-failures (fidelity/known limits)")
    if XFAIL:
        print("EXPECTED-FAILURES (measured, not fatal): " + ", ".join(XFAIL))
    if FAIL:
        print("HARD FAILURES (converter robustness gaps): " + ", ".join(FAIL))
        sys.exit(1)
    print("No hard failures. (Review expected-failures above for fidelity limits.)")


if __name__ == "__main__":
    main()
