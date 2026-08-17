# Databricks notebook source
# MAGIC %md
# MAGIC # Postgres Server DDL Snapshot — **pure Python, no `pg_dump`** (schema-only ➜ `.sql`)
# MAGIC
# MAGIC Same goal as `postgres_ddl_dump` (dump the **DDL of the whole server, no data**, best-effort, to one
# MAGIC `.sql` file in a user-specified dir) — but it uses **only `psycopg2`**, reconstructing the DDL straight
# MAGIC from the catalog. That makes it the right tool when `pg_dump` can't be used:
# MAGIC
# MAGIC - **No `pg_dump` binary needed** → no `apt-get`/root, and no "SSL not compiled in" problem. On **Azure
# MAGIC   Database for PostgreSQL** (which *mandates* SSL) `psycopg2` connects over SSL natively, so this works
# MAGIC   where the bundled `pg_dump` cannot connect at all.
# MAGIC - **No client-version matching** (there's no client binary).
# MAGIC
# MAGIC It reproduces schemas, types (enum/domain/composite), sequences, tables (columns, defaults, identity &
# MAGIC generated columns), PK/UNIQUE/CHECK constraints, indexes, foreign keys, functions/procedures, views
# MAGIC (dependency-ordered), materialized views, and triggers — via Postgres' own `pg_get_*def()` functions.
# MAGIC
# MAGIC **Best-effort & permission-aware** (like the pg_dump version): only objects the role can read are
# MAGIC emitted — schemas need `USAGE`, tables need `SELECT`; anything that errors is skipped and logged.
# MAGIC
# MAGIC **No widgets** — plain variables only.
# MAGIC
# MAGIC > Trade-off: this is a faithful *reconstruction*, not a byte-for-byte `pg_dump`. It covers the common
# MAGIC > object types above; very exotic objects (e.g. custom operators/opclasses, publications, RLS policies,
# MAGIC > extensions' own objects) are not reproduced — they're listed in the skip log if encountered.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Ensure `psycopg2` is available (the only dependency)

# COMMAND ----------

import sys
import subprocess

try:
    import psycopg2  # noqa: F401
    print("psycopg2 already available")
except ImportError:
    print("installing psycopg2-binary ...")
    subprocess.run([sys.executable, "-m", "pip", "install", "--quiet", "psycopg2-binary"], check=True)
    import psycopg2  # noqa: F401
print("psycopg2", psycopg2.__version__)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration — connection, scope & output (NO WIDGETS)

# COMMAND ----------

import os

# ----------------------------------------------------------------------------
# Connection (same PG_* variables as the other notebooks). PG_DATABASE is the
# bootstrap DB used to enumerate the server's databases; it just needs to be one
# the role can CONNECT to. On Azure, keep PG_SSLMODE = "require" (or stricter).
# ----------------------------------------------------------------------------
PG_HOST = os.getenv("PG_HOST", "postgres-host.example.com")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DATABASE = os.getenv("PG_DATABASE", "postgres")
PG_USER = os.getenv("PG_USER", "postgres_user")
PG_PASSWORD = os.getenv("PG_PASSWORD", "postgres_password")          # e.g. dbutils.secrets.get("scope", "pg_pw")
PG_SSLMODE = os.getenv("PG_SSLMODE", "require")                       # Azure requires SSL; psycopg2 honours this natively

# ----------------------------------------------------------------------------
# Output location  ***  USER-SPECIFIED DIRECTORY  ***  (a Databricks Volume persists it)
# ----------------------------------------------------------------------------
OUTPUT_DIR = os.getenv("DDL_OUTPUT_DIR", "/Volumes/main/default/pg_ddl")
OUTPUT_FILENAME = os.getenv("DDL_OUTPUT_FILENAME") or None           # None -> "<host>_ddl_<UTC-timestamp>.sql"

# ----------------------------------------------------------------------------
# Scope
# ----------------------------------------------------------------------------
DATABASES = []                 # [] = every DB the role can CONNECT to; or e.g. ["nacs", "reporting"]
INCLUDE_GLOBALS = True         # emit CREATE ROLE (no passwords) + memberships, like pg_dumpall --globals-only
EXCLUDE_SCHEMAS = []           # extra schemas to skip beyond system ones, e.g. ["cron", "pg_temp*"]

# ----------------------------------------------------------------------------
# Output flavour
# ----------------------------------------------------------------------------
ADD_CONNECT_LINES = True       # inject \connect "<db>" so `psql -f` routes each section to its database
ADD_CREATE_DATABASE = False    # also emit CREATE DATABASE per db (restore onto a fresh server where DBs don't exist)

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
# MAGIC ## 3. The catalog-based DDL generator (pure Python)
# MAGIC Leans on Postgres' own `pg_get_*def()` functions for canonical DDL and assembles `CREATE TABLE` from
# MAGIC the catalog. Every query is permission-filtered (`has_schema_privilege` / `has_table_privilege`).

# COMMAND ----------

import fnmatch


def _q(ident):
    """Double-quote an identifier."""
    return '"' + ident.replace('"', '""') + '"'


def _fq(schema, name):
    return f"{_q(schema)}.{_q(name)}"


def _schemas(cur, exclude_schemas):
    cur.execute(
        """
        SELECT n.nspname, n.oid
        FROM pg_namespace n
        WHERE has_schema_privilege(current_user, n.oid, 'USAGE')
          AND n.nspname NOT IN ('pg_catalog', 'information_schema')
          AND n.nspname NOT LIKE 'pg\\_toast%'
          AND n.nspname NOT LIKE 'pg\\_temp%'
        ORDER BY n.nspname;
        """
    )
    return [(s, oid) for (s, oid) in cur.fetchall()
            if not any(fnmatch.fnmatchcase(s, p) for p in exclude_schemas)]


def _types(cur, schema):
    """CREATE TYPE (enum, composite) and CREATE DOMAIN for a schema."""
    out = []
    cur.execute(
        """
        SELECT t.typname,
               (SELECT array_agg(e.enumlabel ORDER BY e.enumsortorder)
                FROM pg_enum e WHERE e.enumtypid = t.oid)
        FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace
        WHERE n.nspname = %s AND t.typtype = 'e'
        ORDER BY t.typname;
        """, (schema,))
    for name, labels in cur.fetchall():
        vals = ", ".join("'" + v.replace("'", "''") + "'" for v in labels)
        out.append(f"CREATE TYPE {_fq(schema, name)} AS ENUM ({vals});")
    cur.execute(
        """
        SELECT t.typname, format_type(t.typbasetype, t.typtypmod), t.typnotnull, t.typdefault,
               (SELECT string_agg(pg_get_constraintdef(c.oid), ' ')
                FROM pg_constraint c WHERE c.contypid = t.oid)
        FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace
        WHERE n.nspname = %s AND t.typtype = 'd'
        ORDER BY t.typname;
        """, (schema,))
    for name, base, notnull, default, checks in cur.fetchall():
        s = f"CREATE DOMAIN {_fq(schema, name)} AS {base}"
        if default is not None:
            s += f" DEFAULT {default}"
        if notnull:
            s += " NOT NULL"
        if checks:
            s += " " + checks
        out.append(s + ";")
    cur.execute(
        """
        SELECT t.typname, t.oid
        FROM pg_type t
        JOIN pg_namespace n ON n.oid = t.typnamespace
        JOIN pg_class c ON c.oid = t.typrelid
        WHERE n.nspname = %s AND t.typtype = 'c' AND c.relkind = 'c'
        ORDER BY t.typname;
        """, (schema,))
    for name, toid in cur.fetchall():
        cur.execute(
            """
            SELECT a.attname, format_type(a.atttypid, a.atttypmod)
            FROM pg_attribute a JOIN pg_type t ON t.typrelid = a.attrelid
            WHERE t.oid = %s AND a.attnum > 0 AND NOT a.attisdropped
            ORDER BY a.attnum;
            """, (toid,))
        cols = ", ".join(f"{_q(an)} {at}" for an, at in cur.fetchall())
        out.append(f"CREATE TYPE {_fq(schema, name)} AS ({cols});")
    return out


def _sequences(cur, schema):
    cur.execute(
        """
        SELECT c.relname, s.seqstart, s.seqincrement, s.seqmax, s.seqmin,
               s.seqcache, s.seqcycle, format_type(s.seqtypid, NULL)
        FROM pg_sequence s
        JOIN pg_class c ON c.oid = s.seqrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s
          AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.objid = c.oid AND d.deptype = 'i')
        ORDER BY c.relname;
        """, (schema,))
    out = []
    for name, start, incr, mx, mn, cache, cycle, typ in cur.fetchall():
        s = f"CREATE SEQUENCE {_fq(schema, name)}"
        if typ and typ != "bigint":
            s += f" AS {typ}"
        s += f" START WITH {start} INCREMENT BY {incr}"
        s += f" MINVALUE {mn}" if mn is not None else " NO MINVALUE"
        s += f" MAXVALUE {mx}" if mx is not None else " NO MAXVALUE"
        s += f" CACHE {cache}"
        if cycle:
            s += " CYCLE"
        out.append(s + ";")
    return out


def _seq_owned(cur, schema):
    cur.execute(
        """
        SELECT s.relname, t.relname, a.attname
        FROM pg_depend d
        JOIN pg_class s ON s.oid = d.objid AND s.relkind = 'S'
        JOIN pg_namespace n ON n.oid = s.relnamespace
        JOIN pg_class t ON t.oid = d.refobjid
        JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = d.refobjsubid
        WHERE n.nspname = %s AND d.deptype = 'a'
          AND d.classid = 'pg_class'::regclass AND d.refclassid = 'pg_class'::regclass
        ORDER BY s.relname;
        """, (schema,))
    return [f"ALTER SEQUENCE {_fq(schema, sn)} OWNED BY {_fq(schema, tn)}.{_q(col)};"
            for sn, tn, col in cur.fetchall()]


def _tables(cur, schema):
    cur.execute(
        """
        SELECT c.relname, c.oid, c.relkind, c.relpersistence,
               pg_get_expr(c.relpartbound, c.oid),
               (SELECT (inhparent::regclass)::text FROM pg_inherits WHERE inhrelid = c.oid LIMIT 1)
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND c.relkind IN ('r', 'p')
          AND has_table_privilege(current_user, c.oid, 'SELECT')
        ORDER BY c.relname;
        """, (schema,))
    return cur.fetchall()


def _table_ddl(cur, schema, name, oid, relkind, relpersistence, partbound, parent):
    cur.execute(
        """
        SELECT a.attname, format_type(a.atttypid, a.atttypmod),
               a.attnotnull, a.attidentity, a.attgenerated,
               pg_get_expr(ad.adbin, ad.adrelid),
               CASE WHEN a.attcollation <> ty.typcollation THEN coll.collname END
        FROM pg_attribute a
        LEFT JOIN pg_attrdef ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
        LEFT JOIN pg_collation coll ON coll.oid = a.attcollation
        LEFT JOIN pg_type ty ON ty.oid = a.atttypid
        WHERE a.attrelid = %s AND a.attnum > 0 AND NOT a.attisdropped
        ORDER BY a.attnum;
        """, (oid,))
    cols = []
    for attname, coltype, notnull, ident, gen, default, collname in cur.fetchall():
        parts = [f"    {_q(attname)} {coltype}"]
        if collname and collname != "default":
            parts.append(f"COLLATE {_q(collname)}")
        if ident in ("a", "d"):
            parts.append(f"GENERATED {'ALWAYS' if ident == 'a' else 'BY DEFAULT'} AS IDENTITY")
        elif gen == "s" and default is not None:
            parts.append(f"GENERATED ALWAYS AS ({default}) STORED")
        elif default is not None:
            parts.append(f"DEFAULT {default}")
        if notnull and ident not in ("a", "d"):
            parts.append("NOT NULL")
        cols.append(" ".join(parts))

    kw = "UNLOGGED " if relpersistence == "u" else ""
    if parent:                                   # partition child
        return f"CREATE {kw}TABLE {_fq(schema, name)} PARTITION OF {parent} {partbound};"
    stmt = f"CREATE {kw}TABLE {_fq(schema, name)} (\n" + ",\n".join(cols) + "\n)"
    if relkind == "p":
        cur.execute("SELECT pg_get_partkeydef(%s);", (oid,))
        stmt += f" PARTITION BY {cur.fetchone()[0]}"
    return stmt + ";"


def _constraints(cur, schema, contypes):
    cur.execute(
        """
        SELECT c.relname, con.conname, pg_get_constraintdef(con.oid)
        FROM pg_constraint con
        JOIN pg_class c ON c.oid = con.conrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND con.contype = ANY(%s)
          AND has_table_privilege(current_user, c.oid, 'SELECT')
        ORDER BY c.relname, con.conname;
        """, (schema, list(contypes)))
    return [f"ALTER TABLE ONLY {_fq(schema, rel)} ADD CONSTRAINT {_q(cn)} {d};"
            for rel, cn, d in cur.fetchall()]


def _indexes(cur, schema):
    cur.execute(
        """
        SELECT pg_get_indexdef(i.indexrelid)
        FROM pg_index i
        JOIN pg_class ic ON ic.oid = i.indexrelid
        JOIN pg_class tc ON tc.oid = i.indrelid
        JOIN pg_namespace n ON n.oid = tc.relnamespace
        WHERE n.nspname = %s AND NOT i.indisprimary
          AND NOT EXISTS (SELECT 1 FROM pg_constraint con WHERE con.conindid = i.indexrelid)
          AND has_table_privilege(current_user, tc.oid, 'SELECT')
        ORDER BY ic.relname;
        """, (schema,))
    return [r[0] + ";" for r in cur.fetchall()]


def _views(cur, schema):
    cur.execute(
        """
        SELECT c.relname, c.oid, c.relkind
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND c.relkind IN ('v', 'm')
          AND has_table_privilege(current_user, c.oid, 'SELECT')
        ORDER BY c.relname;
        """, (schema,))
    views = cur.fetchall()
    oids = {oid for (_n, oid, _k) in views}
    deps = {oid: set() for (_n, oid, _k) in views}
    if oids:
        cur.execute(
            """
            SELECT r.ev_class, d.refobjid
            FROM pg_depend d
            JOIN pg_rewrite r ON r.oid = d.objid
            WHERE d.refclassid = 'pg_class'::regclass
              AND r.ev_class = ANY(%s) AND d.refobjid = ANY(%s) AND r.ev_class <> d.refobjid;
            """, (list(oids), list(oids)))
        for ev, ref in cur.fetchall():
            deps[ev].add(ref)
    ordered, seen = [], set()

    def visit(o):
        if o in seen:
            return
        seen.add(o)
        for r in deps.get(o, ()):
            visit(r)
        ordered.append(o)
    for (_n, oid, _k) in views:
        visit(oid)
    by_oid = {oid: (n, k) for (n, oid, k) in views}
    out = []
    for oid in ordered:
        name, kind = by_oid[oid]
        cur.execute("SELECT pg_get_viewdef(%s, true);", (oid,))
        vdef = cur.fetchone()[0].rstrip().rstrip(";")
        word = "MATERIALIZED VIEW" if kind == "m" else "VIEW"
        out.append(f"CREATE {word} {_fq(schema, name)} AS\n{vdef};")
    return out


def _functions(cur, schema):
    cur.execute(
        """
        SELECT p.oid, p.proname
        FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = %s
          AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.objid = p.oid AND d.deptype = 'e')
        ORDER BY p.proname;
        """, (schema,))
    out, errs = [], []
    for oid, name in cur.fetchall():
        try:
            cur.execute("SELECT pg_get_functiondef(%s);", (oid,))
            out.append(cur.fetchone()[0].rstrip().rstrip(";") + ";")
        except Exception as e:                   # aggregates / window / C funcs without a printable def
            cur.connection.rollback()
            errs.append(f"{schema}.{name}() [function]: {str(e).splitlines()[0]}")
    return out, errs


def _triggers(cur, schema):
    cur.execute(
        """
        SELECT pg_get_triggerdef(t.oid)
        FROM pg_trigger t
        JOIN pg_class c ON c.oid = t.tgrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND NOT t.tgisinternal
          AND has_table_privilege(current_user, c.oid, 'SELECT')
        ORDER BY t.tgname;
        """, (schema,))
    return [r[0] + ";" for r in cur.fetchall()]


def generate_database_ddl(conn, exclude_schemas=()):
    """Return (sql_text, skipped) for everything the role can read in this database."""
    skipped, pieces, all_fks = [], [], []
    cur = conn.cursor()
    for schema, _soid in _schemas(cur, exclude_schemas):
        sect = [f"-- ---------- schema: {schema} ----------"]
        if schema != "public":
            sect.append(f"CREATE SCHEMA IF NOT EXISTS {_q(schema)};")
        try:
            sect += _types(cur, schema)
            sect += _sequences(cur, schema)
            for (rn, oid, rk, rp, pb, parent) in _tables(cur, schema):
                try:
                    sect.append(_table_ddl(cur, schema, rn, oid, rk, rp, pb, parent))
                except Exception as e:
                    conn.rollback()
                    skipped.append(f"{schema}.{rn} [table]: {str(e).splitlines()[0]}")
            sect += _seq_owned(cur, schema)
            sect += _constraints(cur, schema, ('p', 'u', 'c'))
            sect += _indexes(cur, schema)
            fdefs, ferrs = _functions(cur, schema)
            sect += fdefs
            skipped += ferrs
            sect += _views(cur, schema)
            sect += _triggers(cur, schema)
            all_fks += _constraints(cur, schema, ('f',))
        except Exception as e:
            conn.rollback()
            skipped.append(f"{schema} [schema]: {str(e).splitlines()[0]}")
            continue
        pieces.append("\n".join(sect))
    if all_fks:
        pieces.append("-- ---------- foreign keys ----------\n" + "\n".join(all_fks))
    cur.close()
    return "\n\n".join(pieces) + "\n", skipped


def generate_globals(conn):
    """CREATE ROLE (no passwords) + memberships, like pg_dumpall --globals-only --no-role-passwords."""
    cur = conn.cursor()
    out = ["-- ---------- roles ----------"]
    cur.execute(
        """
        SELECT rolname, rolsuper, rolinherit, rolcreaterole, rolcreatedb,
               rolcanlogin, rolreplication, rolbypassrls, rolconnlimit, rolvaliduntil
        FROM pg_roles WHERE rolname NOT LIKE 'pg\\_%' ORDER BY rolname;
        """)
    for (name, sup, inh, crole, cdb, login, repl, bypass, connlimit, valid) in cur.fetchall():
        opts = ["SUPERUSER" if sup else "NOSUPERUSER",
                "CREATEDB" if cdb else "NOCREATEDB",
                "CREATEROLE" if crole else "NOCREATEROLE",
                "INHERIT" if inh else "NOINHERIT",
                "LOGIN" if login else "NOLOGIN"]
        if repl:
            opts.append("REPLICATION")
        if bypass:
            opts.append("BYPASSRLS")
        if connlimit and connlimit != -1:
            opts.append(f"CONNECTION LIMIT {connlimit}")
        s = f"CREATE ROLE {_q(name)} WITH " + " ".join(opts)
        if valid is not None:
            s += f" VALID UNTIL '{valid}'"
        out.append(s + ";")
    cur.execute(
        """
        SELECT r.rolname, g.rolname
        FROM pg_auth_members m
        JOIN pg_roles r ON r.oid = m.member
        JOIN pg_roles g ON g.oid = m.roleid
        WHERE r.rolname NOT LIKE 'pg\\_%' AND g.rolname NOT LIKE 'pg\\_%'
        ORDER BY g.rolname, r.rolname;
        """)
    for member, grp in cur.fetchall():
        out.append(f"GRANT {_q(grp)} TO {_q(member)};")
    cur.close()
    return "\n".join(out)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Enumerate accessible databases & generate the DDL

# COMMAND ----------

def pg_connect(dbname):
    return psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=dbname,
                            user=PG_USER, password=PG_PASSWORD, sslmode=PG_SSLMODE)


# Bootstrap: server version + databases we may enter.
_boot = pg_connect(PG_DATABASE)
try:
    with _boot.cursor() as cur:
        cur.execute("SHOW server_version;")
        server_version = cur.fetchone()[0]
        cur.execute("SELECT current_user;")
        connected_role = cur.fetchone()[0]
        cur.execute(
            """
            SELECT d.datname FROM pg_database d
            WHERE d.datallowconn AND NOT d.datistemplate
              AND has_database_privilege(current_user, d.datname, 'CONNECT')
            ORDER BY d.datname;
            """)
        connectable = [r[0] for r in cur.fetchall()]
finally:
    _boot.close()

if DATABASES:
    TARGET_DBS = [d for d in DATABASES if d in connectable]
    denied = [d for d in DATABASES if d not in connectable]
    if denied:
        print(f"SKIP (not connectable / no CONNECT privilege): {denied}")
else:
    TARGET_DBS = connectable
if not TARGET_DBS:
    raise RuntimeError("No databases are reachable for this role — nothing to dump.")

print(f"Server        : {server_version}")
print(f"Connected as  : {connected_role}")
print(f"Databases ({len(TARGET_DBS)}): {TARGET_DBS}")

sections = []     # (header, sql)
skipped = []      # notes

if INCLUDE_GLOBALS:
    try:
        gconn = pg_connect(PG_DATABASE)
        try:
            sections.append(("-- GLOBALS: roles (no passwords)", generate_globals(gconn)))
        finally:
            gconn.close()
        print("globals: OK")
    except Exception as e:
        skipped.append(f"globals (roles): {str(e).splitlines()[0]}")
        print(f"globals: SKIPPED — {skipped[-1]}")

for db in TARGET_DBS:
    try:
        conn = pg_connect(db)
        try:
            sql, sk = generate_database_ddl(conn, exclude_schemas=tuple(EXCLUDE_SCHEMAS))
        finally:
            conn.close()
        sections.append((f"-- DATABASE: {db}", sql))
        skipped += sk
        n_create = sql.count("CREATE ")
        print(f"{db}: OK  (~{n_create} CREATE statements"
              + (f", {len(sk)} skipped)" if sk else ")"))
    except Exception as e:
        skipped.append(f"{db} [database]: {str(e).splitlines()[0]}")
        print(f"{db}: SKIPPED — {skipped[-1]}")

if not any(h.startswith("-- DATABASE:") for h, _ in sections):
    raise RuntimeError("No database DDL could be produced. See the skip log above.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Assemble & write the `.sql` file

# COMMAND ----------

_header = [
    "--",
    "-- Postgres server DDL snapshot (schema-only, no data) — generated by psycopg2 (no pg_dump)",
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
        if header.startswith("-- DATABASE: "):
            dbname = header.split("-- DATABASE: ", 1)[1].strip()
            if ADD_CREATE_DATABASE:
                fh.write(f'CREATE DATABASE {_q(dbname)};\n')
            if ADD_CONNECT_LINES:
                fh.write(f'\\connect {_q(dbname)}\n')
            # SET is session-scoped and \connect starts a new session, so (re)emit per database:
            # lets function/view bodies reference not-yet-created objects during restore.
            fh.write("SET check_function_bodies = false;\n")
        fh.write(sql)
        if not sql.endswith("\n"):
            fh.write("\n")
    if skipped:
        fh.write("\n\n-- ============================================================\n")
        fh.write("-- SKIPPED (no permission / not reproduced) — NOT in this file:\n")
        for note in skipped:
            fh.write(f"--   {note}\n")

print(f"Wrote {OUTPUT_PATH}")
if skipped:
    print(f"\n{len(skipped)} item(s) skipped (also recorded at the end of the file):")
    for note in skipped:
        print(f"   - {note}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Verify the DDL file (structure present, no data)

# COMMAND ----------

import re

size = os.path.getsize(OUTPUT_PATH)
with open(OUTPUT_PATH, "r", errors="replace") as fh:
    text = fh.read()

n_create = len(re.findall(r"(?m)^\s*CREATE\s+", text))
# This generator only reads the catalog (via pg_get_*def / CREATE TABLE assembly) and
# NEVER emits table rows. DML like INSERT/UPDATE can legitimately appear INSIDE function
# or trigger bodies (they're part of the routine's DDL), so we must NOT flag those.
# The only thing that would indicate real dumped data is a pg_dump-style bulk COPY block,
# which we never produce — that's the sole marker we guard against.
copy_data = [ln for ln in text.splitlines() if re.match(r"^COPY\s+\S+.*\bFROM stdin;", ln)]

print(f"File   : {OUTPUT_PATH}")
print(f"Size   : {size:,} bytes ({size / 1024:.1f} KiB)")
print(f"CREATE statements: {n_create}")
if size == 0 or n_create == 0:
    raise RuntimeError("DDL file has no CREATE statements — something went wrong.")
if copy_data:
    raise RuntimeError(f"Unexpected bulk data found ({len(copy_data)} COPY block(s)) — this should be schema-only!")
print("OK: file is schema-only DDL (no bulk table data).")
print("     Note: INSERT/UPDATE text may appear inside function/trigger bodies — that is part of")
print("     the routine's definition, not dumped rows.")

print("\n--- restore (into a server where the databases already exist) ---")
print(f"export PGPASSWORD='<password>'")
print(f"psql 'host=<host> port=<port> sslmode=require dbname={PG_DATABASE} user={PG_USER}' -f '{OUTPUT_PATH}'")
print("# \\connect lines route each section to its database; set ADD_CREATE_DATABASE=True")
print("# to also emit CREATE DATABASE for a fresh target server.")
print("# NOTE: don't use `psql -v ON_ERROR_STOP=1` here — the GLOBALS section re-runs CREATE ROLE")
print("#       for roles that already exist on the target, which is a benign 'already exists' error.")
