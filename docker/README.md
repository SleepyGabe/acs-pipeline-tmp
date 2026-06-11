# Local Docker test environment

Spins up an **Oracle** (source) and a **Postgres** (target) database so you can test-run the
`oracle_to_postgres_migration.py` notebook end to end against real databases.

## What's in here

```
docker-compose.yml                 # both DB containers (lives in repo root)
docker/oracle/init/01_schema.sql   # creates CUSTOMERS + ORDERS (+ seq, identity, FK, index, check)
docker/oracle/init/02_seed.sql     # inserts 3 customers + 3 orders
docker/postgres/init/01_init.sql   # ensures target schema + pgcrypto
docker/README.md                   # this file
```

The Oracle seed deliberately exercises every notebook feature: identity column, standalone
sequence, PK/UNIQUE/CHECK constraints, an index, and a FOREIGN KEY (`ORDERS → CUSTOMERS`) so
the dependency-aware load ordering and deferred-FK pass both get tested.

## Start it

```bash
docker compose up -d
docker compose logs -f oracle      # wait for "DATABASE IS READY TO USE!" (first run: a few minutes)
docker compose ps                  # both should be (healthy)
```

Oracle's **first** startup runs the init scripts and takes a while. Subsequent starts are fast
(data persists in the `oracle-data` / `postgres-data` volumes). To start completely fresh:

```bash
docker compose down -v && docker compose up -d
```

## Point the notebook at it (cell §1)

These containers match the notebook's default placeholders except for host/service. Set:

| Notebook variable | Value |
|-------------------|-------|
| `ORACLE_HOST` | `localhost` (or `host.docker.internal` from another container) |
| `ORACLE_PORT` | `1521` |
| `ORACLE_SERVICE_NAME` | `XEPDB1` |
| `ORACLE_SID` | `None` |
| `ORACLE_USER` | `oracle_user` |
| `ORACLE_PASSWORD` | `oracle_password` |
| `ORACLE_SCHEMA` | `ORACLE_USER` |
| `PG_HOST` | `localhost` |
| `PG_PORT` | `5432` |
| `PG_DATABASE` | `target_db` |
| `PG_USER` | `postgres_user` |
| `PG_PASSWORD` | `postgres_password` |
| `PG_SCHEMA` | `public` |
| `TABLE_NAMES` | `["CUSTOMERS", "ORDERS"]` (or set `DISCOVER_TABLES = True`) |

> Running the notebook on Databricks? Databricks can't reach `localhost`. Either run the
> migration logic locally (plain Python — the cells work outside Databricks except the
> `%pip`/`display`/`spark` bits), or expose these databases on a host Databricks can route to.

## Verify the result

```bash
# Source rows (Oracle)
docker exec -it ora2pg-oracle sqlplus -s oracle_user/oracle_password@localhost:1521/XEPDB1 \
  <<< "SELECT COUNT(*) FROM CUSTOMERS; SELECT COUNT(*) FROM ORDERS; EXIT;"

# Target rows (Postgres) — after running the notebook
docker exec -it ora2pg-postgres psql -U postgres_user -d target_db \
  -c "SELECT count(*) FROM customers;" -c "SELECT count(*) FROM orders;"
```

Expect 3 customers and 3 orders on both sides.

## Apple Silicon / arm64

`gvenzl/oracle-xe:21-slim` is amd64. On Apple Silicon either let Docker emulate it (slow) by
adding under the `oracle` service:

```yaml
    platform: linux/amd64
```

or switch to the arm64-native image — set `image: gvenzl/oracle-free:23-slim` and change the
notebook's `ORACLE_SERVICE_NAME` to `FREEPDB1` (everything else is identical).
