# Convenience targets for the local Docker test environment.

.PHONY: test up down clean logs

# Boot the stack, run the migration, diff row counts, then tear down.
test:
	./docker/test/run_test.sh

# Start the databases and leave them running.
up:
	docker compose up -d

# Stop the databases (keep data volumes).
down:
	docker compose down

# Stop the databases and wipe data volumes (fresh start next time).
clean:
	docker compose down -v

# Follow Oracle startup / init logs.
logs:
	docker compose logs -f oracle
