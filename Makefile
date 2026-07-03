BINARY  := gno-ibc-relayer-api
CONFIG  := config.toml
DB_HOST ?= 127.0.0.1
DB_PORT ?= 5432
DB_USER ?= postgres
DB_NAME ?= voyager

.PHONY: build run init seed seed-clean drop tidy test help

help:
	@echo "Usage: make <target>"
	@echo ""
	@echo "Targets:"
	@echo "  build       Build the server binary"
	@echo "  run         Build and start the server in the background (logs → indexer.log)"
	@echo "  init        Create tables, run migrations, install pg_notify triggers (run once)"
	@echo "  seed        Insert 100 dummy transfers (keeps existing data)"
	@echo "  seed-clean  Truncate transfers table then insert 100 dummy transfers"
	@echo "  drop        Drop all tables and pg_notify triggers/functions"
	@echo "  test        Run all unit tests"
	@echo "  tidy        Run go mod tidy"
	@echo "  help        Show this help message"

build:
	go build -o $(BINARY) ./cmd/server

# create tables, run migrations, and install pg_notify triggers (run once)
init:
	psql "host=$(DB_HOST) port=$(DB_PORT) user=$(DB_USER) dbname=$(DB_NAME)" \
		-f migrations/001_init.sql
	psql "host=$(DB_HOST) port=$(DB_PORT) user=$(DB_USER) dbname=$(DB_NAME)" \
		-f migrations/002_add_err_msg.sql
	psql "host=$(DB_HOST) port=$(DB_PORT) user=$(DB_USER) dbname=$(DB_NAME)" \
		-f migrations/003_add_tx_in_out.sql
	go run ./cmd/setup-trigger -config $(CONFIG)

# build and start the server in the background, logs go to indexer.log
run: build
	./$(BINARY) -config $(CONFIG) >> indexer.log 2>&1 &

# insert 100 dummy transfers (keeps existing data)
seed:
	go run ./cmd/seed -config $(CONFIG)

# truncate transfers table then insert 100 dummy transfers
seed-clean:
	go run ./cmd/seed -config $(CONFIG) -truncate

# drop all tables and pg_notify triggers/functions
drop:
	psql "host=$(DB_HOST) port=$(DB_PORT) user=$(DB_USER) dbname=$(DB_NAME)" \
		-c "DROP TABLE IF EXISTS transfers; DROP TABLE IF EXISTS indexer_cursors; \
		    DROP TRIGGER IF EXISTS queue_insert_trigger ON queue; \
		    DROP TRIGGER IF EXISTS done_insert_trigger ON done; \
		    DROP TRIGGER IF EXISTS failed_insert_trigger ON failed; \
		    DROP FUNCTION IF EXISTS notify_queue_insert; \
		    DROP FUNCTION IF EXISTS notify_done_insert; \
		    DROP FUNCTION IF EXISTS notify_failed_insert;"

test:
	go test ./internal/... -v

tidy:
	go mod tidy
