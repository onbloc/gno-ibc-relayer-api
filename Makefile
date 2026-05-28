BINARY  := gno-ibc-relayer-api
CONFIG  := config.toml
DB_HOST ?= 127.0.0.1
DB_PORT ?= 5432
DB_USER ?= postgres
DB_NAME ?= voyager

.PHONY: build run init seed seed-clean drop tidy

build:
	go build -o $(BINARY) ./cmd/server

# create tables, run migrations, and install pg_notify triggers (run once)
init:
	psql "host=$(DB_HOST) port=$(DB_PORT) user=$(DB_USER) dbname=$(DB_NAME)" \
		-f migrations/001_init.sql
	psql "host=$(DB_HOST) port=$(DB_PORT) user=$(DB_USER) dbname=$(DB_NAME)" \
		-f migrations/002_add_err_msg.sql
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

# drop all tables (transfers, indexer_cursors)
drop:
	psql "host=$(DB_HOST) port=$(DB_PORT) user=$(DB_USER) dbname=$(DB_NAME)" \
		-c "DROP TABLE IF EXISTS transfers; DROP TABLE IF EXISTS indexer_cursors;"

tidy:
	go mod tidy
