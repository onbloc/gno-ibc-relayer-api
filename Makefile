BINARY  := gno-ibc-relayer-api
CONFIG  := config.toml
DB_HOST ?= 127.0.0.1
DB_PORT ?= 5432
DB_USER ?= postgres
DB_NAME ?= voyager

.PHONY: build run migrate migrate-drop tidy

build:
	go build -o $(BINARY) ./cmd/server

run: build
	./$(BINARY) -config $(CONFIG)

migrate:
	psql "host=$(DB_HOST) port=$(DB_PORT) user=$(DB_USER) dbname=$(DB_NAME)" \
		-f migrations/001_init.sql

migrate-drop:
	psql "host=$(DB_HOST) port=$(DB_PORT) user=$(DB_USER) dbname=$(DB_NAME)" \
		-c "DROP TABLE IF EXISTS transfers; DROP TABLE IF EXISTS indexer_cursors;"

tidy:
	go mod tidy
