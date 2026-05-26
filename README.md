# gno-ibc-relayer-api

A lightweight Go API server that indexes IBC packet transfer events from the [Union](https://union.build) relayer's PostgreSQL database and exposes them via a REST API.

## Overview

The Union relayer processes cross-chain transfers through three tables (`queue`, `done`, `failed`). This server reads those tables, decodes the ABI-encoded `ZkgmPacket` payload, and tracks each transfer's lifecycle in a `transfers` table.

```
Union Relayer DB (read-only)          Our DB
┌────────────────────────┐            ┌──────────────┐
│ queue  → packet_send   │──indexer──▶│  transfers   │
│ done                   │            │  (decoded)   │
│ failed                 │            └──────────────┘
└────────────────────────┘                  │
                                        REST API
```

**Supported chains:** gno ↔ Ethereum (union is an intermediate relay and is excluded)

### Transfer status

| Value | Name       | Description                              |
|-------|------------|------------------------------------------|
| `0`   | detected   | `packet_send` found in relayer queue     |
| `1`   | processing | item removed from queue, not yet settled |
| `2`   | done       | item appeared in relayer `done` table    |
| `3`   | failed     | item appeared in relayer `failed` table  |

## Requirements

- Go 1.22+
- PostgreSQL 16 (shared with the relayer)

## Configuration

Copy `config.toml` and fill in your database credentials.

```toml
[server]
port = 8080

[relayer_db]           # relayer's existing DB (read-only)
host     = "127.0.0.1"
port     = 5432
user     = "postgres"
password = "secret"
dbname   = "voyager"
sslmode  = "disable"

[app_db]               # DB where transfers table lives (can be the same DB)
host     = "127.0.0.1"
port     = 5432
user     = "postgres"
password = "secret"
dbname   = "voyager"
sslmode  = "disable"

[indexer]
poll_interval_sec = 5   # how often to poll the relayer DB
batch_size        = 100

# gno <> eth direct channel mapping
[[channel_chains]]
src_chain_id   = "dev"
dst_chain_id   = "11155111"
src_channel_id = 2
dst_channel_id = 28

[[channel_chains]]
src_chain_id   = "11155111"
dst_chain_id   = "dev"
src_channel_id = 28
dst_channel_id = 2
```

## Setup

**1. Run the migration**

```bash
psql "host=127.0.0.1 user=postgres dbname=voyager" -f migrations/001_init.sql
```

**2. Build and run**

```bash
make run
# or
go run ./cmd/server -config config.toml
```

## API

### GET `/transfers/{id}`

Fetch a single transfer by its relayer queue ID.

```bash
curl http://localhost:8080/transfers/74939729
```

---

### GET `/transfers`

List transfers by wallet address. `address` is required — omitting it returns an empty array.

| Parameter | Type   | Required | Description                              |
|-----------|--------|----------|------------------------------------------|
| `address` | string | yes      | Matches `from_address` OR `to_address`   |
| `status`  | int    | no       | Filter by status (0–3). Omit for all.    |
| `order`   | string | no       | `desc` (default, newest first) or `asc`  |
| `limit`   | int    | no       | Max results, default 20, max 100         |
| `offset`  | int    | no       | Pagination offset                        |

```bash
# All transfers for a wallet
curl "http://localhost:8080/transfers?address=0xf4ad3b02d44fa88371ef8faa232f789068b5f56b"

# Only completed transfers
curl "http://localhost:8080/transfers?address=g1jg8mtutu9khhfwc4nxmuhcpftf0pajdhfvsqf5&status=2"

# Oldest first
curl "http://localhost:8080/transfers?address=0xf4ad3b02...&order=asc"
```

**Response**

```json
{
  "data": [
    {
      "id": 74939729,
      "packet_hash": "0xfd67a60d...",
      "src_chain_id": "dev",
      "dst_chain_id": "11155111",
      "src_channel_id": 2,
      "dst_channel_id": 28,
      "from_address": "g1jg8mtutu9khhfwc4nxmuhcpftf0pajdhfvsqf5",
      "to_address": "0xf4ad3b02d44fa88371ef8faa232f789068b5f56b",
      "base_token": "0x7fed1d819109fb7a095137bf867abe61db36c99c",
      "base_amount": "1000000",
      "quote_token": "ugnot",
      "quote_amount": "1000000",
      "height": 81037,
      "tx_hash": "0x3966e3f3...",
      "timeout_timestamp": 1779859590954000000,
      "status": 2,
      "created_at": "2026-05-26T05:28:50Z",
      "done_at": "2026-05-26T05:29:12Z"
    }
  ],
  "limit": 20,
  "offset": 0
}
```

---

### GET `/stats`

```bash
curl http://localhost:8080/stats
```

```json
{
  "total": 1024,
  "detected": 3,
  "processing": 5,
  "done": 1010,
  "failed": 6
}
```
