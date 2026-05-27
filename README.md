# gno-ibc-relayer-api

A lightweight Go API server that indexes IBC packet transfer events from the [Union](https://union.build) relayer's PostgreSQL database and exposes them via a REST API.

## Overview

The Union relayer processes cross-chain transfers through three tables (`queue`, `done`, `failed`). This server reads those tables, decodes the ABI-encoded `ZkgmPacket` payload, and tracks each transfer's lifecycle in a `transfers` table.

**Supported chains:** gno ↔ Ethereum (union is an intermediate relay and is excluded)

**Event mapping per direction:**
- **gno→eth**: detected via `packet_send` on gno queue
- **eth→gno**: detected via `packet_recv` on gno queue (appears after gno has confirmed receipt)

### Transfer status

| Value | Name       | Description                                           |
|-------|------------|-------------------------------------------------------|
| `0`   | detected   | `packet_send` or `packet_recv` found in relayer queue |
| `1`   | processing | item removed from queue, not yet settled              |
| `2`   | done       | item appeared in relayer `done` table                 |
| `3`   | failed     | item appeared in relayer `failed` table               |

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

### GET `/status/{packet_hash}`

Fetch a single transfer by its packet hash.

```bash
curl http://localhost:8080/status/0xfd67a60d...
```

**Response**

```json
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
```

---

### GET `/wallet/{sender_address}`

List transfers by wallet address. Matches `from_address` OR `to_address`.

| Parameter | Type   | Required | Description                             |
|-----------|--------|----------|-----------------------------------------|
| `orderby` | string | no       | `desc` (default, newest first) or `asc` |
| `limit`   | int    | no       | Max results, default 20, max 100        |
| `offset`  | int    | no       | Pagination offset                       |

```bash
curl "http://localhost:8080/wallet/g1jg8mtutu9khhfwc4nxmuhcpftf0pajdhfvsqf5"
curl "http://localhost:8080/wallet/0xf4ad3b02d44fa88371ef8faa232f789068b5f56b?orderby=asc&limit=50"
```

---

### GET `/history`

List all transfers regardless of address.

| Parameter | Type   | Required | Description                             |
|-----------|--------|----------|-----------------------------------------|
| `orderby` | string | no       | `desc` (default, newest first) or `asc` |
| `limit`   | int    | no       | Max results, default 20, max 100        |
| `offset`  | int    | no       | Pagination offset                       |

```bash
curl "http://localhost:8080/history?limit=50&orderby=asc"
```

**Response** (`/wallet` and `/history` share the same shape)

```json
{
  "data": [...],
  "limit": 20,
  "offset": 0
}
```

---

### GET `/summary`

Returns the total number of tracked transfers (`packet_send` + `packet_recv`).

```bash
curl http://localhost:8080/summary
```

```json
{
  "total": 1024
}
```
