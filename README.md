# gno-ibc-relayer-api

A lightweight Go API server that indexes IBC packet transfer events from the [Union](https://union.build) relayer's PostgreSQL database and exposes them via a REST API.

## Overview

The Union relayer processes cross-chain transfers through three tables (`queue`, `done`, `failed`). This server listens to real-time PostgreSQL `NOTIFY` events from those tables, decodes the ABI-encoded `ZkgmPacket` payload, and tracks each transfer's lifecycle in a `transfers` table.

**Supported chains:** gno ↔ Ethereum (union is an intermediate relay and is excluded)

**Event mapping per direction:**
- **gno→eth**: detected via `packet_send` on gno queue
- **eth→gno**: detected via `packet_recv` on gno queue

### Transfer status

| Value | Name       | Description                                                             |
|-------|------------|-------------------------------------------------------------------------|
| `0`   | detected   | `packet_send` or `packet_recv` found in relayer queue                   |
| `1`   | processing | item removed from queue, relay in progress                              |
| `2`   | done       | `packet_recv` confirmed on destination chain (bridge complete)          |
| `3`   | failed     | relay failed — `err_msg` contains the error from the relayer            |

### How status transitions work

```
queue INSERT  →  NOTIFY  →  detected (0)
queue DELETE  →  poll    →  processing (1)
done INSERT (packet_recv)  →  NOTIFY  →  done (2)
failed INSERT              →  NOTIFY  →  failed (3)  +  err_msg stored
```

Status `2 (done)` is set when a `packet_recv` event appears in the relayer's `done` table, matched by `timeout_timestamp` — confirming the packet was received on the destination chain, not just that relay was initiated.

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
poll_interval_sec = 5   # how often to poll for processing state transitions
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

**1. Initialize tables and install triggers (run once)**

```bash
make init
```

This runs both SQL migrations and installs `pg_notify` triggers on the relayer's `queue`, `done`, and `failed` tables.

**2. Build and run**

```bash
make run       # builds and starts in background, logs → indexer.log
make stop      # stop the running server
```

**Other commands**

```bash
make seed        # insert 100 dummy transfers (keeps existing data)
make seed-clean  # truncate transfers table then insert 100 dummy transfers
make drop        # drop all tables and remove pg_notify triggers
make tidy        # go mod tidy
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

**Response**

```json
{
  "data": [
    {
      "id": 106557627,
      "packet_hash": "0x3dd9478a...",
      "src_chain_id": "dev",
      "dst_chain_id": "11155111",
      "src_channel_id": 2,
      "dst_channel_id": 28,
      "from_address": "g1jg8mtutu9khhfwc4nxmuhcpftf0pajdhfvsqf5",
      "to_address": "0xb65aab34cc5a87b334551afc934630215c30ada0",
      "base_token": "ugnot",
      "base_amount": "1000000",
      "quote_token": "0x7fed1d819109fb7a095137bf867abe61db36c99c",
      "quote_amount": "1000000",
      "height": 105256,
      "tx_hash": "0xc69c3761...",
      "timeout_timestamp": 1780023916550000000,
      "status": 3,
      "created_at": "2026-05-28T03:05:23Z",
      "err_msg": "error in voyager-client-update-plugin-state-lens/state-lens/ics23/ics23: error in state/ibc-union/union-testnet-10: client `39` not found"
    }
  ],
  "limit": 20,
  "offset": 0
}
```

> `err_msg` is only present when `status` is `3` (failed).

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

---

### GET `/summary`

Returns the total number of tracked transfers.

```bash
curl http://localhost:8080/summary
```

```json
{
  "total": 1024
}
```
