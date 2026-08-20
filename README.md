# gno-ibc-relayer-api

A lightweight Go API server that indexes IBC packet transfer events from the [Union](https://union.build) relayer's PostgreSQL database and exposes them via a REST API.

## Overview

The Union relayer processes cross-chain transfers through three tables (`queue`, `done`, `failed`). This server listens to real-time PostgreSQL `NOTIFY` events from those tables, decodes the ABI-encoded `ZkgmPacket` payload, and tracks each transfer's lifecycle in a `transfers` table.

**Supported chains:** gno ↔ Ethereum (union is an intermediate relay and is excluded)

**Event mapping per direction:**
- **gno→eth**: detected via `packet_send` on the gno event-source plugin (`make_chain_event`)
- **eth→gno**: detected via `packet_send` on the evm event-source plugin (`make_full_event`, packet fields nested under `event.packet`)

Both directions signal completion via a `write_ack` event in the relayer's `done` table — gno emits it wrapped in `make_chain_event`, evm wraps it in `make_full_event`. A `packet_recv` event (present in both directions, though its payload shape varies) carries the destination-chain receive transaction hash but does not by itself mark the transfer done.

A `write_ack` event's `acknowledgement` field is the ABI-encoded UCS03-ZKGM `Ack{tag, inner_ack}` payload (identical encoding on gno and evm): `tag == TAG_ACK_SUCCESS` (non-zero) means the packet was actually processed successfully on the destination chain, while `tag == TAG_ACK_FAILURE` (zero) means the destination chain rejected the packet — a business-logic failure, not a relay failure, but still an unsuccessful transfer. The indexer decodes this tag and routes the transfer to `done` or `failed` accordingly (an acknowledgement that can't be decoded, or is missing, defaults to success). This is separate from the `failed` table, which only covers relay-level failures (e.g. timeouts).

If the destination chain instead panics/reverts on receipt, the relayer refunds the sender by submitting a `packet_timeout` datagram on the *source* chain — recorded in the relayer's `done` table as a `submit_multicall` transaction-plugin item (one or more datagrams bundled in a single tx), not a `make_chain_event`/`make_full_event` chain-observed event. It carries no `packet_hash`, so it's matched the same way as `failed`-table promise items: by `timeout_timestamp` + `source_channel_id`. The indexer marks the matched transfer `3 (failed)`.

### Transfer status

| Value | Name       | Description                                                    |
|-------|------------|------------------------------------------------------------------|
| `0`   | detected   | `packet_send` found in relayer queue                              |
| `1`   | processing | item removed from queue, relay in progress                        |
| `2`   | done       | `write_ack` confirmed on destination chain with `TAG_ACK_SUCCESS`  |
| `3`   | failed     | relay failed, `write_ack` confirmed with `TAG_ACK_FAILURE`, or refunded via `packet_timeout` — `err_msg` contains the relayer error or decoded failure reason |

### How status transitions work

```
queue INSERT  →  NOTIFY  →  detected (0)
queue DELETE  →  poll    →  processing (1)
done INSERT (packet_recv)          →  NOTIFY  →  tx_in populated (status unchanged)
done INSERT (write_ack, success)   →  NOTIFY  →  done (2)
done INSERT (write_ack, ack error) →  NOTIFY  →  failed (3)  +  err_msg stored (+ tx_in fallback)
done INSERT (packet_timeout)       →  NOTIFY  →  failed (3)  +  err_msg stored
failed INSERT                      →  NOTIFY  →  failed (3)  +  err_msg stored
```

Status `2 (done)` is set when a `write_ack` event appears in the relayer's `done` table with a successful ack tag, matched by `packet_hash` — confirming the packet was received *and acknowledged* on the destination chain, not just that relay was initiated. If the ack tag instead decodes to `TAG_ACK_FAILURE`, the transfer is marked `3 (failed)` with the decoded inner ack (when printable) recorded in `err_msg`. A separate `packet_recv` event (also matched by `packet_hash`) fills in `tx_in` with the destination-chain receive transaction hash; if `packet_recv` is missed or never emitted (as on evm, where receive+ack happen in the same transaction), the triggering event's own transaction hash is used as a fallback — this applies both when `write_ack` succeeds and when it decodes to `TAG_ACK_FAILURE`, so `tx_in` is populated consistently regardless of outcome. `packet_timeout` and relay-error `failed` rows carry no destination-chain tx hash, so they leave `tx_in` to whatever a prior `packet_recv` may have already set.

A `packet_timeout` datagram (source-chain refund after a destination-chain panic) is matched by `timeout_timestamp` + `source_channel_id` instead, since it carries no `packet_hash`; a single `submit_multicall` row can bundle refunds for several packets at once, and each is matched independently.

## Project Structure

```
cmd/
  server/          # entrypoint: starts indexer + HTTP server
  seed/            # dev tool: insert dummy transfers
  setup-trigger/   # one-time: install pg_notify triggers
internal/
  config/          # config file parsing (TOML)
  db/              # Transfer model, Store (queries), connection pool
  indexer/         # event listener, queue sync, voyager parser
  server/          # HTTP handlers and router
  tools/ethabi/    # ABI decoder for ZkgmPacket (ported from gno.land)
```

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
make run    # builds and starts in background, logs → indexer.log
```

**Other commands**

```bash
make test        # run all unit tests
make seed        # insert 100 dummy transfers (keeps existing data)
make seed-clean  # truncate transfers table then insert 100 dummy transfers
make drop        # drop all tables and remove pg_notify triggers
make tidy        # go mod tidy
make help        # show all available commands
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
  "timeout_timestamp": 1779859590954000000,
  "status": 2,
  "created_at": "2026-05-26T05:28:50Z",
  "done_at": "2026-05-26T05:29:12Z",
  "tx_out": "0x3966e3f3...",
  "tx_in": "0x8a12ff01..."
}
```

> `tx_out` is the source-chain send transaction, always present. `tx_in` is the destination-chain receive transaction — omitted from the response until a `packet_recv`/`write_ack` event fills it in.

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
      "timeout_timestamp": 1780023916550000000,
      "status": 3,
      "created_at": "2026-05-28T03:05:23Z",
      "err_msg": "error in voyager-client-update-plugin-state-lens/state-lens/ics23/ics23: error in state/ibc-union/union-testnet-10: client `39` not found",
      "tx_out": "0xc69c3761..."
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

---

### GET `/summary/recent`

Returns a per-status breakdown over the most recently created transfers. Result is cached in-process for 5s to avoid re-running the aggregate query on every poll.

| Parameter | Type | Required | Description                                  |
|-----------|------|----------|-----------------------------------------------|
| `limit`   | int  | no       | Number of most recent transfers to bucket, default 1000, max 5000 |

```bash
curl "http://localhost:8080/summary/recent?limit=1000"
```

```json
{
  "total": 1000,
  "detected": 10,
  "processing": 20,
  "succeeded": 900,
  "failed": 70
}
```
