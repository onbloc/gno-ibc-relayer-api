CREATE TABLE IF NOT EXISTS transfers (
    id                  BIGINT PRIMARY KEY,      -- relayer queue.id (sequence)
    packet_hash         TEXT        NOT NULL,

    -- chain (gno <> eth only, union excluded)
    src_chain_id        TEXT        NOT NULL,
    dst_chain_id        TEXT        NOT NULL,
    src_channel_id      INT,
    dst_channel_id      INT,

    -- decoded from ZkgmPacket TOKEN_ORDER
    from_address        TEXT,
    to_address          TEXT,
    base_token          TEXT,
    base_amount         TEXT,
    quote_token         TEXT,
    quote_amount        TEXT,

    -- tx info
    height              BIGINT,
    tx_hash             TEXT,
    timeout_timestamp   BIGINT,

    -- 0=detected, 1=processing, 2=done, 3=failed
    status              INT         NOT NULL DEFAULT 0,

    created_at          TIMESTAMPTZ NOT NULL,
    done_at             TIMESTAMPTZ,

    raw_item            JSONB       NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_transfers_status       ON transfers (status);
CREATE INDEX IF NOT EXISTS idx_transfers_packet_hash  ON transfers (packet_hash);
CREATE INDEX IF NOT EXISTS idx_transfers_src_chain    ON transfers (src_chain_id);
CREATE INDEX IF NOT EXISTS idx_transfers_dst_chain    ON transfers (dst_chain_id);
CREATE INDEX IF NOT EXISTS idx_transfers_from_address ON transfers (from_address);
CREATE INDEX IF NOT EXISTS idx_transfers_to_address   ON transfers (to_address);
CREATE INDEX IF NOT EXISTS idx_transfers_created_at   ON transfers (created_at DESC);

CREATE TABLE IF NOT EXISTS indexer_cursors (
    name    TEXT   PRIMARY KEY,
    last_id BIGINT NOT NULL DEFAULT 0
);
