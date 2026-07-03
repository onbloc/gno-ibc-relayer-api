package db

import "time"

type TransferStatus int

const (
	StatusDetected   TransferStatus = 0
	StatusProcessing TransferStatus = 1
	StatusDone       TransferStatus = 2
	StatusFailed     TransferStatus = 3
)

type Transfer struct {
	ID         int64  `json:"id"`
	PacketHash string `json:"packet_hash"`

	SrcChainID   string `json:"src_chain_id"`
	DstChainID   string `json:"dst_chain_id"`
	SrcChannelID int    `json:"src_channel_id"`
	DstChannelID int    `json:"dst_channel_id"`

	FromAddress string `json:"from_address"`
	ToAddress   string `json:"to_address"`
	BaseToken   string `json:"base_token"`
	BaseAmount  string `json:"base_amount"`
	QuoteToken  string `json:"quote_token"`
	QuoteAmount string `json:"quote_amount"`

	Height           int64 `json:"height"`
	TimeoutTimestamp int64 `json:"timeout_timestamp"`

	Status    TransferStatus `json:"status"`
	CreatedAt time.Time      `json:"created_at"`
	DoneAt    *time.Time     `json:"done_at,omitempty"`
	ErrMsg    *string        `json:"err_msg,omitempty"`

	// TxOut is the source-chain send transaction hash.
	// TxIn is the destination-chain receive transaction hash, set once a packet_recv/write_ack is matched.
	TxOut string  `json:"tx_out"`
	TxIn  *string `json:"tx_in,omitempty"`
}
