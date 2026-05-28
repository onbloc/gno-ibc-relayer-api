package model

import "time"

type TransferStatus int

const (
	StatusDetected   TransferStatus = 0 // packet_send found in queue
	StatusProcessing TransferStatus = 1 // removed from queue, not yet done/failed
	StatusDone       TransferStatus = 2 // in done table
	StatusFailed     TransferStatus = 3 // in failed table
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
	TxHash           string `json:"tx_hash"`
	TimeoutTimestamp int64  `json:"timeout_timestamp"`

	Status    TransferStatus `json:"status"`
	CreatedAt time.Time      `json:"created_at"`
	DoneAt    *time.Time     `json:"done_at,omitempty"`
	ErrMsg    *string        `json:"err_msg,omitempty"`
}
