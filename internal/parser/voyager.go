package parser

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/onbloc/gno-ibc-relayer-api/internal/config"
	"github.com/onbloc/gno-ibc-relayer-api/internal/ethabi"
	"github.com/onbloc/gno-ibc-relayer-api/internal/model"
)

// ── raw JSON types ────────────────────────────────────────────────────────────

type typedValue struct {
	Type  string          `json:"@type"`
	Value json.RawMessage `json:"@value"`
}

type pluginBody struct {
	Plugin  string     `json:"plugin"`
	Message typedValue `json:"message"`
}

type chainEventBody struct {
	Event  typedValue `json:"event"`
	Height string     `json:"height"`
	TxHash string     `json:"tx_hash"`
}

type packetSendValue struct {
	PacketData              string `json:"packet_data"`
	PacketHash              string `json:"packet_hash"`
	SourceChannelID         int    `json:"source_channel_id"`
	DestinationChannelID    int    `json:"destination_channel_id"`
	SourceConnectionID      int    `json:"source_connection_id"`
	DestinationConnectionID int    `json:"destination_connection_id"`
	TimeoutTimestamp        int64  `json:"timeout_timestamp"`
}

// ── public API ────────────────────────────────────────────────────────────────

// Parse converts a raw voyager item into a Transfer.
// Returns (nil, nil) for non-packet_send events and union relay packets.
func Parse(id int64, rawItem []byte, createdAt time.Time, chains []config.ChannelChain) (*model.Transfer, error) {
	var outer typedValue
	if err := json.Unmarshal(rawItem, &outer); err != nil {
		return nil, fmt.Errorf("parser: unmarshal: %w", err)
	}
	if outer.Type != "call" {
		return nil, nil
	}

	var callVal typedValue
	if err := json.Unmarshal(outer.Value, &callVal); err != nil {
		return nil, nil
	}
	if callVal.Type != "plugin" {
		return nil, nil
	}

	var body pluginBody
	if err := json.Unmarshal(callVal.Value, &body); err != nil {
		return nil, nil
	}
	if body.Message.Type != "make_chain_event" {
		return nil, nil
	}

	var chainEvent chainEventBody
	if err := json.Unmarshal(body.Message.Value, &chainEvent); err != nil {
		return nil, fmt.Errorf("parser: decode chain event: %w", err)
	}
	if chainEvent.Event.Type != "packet_send" {
		return nil, nil
	}

	// extract source chain from plugin name
	// e.g. "voyager-event-source-plugin-gno/dev" → "dev"
	srcChainID := chainFromPlugin(body.Plugin)
	if srcChainID == "" {
		return nil, nil
	}

	var ev packetSendValue
	if err := json.Unmarshal(chainEvent.Event.Value, &ev); err != nil {
		return nil, fmt.Errorf("parser: decode packet_send: %w", err)
	}

	// look up destination chain — returns "" for union relay packets
	dstChainID := findDstChain(chains, srcChainID, ev.SourceChannelID)
	if dstChainID == "" {
		return nil, nil
	}

	height, _ := strconv.ParseInt(chainEvent.Height, 10, 64)

	t := &model.Transfer{
		ID:               id,
		PacketHash:       ev.PacketHash,
		SrcChainID:       srcChainID,
		DstChainID:       dstChainID,
		SrcChannelID:     ev.SourceChannelID,
		DstChannelID:     ev.DestinationChannelID,
		Height:           height,
		TxHash:           chainEvent.TxHash,
		TimeoutTimestamp: ev.TimeoutTimestamp,
		Status:           model.StatusDetected,
		CreatedAt:        createdAt,
		RawItem:          rawItem,
	}

	if err := decodePacketData(t, ev.PacketData); err != nil {
		// non-fatal: store without decoded transfer fields
		_ = err
	}

	return t, nil
}

// ── internal ─────────────────────────────────────────────────────────────────

func decodePacketData(t *model.Transfer, hexData string) error {
	if hexData == "" {
		return nil
	}
	zkgm, err := ethabi.DecodeZkgmPacket(hexData)
	if err != nil {
		return err
	}
	if zkgm.Instruction.Opcode != ethabi.OpcodeTokenOrder {
		return nil
	}

	order, err := zkgm.DecodeTokenOrder()
	if err != nil {
		return err
	}

	t.FromAddress = renderBytes(order.Sender)
	t.ToAddress = renderBytes(order.Receiver)
	t.BaseToken = renderBytes(order.BaseToken)
	t.BaseAmount = order.BaseAmount.String()
	t.QuoteToken = renderBytes(order.QuoteToken)
	t.QuoteAmount = order.QuoteAmount.String()

	return nil
}

// chainFromPlugin extracts the chain ID from a voyager plugin name.
// "voyager-event-source-plugin-gno/dev" → "dev"
// "voyager-event-source-plugin-evm/11155111" → "11155111"
func chainFromPlugin(plugin string) string {
	idx := strings.LastIndex(plugin, "/")
	if idx < 0 {
		return ""
	}
	return plugin[idx+1:]
}

// findDstChain looks up the destination chain for a given source chain + channel.
func findDstChain(chains []config.ChannelChain, srcChainID string, srcChannelID int) string {
	for _, cc := range chains {
		if cc.SrcChainID == srcChainID && cc.SrcChannelID == srcChannelID {
			return cc.DstChainID
		}
	}
	return ""
}

// renderBytes renders bytes as a UTF-8 string (gno bech32) or 0x hex (EVM address).
func renderBytes(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	s := strings.TrimRight(string(b), "\x00")
	for _, r := range s {
		if r < 32 || r > 126 {
			return "0x" + hex.EncodeToString(b)
		}
	}
	return s
}
