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

// ItemFields holds key fields extracted from a Voyager item for matching transfers.
type ItemFields struct {
	EventType        string // "packet_send" or "packet_recv"
	TimeoutTimestamp int64
	SrcChannelID     int
}

// ParseItemFields extracts matching fields from:
//   - make_chain_event items (call type) — used in done table
//   - promise items with batches — used in failed table
//
// Returns nil for irrelevant item types.
func ParseItemFields(raw []byte) *ItemFields {
	var outer typedValue
	if err := json.Unmarshal(raw, &outer); err != nil {
		return nil
	}

	switch outer.Type {
	case "call":
		var callVal typedValue
		if err := json.Unmarshal(outer.Value, &callVal); err != nil || callVal.Type != "plugin" {
			return nil
		}
		var body pluginBody
		if err := json.Unmarshal(callVal.Value, &body); err != nil || body.Message.Type != "make_chain_event" {
			return nil
		}
		var chainEvent chainEventBody
		if err := json.Unmarshal(body.Message.Value, &chainEvent); err != nil {
			return nil
		}
		if chainEvent.Event.Type != "packet_send" && chainEvent.Event.Type != "packet_recv" {
			return nil
		}
		var ev packetSendValue
		if err := json.Unmarshal(chainEvent.Event.Value, &ev); err != nil {
			return nil
		}
		return &ItemFields{
			EventType:        chainEvent.Event.Type,
			TimeoutTimestamp: ev.TimeoutTimestamp,
			SrcChannelID:     ev.SourceChannelID,
		}

	case "promise":
		var promise struct {
			Receiver struct {
				Value struct {
					Message struct {
						Value struct {
							Batches [][]struct {
								Event struct {
									Type  string `json:"@type"`
									Value struct {
										Packet struct {
											TimeoutTimestamp int64 `json:"timeout_timestamp"`
											SourceChannel    struct {
												ChannelID int `json:"channel_id"`
											} `json:"source_channel"`
										} `json:"packet"`
									} `json:"@value"`
								} `json:"event"`
							} `json:"batches"`
						} `json:"@value"`
					} `json:"message"`
				} `json:"@value"`
			} `json:"receiver"`
		}
		if err := json.Unmarshal(outer.Value, &promise); err != nil {
			return nil
		}
		for _, batch := range promise.Receiver.Value.Message.Value.Batches {
			for _, entry := range batch {
				ts := entry.Event.Value.Packet.TimeoutTimestamp
				ch := entry.Event.Value.Packet.SourceChannel.ChannelID
				if ts != 0 {
					return &ItemFields{
						EventType:        entry.Event.Type,
						TimeoutTimestamp: ts,
						SrcChannelID:     ch,
					}
				}
			}
		}
	}
	return nil
}

// Parse converts a raw voyager item into a Transfer.
// Returns (nil, nil) for irrelevant events and union relay packets.
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

	switch chainEvent.Event.Type {
	case "packet_send":
		return parsePacketSend(id, body.Plugin, chainEvent, chains, createdAt)
	case "packet_recv":
		return parsePacketRecv(id, chainEvent, chains, createdAt)
	default:
		return nil, nil
	}
}

// parsePacketSend handles gno→eth direction: packet_send from the source chain.
func parsePacketSend(id int64, plugin string, chainEvent chainEventBody, chains []config.ChannelChain, createdAt time.Time) (*model.Transfer, error) {
	// extract source chain from plugin name
	// e.g. "voyager-event-source-plugin-gno/dev" → "dev"
	srcChainID := chainFromPlugin(plugin)
	if srcChainID == "" {
		return nil, nil
	}

	var ev packetSendValue
	if err := json.Unmarshal(chainEvent.Event.Value, &ev); err != nil {
		return nil, fmt.Errorf("parser: decode packet_send: %w", err)
	}

	// returns "" for union relay packets (not in our channel map)
	dstChainID := findDstChain(chains, srcChainID, ev.SourceChannelID)
	if dstChainID == "" {
		return nil, nil
	}

	return buildTransfer(id, ev, chainEvent, srcChainID, dstChainID, createdAt)
}

// parsePacketRecv handles eth→gno direction: packet_recv on the destination chain.
// The source chain is identified by source_channel_id in the event (e.g. 28 → eth).
func parsePacketRecv(id int64, chainEvent chainEventBody, chains []config.ChannelChain, createdAt time.Time) (*model.Transfer, error) {
	var ev packetSendValue
	if err := json.Unmarshal(chainEvent.Event.Value, &ev); err != nil {
		return nil, fmt.Errorf("parser: decode packet_recv: %w", err)
	}

	srcChainID, dstChainID := findChainsBySourceChannel(chains, ev.SourceChannelID)
	if srcChainID == "" {
		return nil, nil
	}

	return buildTransfer(id, ev, chainEvent, srcChainID, dstChainID, createdAt)
}

func buildTransfer(id int64, ev packetSendValue, chainEvent chainEventBody, srcChainID, dstChainID string, createdAt time.Time) (*model.Transfer, error) {
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

// findChainsBySourceChannel looks up src/dst chain IDs by source channel ID alone.
// Used for packet_recv events where the plugin chain is the destination, not the source.
func findChainsBySourceChannel(chains []config.ChannelChain, srcChannelID int) (srcChainID, dstChainID string) {
	for _, cc := range chains {
		if cc.SrcChannelID == srcChannelID {
			return cc.SrcChainID, cc.DstChainID
		}
	}
	return "", ""
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
