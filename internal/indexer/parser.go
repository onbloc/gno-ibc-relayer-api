package indexer

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/onbloc/gno-ibc-relayer-api/internal/config"
	"github.com/onbloc/gno-ibc-relayer-api/internal/db"
	"github.com/onbloc/gno-ibc-relayer-api/internal/tools/ethabi"
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

// writeAckValue is the destination-chain ack event for a direct (non-union)
// gno<->evm route. Voyager records completion this way instead of packet_recv.
type writeAckValue struct {
	ChannelID  int    `json:"channel_id"`
	PacketHash string `json:"packet_hash"`
}

// ── public API ────────────────────────────────────────────────────────────────

// ItemFields holds key fields extracted from a Voyager item for matching transfers.
type ItemFields struct {
	EventType        string
	TimeoutTimestamp int64
	SrcChannelID     int
	PacketHash       string
}

// ParseItemFields extracts matching fields from:
//   - make_chain_event items (call type) — packet_send/packet_recv/write_ack (gno-origin ack), used in done table
//   - make_full_event items (call type) — write_ack (evm-origin ack), used in done table
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
		if err := json.Unmarshal(callVal.Value, &body); err != nil {
			return nil
		}

		if body.Message.Type == "make_full_event" {
			var chainEvent chainEventBody
			if err := json.Unmarshal(body.Message.Value, &chainEvent); err != nil || chainEvent.Event.Type != "write_ack" {
				return nil
			}
			var ack writeAckValue
			if err := json.Unmarshal(chainEvent.Event.Value, &ack); err != nil {
				return nil
			}
			return &ItemFields{
				EventType:  chainEvent.Event.Type,
				PacketHash: ack.PacketHash,
			}
		}

		if body.Message.Type != "make_chain_event" {
			return nil
		}
		var chainEvent chainEventBody
		if err := json.Unmarshal(body.Message.Value, &chainEvent); err != nil {
			return nil
		}
		if chainEvent.Event.Type != "packet_send" && chainEvent.Event.Type != "packet_recv" && chainEvent.Event.Type != "write_ack" {
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
			PacketHash:       ev.PacketHash,
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
func Parse(id int64, rawItem []byte, createdAt time.Time, chains []config.ChannelChain) (*db.Transfer, error) {
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
		return parsePacketRecv(id, body.Plugin, chainEvent, chains, createdAt)
	default:
		return nil, nil
	}
}

func parsePacketSend(id int64, plugin string, chainEvent chainEventBody, chains []config.ChannelChain, createdAt time.Time) (*db.Transfer, error) {
	srcChainID := chainFromPlugin(plugin)
	if srcChainID == "" {
		return nil, nil
	}

	var ev packetSendValue
	if err := json.Unmarshal(chainEvent.Event.Value, &ev); err != nil {
		return nil, fmt.Errorf("parser: decode packet_send: %w", err)
	}

	dstChainID := findDstChain(chains, srcChainID, ev.SourceChannelID)
	if dstChainID == "" {
		return nil, nil
	}

	return buildTransfer(id, ev, chainEvent, srcChainID, dstChainID, isGnoPlugin(plugin), createdAt)
}

func parsePacketRecv(id int64, plugin string, chainEvent chainEventBody, chains []config.ChannelChain, createdAt time.Time) (*db.Transfer, error) {
	var ev packetSendValue
	if err := json.Unmarshal(chainEvent.Event.Value, &ev); err != nil {
		return nil, fmt.Errorf("parser: decode packet_recv: %w", err)
	}

	srcChainID, dstChainID := findChainsBySourceChannel(chains, ev.SourceChannelID)
	if srcChainID == "" {
		return nil, nil
	}

	return buildTransfer(id, ev, chainEvent, srcChainID, dstChainID, isGnoPlugin(plugin), createdAt)
}

func buildTransfer(id int64, ev packetSendValue, chainEvent chainEventBody, srcChainID, dstChainID string, isGno bool, createdAt time.Time) (*db.Transfer, error) {
	height, _ := strconv.ParseInt(chainEvent.Height, 10, 64)

	t := &db.Transfer{
		ID:               id,
		PacketHash:       ev.PacketHash,
		SrcChainID:       srcChainID,
		DstChainID:       dstChainID,
		SrcChannelID:     ev.SourceChannelID,
		DstChannelID:     ev.DestinationChannelID,
		Height:           height,
		TxHash:           formatTxHash(chainEvent.TxHash, isGno),
		TimeoutTimestamp: ev.TimeoutTimestamp,
		Status:           db.StatusDetected,
		CreatedAt:        createdAt,
	}

	if err := decodePacketData(t, ev.PacketData); err != nil {
		_ = err
	}

	return t, nil
}

// ── internal ─────────────────────────────────────────────────────────────────

func decodePacketData(t *db.Transfer, hexData string) error {
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

func chainFromPlugin(plugin string) string {
	idx := strings.LastIndex(plugin, "/")
	if idx < 0 {
		return ""
	}
	return plugin[idx+1:]
}

func isGnoPlugin(plugin string) bool {
	return strings.Contains(plugin, "-gno/")
}

func formatTxHash(txHash string, isGno bool) string {
	if txHash == "" || !isGno {
		return txHash
	}
	raw := strings.TrimPrefix(txHash, "0x")
	b, err := hex.DecodeString(raw)
	if err != nil {
		return txHash
	}
	return base64.StdEncoding.EncodeToString(b)
}

func findDstChain(chains []config.ChannelChain, srcChainID string, srcChannelID int) string {
	for _, cc := range chains {
		if cc.SrcChainID == srcChainID && cc.SrcChannelID == srcChannelID {
			return cc.DstChainID
		}
	}
	return ""
}

func findChainsBySourceChannel(chains []config.ChannelChain, srcChannelID int) (srcChainID, dstChainID string) {
	for _, cc := range chains {
		if cc.SrcChannelID == srcChannelID {
			return cc.SrcChainID, cc.DstChainID
		}
	}
	return "", ""
}

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
