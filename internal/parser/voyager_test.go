package parser

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"

	"github.com/onbloc/gno-ibc-relayer-api/internal/config"
)

// ── isGnoPlugin ───────────────────────────────────────────────────────────────

func TestIsGnoPlugin(t *testing.T) {
	cases := []struct {
		plugin string
		want   bool
	}{
		{"voyager-event-source-plugin-gno/dev", true},
		{"voyager-event-source-plugin-gno/staging", true},
		{"voyager-event-source-plugin-evm/11155111", false},
		{"voyager-event-source-plugin-evm/1", false},
		{"", false},
	}
	for _, tc := range cases {
		if got := isGnoPlugin(tc.plugin); got != tc.want {
			t.Errorf("isGnoPlugin(%q) = %v, want %v", tc.plugin, got, tc.want)
		}
	}
}

// ── formatTxHash ──────────────────────────────────────────────────────────────

func TestFormatTxHash(t *testing.T) {
	rawBytes := []byte{0xde, 0xad, 0xbe, 0xef, 0xca, 0xfe}
	hexHash := hex.EncodeToString(rawBytes)            // "deadbeefcafe"
	b64Hash := base64.StdEncoding.EncodeToString(rawBytes) // "3q2+78r+"

	cases := []struct {
		name   string
		txHash string
		isGno  bool
		want   string
	}{
		{
			name:   "gno: hex converted to base64",
			txHash: hexHash,
			isGno:  true,
			want:   b64Hash,
		},
		{
			name:   "gno: hex with 0x prefix converted to base64",
			txHash: "0x" + hexHash,
			isGno:  true,
			want:   b64Hash,
		},
		{
			name:   "evm: hex returned as-is",
			txHash: "0x" + hexHash,
			isGno:  false,
			want:   "0x" + hexHash,
		},
		{
			name:   "evm: hex without prefix returned as-is",
			txHash: hexHash,
			isGno:  false,
			want:   hexHash,
		},
		{
			name:   "empty hash returned as-is regardless of chain",
			txHash: "",
			isGno:  true,
			want:   "",
		},
		{
			name:   "invalid hex for gno returns original value",
			txHash: "not-hex",
			isGno:  true,
			want:   "not-hex",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := formatTxHash(tc.txHash, tc.isGno)
			if got != tc.want {
				t.Errorf("formatTxHash(%q, %v) = %q, want %q", tc.txHash, tc.isGno, got, tc.want)
			}
		})
	}
}

// ── Parse: tx_hash encoding end-to-end ───────────────────────────────────────

var testChains = []config.ChannelChain{
	{SrcChainID: "dev", DstChainID: "11155111", SrcChannelID: 2, DstChannelID: 28},
	{SrcChainID: "11155111", DstChainID: "dev", SrcChannelID: 28, DstChannelID: 2},
}

// buildQueueItem constructs a raw queue item JSON matching the voyager format.
func buildQueueItem(plugin, eventType, txHash string, srcChannelID, dstChannelID int) []byte {
	packetData := packetSendValue{
		PacketHash:           "testhash",
		SourceChannelID:      srcChannelID,
		DestinationChannelID: dstChannelID,
		TimeoutTimestamp:     9999999,
	}
	packetDataBytes, _ := json.Marshal(packetData)

	chainEvent := map[string]any{
		"event": map[string]any{
			"@type":  eventType,
			"@value": json.RawMessage(packetDataBytes),
		},
		"height":  "100",
		"tx_hash": txHash,
	}
	chainEventBytes, _ := json.Marshal(chainEvent)

	pluginBody := map[string]any{
		"plugin": plugin,
		"message": map[string]any{
			"@type":  "make_chain_event",
			"@value": json.RawMessage(chainEventBytes),
		},
	}
	pluginBodyBytes, _ := json.Marshal(pluginBody)

	item := map[string]any{
		"@type": "call",
		"@value": map[string]any{
			"@type":  "plugin",
			"@value": json.RawMessage(pluginBodyBytes),
		},
	}
	b, _ := json.Marshal(item)
	return b
}

func TestParse_TxHashEncoding(t *testing.T) {
	rawBytes := []byte{0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89}
	hexHash := hex.EncodeToString(rawBytes)
	b64Hash := base64.StdEncoding.EncodeToString(rawBytes)

	cases := []struct {
		name       string
		plugin     string
		eventType  string
		srcChannel int
		dstChannel int
		wantTxHash string
	}{
		{
			name:       "gno packet_send stores base64",
			plugin:     "voyager-event-source-plugin-gno/dev",
			eventType:  "packet_send",
			srcChannel: 2,
			dstChannel: 28,
			wantTxHash: b64Hash,
		},
		{
			name:       "evm packet_send stores hex",
			plugin:     "voyager-event-source-plugin-evm/11155111",
			eventType:  "packet_send",
			srcChannel: 28,
			dstChannel: 2,
			wantTxHash: hexHash,
		},
		{
			name:       "gno packet_recv stores base64",
			plugin:     "voyager-event-source-plugin-gno/dev",
			eventType:  "packet_recv",
			srcChannel: 28,
			dstChannel: 2,
			wantTxHash: b64Hash,
		},
		{
			name:       "evm packet_recv stores hex",
			plugin:     "voyager-event-source-plugin-evm/11155111",
			eventType:  "packet_recv",
			srcChannel: 2,
			dstChannel: 28,
			wantTxHash: hexHash,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw := buildQueueItem(tc.plugin, tc.eventType, hexHash, tc.srcChannel, tc.dstChannel)
			transfer, err := Parse(1, raw, time.Now(), testChains)
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}
			if transfer == nil {
				t.Fatal("Parse returned nil transfer")
			}
			if transfer.TxHash != tc.wantTxHash {
				t.Errorf("TxHash = %q, want %q", transfer.TxHash, tc.wantTxHash)
			}
		})
	}
}
