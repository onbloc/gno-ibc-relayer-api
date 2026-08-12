package indexer

import (
	"encoding/base64"
	"encoding/binary"
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
	hexHash := hex.EncodeToString(rawBytes)                // "deadbeefcafe"
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

// buildFullEventQueueItem constructs a raw queue item JSON matching the evm
// event-source plugin's make_full_event shape, where packet fields are nested
// under event.@value.packet instead of flat under event.@value.
func buildFullEventQueueItem(plugin, eventType, txHash, packetHash string, srcChannelID, dstChannelID int, blockNumber int64) []byte {
	packet := map[string]any{
		"packet": map[string]any{
			"source_channel_id":      srcChannelID,
			"destination_channel_id": dstChannelID,
			"timeout_timestamp":      9999999,
		},
		"channel_id":  srcChannelID,
		"packet_hash": packetHash,
	}
	packetBytes, _ := json.Marshal(packet)

	chainEvent := map[string]any{
		"event": map[string]any{
			"@type":  eventType,
			"@value": json.RawMessage(packetBytes),
		},
		"tx_hash":      txHash,
		"block_number": blockNumber,
	}
	chainEventBytes, _ := json.Marshal(chainEvent)

	pluginBody := map[string]any{
		"plugin": plugin,
		"message": map[string]any{
			"@type":  "make_full_event",
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

func TestParse_MakeFullEvent_EvmOrigin(t *testing.T) {
	raw := buildFullEventQueueItem("voyager-event-source-plugin-evm/11155111", "packet_send", "0xabc", "0xpackethash", 28, 2, 12345)
	transfer, err := Parse(1, raw, time.Now(), testChains)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	if transfer == nil {
		t.Fatal("Parse returned nil transfer, expected evm-origin transfer to be created")
	}
	if transfer.SrcChainID != "11155111" || transfer.DstChainID != "dev" {
		t.Errorf("chains = %s->%s, want 11155111->dev", transfer.SrcChainID, transfer.DstChainID)
	}
	if transfer.PacketHash != "0xpackethash" {
		t.Errorf("PacketHash = %q, want 0xpackethash", transfer.PacketHash)
	}
	if transfer.Height != 12345 {
		t.Errorf("Height = %d, want 12345 (from block_number)", transfer.Height)
	}
	if transfer.TxOut != "0xabc" {
		t.Errorf("TxOut = %q, want 0xabc (evm hex, unchanged)", transfer.TxOut)
	}
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
			if transfer.TxOut != tc.wantTxHash {
				t.Errorf("TxOut = %q, want %q", transfer.TxOut, tc.wantTxHash)
			}
		})
	}
}

// ── chainFromPlugin ───────────────────────────────────────────────────────────

func TestChainFromPlugin(t *testing.T) {
	cases := []struct {
		plugin string
		want   string
	}{
		{"voyager-event-source-plugin-gno/dev", "dev"},
		{"voyager-event-source-plugin-evm/11155111", "11155111"},
		{"a/b/c", "c"},
		{"no-slash", ""},
		{"", ""},
	}
	for _, tc := range cases {
		got := chainFromPlugin(tc.plugin)
		if got != tc.want {
			t.Errorf("chainFromPlugin(%q) = %q, want %q", tc.plugin, got, tc.want)
		}
	}
}

// ── renderBytes ───────────────────────────────────────────────────────────────

func TestRenderBytes(t *testing.T) {
	evmAddr := []byte{0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67}
	cases := []struct {
		name  string
		input []byte
		want  string
	}{
		{"nil", nil, ""},
		{"empty", []byte{}, ""},
		{"ascii gno address", []byte("g1jg8mtutu9khhfwc4nxmuhcpftf0pajdhfvsqf5"), "g1jg8mtutu9khhfwc4nxmuhcpftf0pajdhfvsqf5"},
		{"null-terminated ascii", append([]byte("g1abc"), 0, 0), "g1abc"},
		{"evm address bytes", evmAddr, "0x" + hex.EncodeToString(evmAddr)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := renderBytes(tc.input)
			if got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

// ── findDstChain ──────────────────────────────────────────────────────────────

func TestFindDstChain(t *testing.T) {
	chains := []config.ChannelChain{
		{SrcChainID: "gno", DstChainID: "eth", SrcChannelID: 2, DstChannelID: 28},
		{SrcChainID: "eth", DstChainID: "gno", SrcChannelID: 28, DstChannelID: 2},
	}
	cases := []struct {
		srcChainID   string
		srcChannelID int
		want         string
	}{
		{"gno", 2, "eth"},
		{"eth", 28, "gno"},
		{"gno", 28, ""},
		{"unknown", 2, ""},
		{"gno", 99, ""},
	}
	for _, tc := range cases {
		got := findDstChain(chains, tc.srcChainID, tc.srcChannelID)
		if got != tc.want {
			t.Errorf("findDstChain(%q, %d) = %q, want %q", tc.srcChainID, tc.srcChannelID, got, tc.want)
		}
	}
}

// ── findChainsBySourceChannel ─────────────────────────────────────────────────

func TestFindChainsBySourceChannel(t *testing.T) {
	chains := []config.ChannelChain{
		{SrcChainID: "gno", DstChainID: "eth", SrcChannelID: 2, DstChannelID: 28},
		{SrcChainID: "eth", DstChainID: "gno", SrcChannelID: 28, DstChannelID: 2},
	}
	cases := []struct {
		srcChannelID int
		wantSrc      string
		wantDst      string
	}{
		{2, "gno", "eth"},
		{28, "eth", "gno"},
		{99, "", ""},
	}
	for _, tc := range cases {
		gotSrc, gotDst := findChainsBySourceChannel(chains, tc.srcChannelID)
		if gotSrc != tc.wantSrc || gotDst != tc.wantDst {
			t.Errorf("findChainsBySourceChannel(%d) = (%q, %q), want (%q, %q)",
				tc.srcChannelID, gotSrc, gotDst, tc.wantSrc, tc.wantDst)
		}
	}
}

// ── ParseItemFields ───────────────────────────────────────────────────────────

func buildPromiseItem(eventType string, timeoutTS int64, srcChannelID int) []byte {
	item := map[string]any{
		"@type": "promise",
		"@value": map[string]any{
			"receiver": map[string]any{
				"@value": map[string]any{
					"message": map[string]any{
						"@value": map[string]any{
							"batches": [][]map[string]any{
								{
									{
										"event": map[string]any{
											"@type": eventType,
											"@value": map[string]any{
												"packet": map[string]any{
													"timeout_timestamp": timeoutTS,
													"source_channel": map[string]any{
														"channel_id": srcChannelID,
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
	b, _ := json.Marshal(item)
	return b
}

type multicallEntry struct {
	eventType        string
	timeoutTimestamp int64
	srcChannelID     int
}

// buildSubmitMulticallItem constructs a raw done item JSON for a
// submit_multicall transaction-plugin item, which bundles one or more
// datagrams (e.g. packet_timeout) submitted in a single transaction.
func buildSubmitMulticallItem(plugin string, entries []multicallEntry) []byte {
	values := make([]map[string]any, len(entries))
	for i, e := range entries {
		values[i] = map[string]any{
			"@type": e.eventType,
			"@value": map[string]any{
				"proof": "0xdeadbeef",
				"packet": map[string]any{
					"data":                   "0xc73010ce",
					"timeout_height":         0,
					"source_channel_id":      e.srcChannelID,
					"timeout_timestamp":      e.timeoutTimestamp,
					"destination_channel_id": 1,
				},
				"proof_height": 13489,
			},
		}
	}

	pluginBody := map[string]any{
		"plugin": plugin,
		"message": map[string]any{
			"@type":  "submit_multicall",
			"@value": values,
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

// encodeAckHex builds a hex-encoded Ack{tag, inner_ack} payload matching the
// abi_encode_params layout used by both the gno and evm write_ack sources.
func encodeAckHex(tag uint64, innerAck []byte) string {
	word := func(v uint64) []byte {
		b := make([]byte, 32)
		binary.BigEndian.PutUint64(b[24:], v)
		return b
	}
	padTo32 := func(b []byte) []byte {
		if rem := len(b) % 32; rem != 0 {
			b = append(b, make([]byte, 32-rem)...)
		}
		return b
	}

	buf := append([]byte{}, word(tag)...)
	buf = append(buf, word(64)...) // offset to inner_ack tail
	buf = append(buf, word(uint64(len(innerAck)))...)
	buf = append(buf, padTo32(append([]byte{}, innerAck...))...)

	return "0x" + hex.EncodeToString(buf)
}

// buildWriteAckItem constructs a raw done item JSON for a write_ack event,
// which is how voyager records completion on direct (non-union) gno<->evm routes.
// ack is the hex-encoded acknowledgement payload; pass "" to omit it.
func buildWriteAckItem(plugin, packetHash, ack string, channelID int) []byte {
	ackVal := writeAckValue{
		ChannelID:       channelID,
		PacketHash:      packetHash,
		Acknowledgement: ack,
	}
	ackBytes, _ := json.Marshal(ackVal)

	chainEvent := map[string]any{
		"event": map[string]any{
			"@type":  "write_ack",
			"@value": json.RawMessage(ackBytes),
		},
		"height":  "100",
		"tx_hash": "0xdeadbeef",
	}
	chainEventBytes, _ := json.Marshal(chainEvent)

	pluginBody := map[string]any{
		"plugin": plugin,
		"message": map[string]any{
			"@type":  "make_full_event",
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

// buildGnoWriteAckItem constructs a raw done item JSON for a write_ack event
// as emitted by the gno event-source plugin, wrapped in make_chain_event.
func buildGnoWriteAckItem(plugin, packetHash, ack string, channelID int) []byte {
	ackVal := writeAckValue{
		ChannelID:       channelID,
		PacketHash:      packetHash,
		Acknowledgement: ack,
	}
	ackBytes, _ := json.Marshal(ackVal)

	chainEvent := map[string]any{
		"event": map[string]any{
			"@type":  "write_ack",
			"@value": json.RawMessage(ackBytes),
		},
		"height":  "100",
		"tx_hash": "deadbeef",
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

func TestParseItemFields(t *testing.T) {
	t.Run("nil on invalid JSON", func(t *testing.T) {
		if got := ParseItemFields([]byte("notjson")); got != nil {
			t.Errorf("expected nil, got %+v", got)
		}
	})
	t.Run("nil on unknown type", func(t *testing.T) {
		raw, _ := json.Marshal(map[string]any{"@type": "other", "@value": map[string]any{}})
		if got := ParseItemFields(raw); got != nil {
			t.Errorf("expected nil, got %+v", got)
		}
	})
	t.Run("call type packet_send returns fields", func(t *testing.T) {
		raw := buildQueueItem("voyager-event-source-plugin-gno/dev", "packet_send", "deadbeef", 2, 28)
		got := ParseItemFields(raw)
		if got == nil {
			t.Fatal("expected non-nil")
		}
		if got.EventType != "packet_send" {
			t.Errorf("EventType = %q, want packet_send", got.EventType)
		}
		if got.SrcChannelID != 2 {
			t.Errorf("SrcChannelID = %d, want 2", got.SrcChannelID)
		}
		if got.TimeoutTimestamp != 9999999 {
			t.Errorf("TimeoutTimestamp = %d, want 9999999", got.TimeoutTimestamp)
		}
		if got.PacketHash != "testhash" {
			t.Errorf("PacketHash = %q, want testhash", got.PacketHash)
		}
		wantB64 := base64.StdEncoding.EncodeToString([]byte{0xde, 0xad, 0xbe, 0xef})
		if got.TxHash != wantB64 {
			t.Errorf("TxHash = %q, want %q (gno tx hash base64-encoded)", got.TxHash, wantB64)
		}
	})
	t.Run("call type write_ack returns fields with packet hash", func(t *testing.T) {
		raw := buildWriteAckItem("voyager-event-source-plugin-evm/11155111", "0xabc123", "", 33)
		got := ParseItemFields(raw)
		if got == nil {
			t.Fatal("expected non-nil")
		}
		if got.EventType != "write_ack" {
			t.Errorf("EventType = %q, want write_ack", got.EventType)
		}
		if got.PacketHash != "0xabc123" {
			t.Errorf("PacketHash = %q, want 0xabc123", got.PacketHash)
		}
		if got.TxHash != "0xdeadbeef" {
			t.Errorf("TxHash = %q, want 0xdeadbeef (evm tx hash unchanged)", got.TxHash)
		}
		if !got.AckSuccess {
			t.Errorf("AckSuccess = false, want true (missing acknowledgement defaults to success)")
		}
	})
	t.Run("write_ack (evm/make_full_event) with TAG_ACK_SUCCESS", func(t *testing.T) {
		raw := buildWriteAckItem("voyager-event-source-plugin-evm/11155111", "0xabc123", encodeAckHex(1, nil), 33)
		got := ParseItemFields(raw)
		if got == nil {
			t.Fatal("expected non-nil")
		}
		if !got.AckSuccess {
			t.Errorf("AckSuccess = false, want true")
		}
		if got.AckError != "" {
			t.Errorf("AckError = %q, want empty", got.AckError)
		}
	})
	t.Run("write_ack (evm/make_full_event) with TAG_ACK_FAILURE", func(t *testing.T) {
		raw := buildWriteAckItem("voyager-event-source-plugin-evm/11155111", "0xabc123", encodeAckHex(0, []byte("insufficient funds")), 33)
		got := ParseItemFields(raw)
		if got == nil {
			t.Fatal("expected non-nil")
		}
		if got.AckSuccess {
			t.Errorf("AckSuccess = true, want false")
		}
		if got.AckError != "insufficient funds" {
			t.Errorf("AckError = %q, want %q", got.AckError, "insufficient funds")
		}
	})
	t.Run("write_ack with malformed acknowledgement defaults to success", func(t *testing.T) {
		raw := buildWriteAckItem("voyager-event-source-plugin-evm/11155111", "0xabc123", "0xnotvalidhex", 33)
		got := ParseItemFields(raw)
		if got == nil {
			t.Fatal("expected non-nil")
		}
		if !got.AckSuccess {
			t.Errorf("AckSuccess = false, want true (undecodable ack should not regress prior behavior)")
		}
		if got.AckError != "" {
			t.Errorf("AckError = %q, want empty", got.AckError)
		}
	})
	t.Run("call type write_ack via make_chain_event (gno) returns fields", func(t *testing.T) {
		raw := buildQueueItem("voyager-event-source-plugin-gno/dev", "write_ack", "deadbeef", 1, 33)
		got := ParseItemFields(raw)
		if got == nil {
			t.Fatal("expected non-nil")
		}
		if got.EventType != "write_ack" {
			t.Errorf("EventType = %q, want write_ack", got.EventType)
		}
		if got.PacketHash != "testhash" {
			t.Errorf("PacketHash = %q, want testhash", got.PacketHash)
		}
		wantB64 := base64.StdEncoding.EncodeToString([]byte{0xde, 0xad, 0xbe, 0xef})
		if got.TxHash != wantB64 {
			t.Errorf("TxHash = %q, want %q (gno tx hash base64-encoded)", got.TxHash, wantB64)
		}
		if !got.AckSuccess {
			t.Errorf("AckSuccess = false, want true (missing acknowledgement defaults to success)")
		}
	})
	t.Run("write_ack (gno/make_chain_event) with TAG_ACK_FAILURE", func(t *testing.T) {
		raw := buildGnoWriteAckItem("voyager-event-source-plugin-gno/dev", "testhash", encodeAckHex(0, []byte("timeout")), 1)
		got := ParseItemFields(raw)
		if got == nil {
			t.Fatal("expected non-nil")
		}
		if got.EventType != "write_ack" {
			t.Errorf("EventType = %q, want write_ack", got.EventType)
		}
		if got.AckSuccess {
			t.Errorf("AckSuccess = true, want false")
		}
		if got.AckError != "timeout" {
			t.Errorf("AckError = %q, want %q", got.AckError, "timeout")
		}
	})
	t.Run("call type packet_recv returns fields", func(t *testing.T) {
		raw := buildQueueItem("voyager-event-source-plugin-evm/11155111", "packet_recv", "deadbeef", 28, 2)
		got := ParseItemFields(raw)
		if got == nil {
			t.Fatal("expected non-nil")
		}
		if got.EventType != "packet_recv" {
			t.Errorf("EventType = %q, want packet_recv", got.EventType)
		}
		if got.TxHash != "deadbeef" {
			t.Errorf("TxHash = %q, want deadbeef (evm tx hash unchanged)", got.TxHash)
		}
	})
	t.Run("call type unknown event returns nil", func(t *testing.T) {
		raw := buildQueueItem("voyager-event-source-plugin-gno/dev", "packet_ack", "deadbeef", 2, 28)
		if got := ParseItemFields(raw); got != nil {
			t.Errorf("expected nil, got %+v", got)
		}
	})
	t.Run("promise type returns fields", func(t *testing.T) {
		raw := buildPromiseItem("packet_ack", 12345678, 5)
		got := ParseItemFields(raw)
		if got == nil {
			t.Fatal("expected non-nil")
		}
		if got.TimeoutTimestamp != 12345678 {
			t.Errorf("TimeoutTimestamp = %d, want 12345678", got.TimeoutTimestamp)
		}
		if got.SrcChannelID != 5 {
			t.Errorf("SrcChannelID = %d, want 5", got.SrcChannelID)
		}
	})
}

// ── ParsePacketTimeouts ───────────────────────────────────────────────────────

func TestParsePacketTimeouts(t *testing.T) {
	t.Run("single packet_timeout entry", func(t *testing.T) {
		raw := buildSubmitMulticallItem("voyager-transaction-plugin-evm/11155111", []multicallEntry{
			{eventType: "packet_timeout", timeoutTimestamp: 1784929190892000000, srcChannelID: 44},
		})
		got := ParsePacketTimeouts(raw)
		if len(got) != 1 {
			t.Fatalf("len(got) = %d, want 1", len(got))
		}
		if got[0].TimeoutTimestamp != 1784929190892000000 {
			t.Errorf("TimeoutTimestamp = %d, want 1784929190892000000", got[0].TimeoutTimestamp)
		}
		if got[0].SrcChannelID != 44 {
			t.Errorf("SrcChannelID = %d, want 44", got[0].SrcChannelID)
		}
	})

	t.Run("multiple packet_timeout entries batched in one multicall", func(t *testing.T) {
		raw := buildSubmitMulticallItem("voyager-transaction-plugin-evm/11155111", []multicallEntry{
			{eventType: "packet_timeout", timeoutTimestamp: 111, srcChannelID: 1},
			{eventType: "packet_timeout", timeoutTimestamp: 222, srcChannelID: 2},
		})
		got := ParsePacketTimeouts(raw)
		if len(got) != 2 {
			t.Fatalf("len(got) = %d, want 2", len(got))
		}
		if got[0].TimeoutTimestamp != 111 || got[0].SrcChannelID != 1 {
			t.Errorf("got[0] = %+v, want {111 1}", got[0])
		}
		if got[1].TimeoutTimestamp != 222 || got[1].SrcChannelID != 2 {
			t.Errorf("got[1] = %+v, want {222 2}", got[1])
		}
	})

	t.Run("ignores non-packet_timeout entries in the multicall", func(t *testing.T) {
		raw := buildSubmitMulticallItem("voyager-transaction-plugin-evm/11155111", []multicallEntry{
			{eventType: "packet_recv", timeoutTimestamp: 111, srcChannelID: 1},
			{eventType: "packet_timeout", timeoutTimestamp: 222, srcChannelID: 2},
		})
		got := ParsePacketTimeouts(raw)
		if len(got) != 1 {
			t.Fatalf("len(got) = %d, want 1", len(got))
		}
		if got[0].SrcChannelID != 2 {
			t.Errorf("SrcChannelID = %d, want 2", got[0].SrcChannelID)
		}
	})

	t.Run("nil for non-submit_multicall call items", func(t *testing.T) {
		raw := buildQueueItem("voyager-event-source-plugin-evm/11155111", "packet_send", "deadbeef", 2, 28)
		if got := ParsePacketTimeouts(raw); got != nil {
			t.Errorf("expected nil, got %+v", got)
		}
	})

	t.Run("nil on invalid JSON", func(t *testing.T) {
		if got := ParsePacketTimeouts([]byte("notjson")); got != nil {
			t.Errorf("expected nil, got %+v", got)
		}
	})
}

// ── Parse: nil-return cases ───────────────────────────────────────────────────

func TestParse_ReturnsNil(t *testing.T) {
	t.Run("non-call type", func(t *testing.T) {
		raw, _ := json.Marshal(map[string]any{"@type": "promise", "@value": map[string]any{}})
		got, err := Parse(1, raw, time.Now(), testChains)
		if err != nil || got != nil {
			t.Errorf("expected (nil, nil), got (%v, %v)", got, err)
		}
	})
	t.Run("call with non-plugin inner", func(t *testing.T) {
		raw, _ := json.Marshal(map[string]any{
			"@type": "call",
			"@value": map[string]any{
				"@type":  "other",
				"@value": map[string]any{},
			},
		})
		got, err := Parse(1, raw, time.Now(), testChains)
		if err != nil || got != nil {
			t.Errorf("expected (nil, nil), got (%v, %v)", got, err)
		}
	})
	t.Run("union relay packet not in chain map", func(t *testing.T) {
		raw := buildQueueItem("voyager-event-source-plugin-gno/dev", "packet_send", "deadbeef", 999, 28)
		got, err := Parse(1, raw, time.Now(), testChains)
		if err != nil || got != nil {
			t.Errorf("expected (nil, nil), got (%v, %v)", got, err)
		}
	})
}
