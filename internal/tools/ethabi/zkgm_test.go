package ethabi

import (
	"encoding/binary"
	"encoding/hex"
	"math/big"
	"testing"
)

// encodeZkgmHex builds a hex-encoded ZkgmPacket{salt, path, instruction} payload
// using the generic tuple encoder in encode_test.go.
func encodeZkgmHex(salt [32]byte, path *big.Int, version, opcode uint8, operand []byte) string {
	instr := []any{version, opcode, operand}
	data := encodeTuple(zkgmPacketSchema.Fields, []any{salt, path, instr})
	return "0x" + hex.EncodeToString(data)
}

// encodeAckHex builds a hex-encoded Ack{tag, inner_ack} payload matching the
// abi_encode_params layout used by both the gno and evm write_ack sources.
func encodeAckHex(tag uint64, innerAck []byte) string {
	word := func(v uint64) []byte {
		b := make([]byte, 32)
		binary.BigEndian.PutUint64(b[24:], v)
		return b
	}

	buf := append([]byte{}, word(tag)...)
	buf = append(buf, word(64)...) // offset to inner_ack tail
	buf = append(buf, word(uint64(len(innerAck)))...)
	buf = append(buf, padTo32(innerAck)...)

	return "0x" + hex.EncodeToString(buf)
}

func TestDecodeAck(t *testing.T) {
	t.Run("success tag with empty inner ack", func(t *testing.T) {
		ack, err := DecodeAck(encodeAckHex(1, nil))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !ack.Success() {
			t.Errorf("Success() = false, want true")
		}
	})

	t.Run("failure tag with error message inner ack", func(t *testing.T) {
		ack, err := DecodeAck(encodeAckHex(0, []byte("insufficient funds")))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if ack.Success() {
			t.Errorf("Success() = true, want false")
		}
		if got := string(ack.InnerAck); got != "insufficient funds" {
			t.Errorf("InnerAck = %q, want %q", got, "insufficient funds")
		}
	})

	t.Run("invalid hex returns error", func(t *testing.T) {
		if _, err := DecodeAck("0xzz"); err == nil {
			t.Error("expected error for invalid hex")
		}
	})
}

// ── DecodeZkgmPacket ─────────────────────────────────────────────────────────

func TestDecodeZkgmPacket(t *testing.T) {
	var salt [32]byte
	copy(salt[:], "test-salt-0123456789abcdef012345")
	path := big.NewInt(42)

	t.Run("forward opcode", func(t *testing.T) {
		hexData := encodeZkgmHex(salt, path, InstrVersion0, OpcodeForward, []byte("forward-operand"))

		pkt, err := DecodeZkgmPacket(hexData)
		if err != nil {
			t.Fatalf("DecodeZkgmPacket() error: %v", err)
		}
		if pkt.Salt != salt {
			t.Errorf("Salt = %x, want %x", pkt.Salt, salt)
		}
		if pkt.Path.Cmp(path) != 0 {
			t.Errorf("Path = %v, want %v", pkt.Path, path)
		}
		if pkt.Instruction.Opcode != OpcodeForward {
			t.Errorf("Opcode = %d, want %d", pkt.Instruction.Opcode, OpcodeForward)
		}
		if string(pkt.Instruction.Operand) != "forward-operand" {
			t.Errorf("Operand = %q, want %q", pkt.Instruction.Operand, "forward-operand")
		}
	})

	t.Run("invalid hex returns error", func(t *testing.T) {
		if _, err := DecodeZkgmPacket("0xzz"); err == nil {
			t.Error("DecodeZkgmPacket() want error for invalid hex, got nil")
		}
	})

	t.Run("truncated data returns error", func(t *testing.T) {
		if _, err := DecodeZkgmPacket("0x1234"); err == nil {
			t.Error("DecodeZkgmPacket() want error for truncated data, got nil")
		}
	})
}

// ── DecodeTokenOrder ─────────────────────────────────────────────────────────

func TestDecodeTokenOrder_V1(t *testing.T) {
	var salt [32]byte
	operand := encodeTuple(tokenOrderV1Schema.Fields, []any{
		[]byte("sender-addr"),
		[]byte("receiver-addr"),
		[]byte("base-token"),
		big.NewInt(1000),
		"SYM",
		"Token Name",
		uint8(18),
		big.NewInt(0),
		[]byte("quote-token"),
		big.NewInt(2000),
	})
	hexData := encodeZkgmHex(salt, big.NewInt(0), InstrVersion1, OpcodeTokenOrder, operand)

	pkt, err := DecodeZkgmPacket(hexData)
	if err != nil {
		t.Fatalf("DecodeZkgmPacket() error: %v", err)
	}
	order, err := pkt.DecodeTokenOrder()
	if err != nil {
		t.Fatalf("DecodeTokenOrder() error: %v", err)
	}
	if string(order.Sender) != "sender-addr" {
		t.Errorf("Sender = %q, want %q", order.Sender, "sender-addr")
	}
	if string(order.Receiver) != "receiver-addr" {
		t.Errorf("Receiver = %q, want %q", order.Receiver, "receiver-addr")
	}
	if string(order.BaseToken) != "base-token" {
		t.Errorf("BaseToken = %q, want %q", order.BaseToken, "base-token")
	}
	if order.BaseAmount.Cmp(big.NewInt(1000)) != 0 {
		t.Errorf("BaseAmount = %v, want 1000", order.BaseAmount)
	}
	if string(order.QuoteToken) != "quote-token" {
		t.Errorf("QuoteToken = %q, want %q", order.QuoteToken, "quote-token")
	}
	if order.QuoteAmount.Cmp(big.NewInt(2000)) != 0 {
		t.Errorf("QuoteAmount = %v, want 2000", order.QuoteAmount)
	}
}

func TestDecodeTokenOrder_V2(t *testing.T) {
	var salt [32]byte
	operand := encodeTuple(tokenOrderV2Schema.Fields, []any{
		[]byte("sender-addr"),
		[]byte("receiver-addr"),
		[]byte("base-token"),
		big.NewInt(1000),
		[]byte("quote-token"),
		big.NewInt(2000),
		uint8(1),
		[]byte("metadata"),
	})
	hexData := encodeZkgmHex(salt, big.NewInt(0), InstrVersion2, OpcodeTokenOrder, operand)

	pkt, err := DecodeZkgmPacket(hexData)
	if err != nil {
		t.Fatalf("DecodeZkgmPacket() error: %v", err)
	}
	order, err := pkt.DecodeTokenOrder()
	if err != nil {
		t.Fatalf("DecodeTokenOrder() error: %v", err)
	}
	if string(order.Sender) != "sender-addr" || string(order.QuoteToken) != "quote-token" {
		t.Errorf("order = %+v", order)
	}
	if order.BaseAmount.Cmp(big.NewInt(1000)) != 0 || order.QuoteAmount.Cmp(big.NewInt(2000)) != 0 {
		t.Errorf("order amounts = %v / %v", order.BaseAmount, order.QuoteAmount)
	}
}

func TestDecodeTokenOrder_WrongOpcode(t *testing.T) {
	var salt [32]byte
	hexData := encodeZkgmHex(salt, big.NewInt(0), InstrVersion1, OpcodeForward, []byte("not-a-token-order"))

	pkt, err := DecodeZkgmPacket(hexData)
	if err != nil {
		t.Fatalf("DecodeZkgmPacket() error: %v", err)
	}
	if _, err := pkt.DecodeTokenOrder(); err == nil {
		t.Error("DecodeTokenOrder() want error for non-token-order opcode, got nil")
	}
}

func TestDecodeTokenOrder_UnknownVersion(t *testing.T) {
	var salt [32]byte
	hexData := encodeZkgmHex(salt, big.NewInt(0), 99, OpcodeTokenOrder, []byte("operand"))

	pkt, err := DecodeZkgmPacket(hexData)
	if err != nil {
		t.Fatalf("DecodeZkgmPacket() error: %v", err)
	}
	if _, err := pkt.DecodeTokenOrder(); err == nil {
		t.Error("DecodeTokenOrder() want error for unknown version, got nil")
	}
}
