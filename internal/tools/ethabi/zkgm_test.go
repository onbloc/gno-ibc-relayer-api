package ethabi

import (
	"encoding/binary"
	"encoding/hex"
	"testing"
)

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
