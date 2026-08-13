package ethabi

import (
	"math/big"
	"testing"
)

// ── round-trip per type ──────────────────────────────────────────────────────

func TestDecode_Uint8(t *testing.T) {
	schema := Schema{Fields: []Field{{Type: TypeUint8}}}
	data := encodeTuple(schema.Fields, []any{uint8(200)})

	vals, err := Decode(schema, data)
	if err != nil {
		t.Fatalf("Decode() error: %v", err)
	}
	if vals[0].(uint8) != 200 {
		t.Errorf("Decode() = %v, want 200", vals[0])
	}
}

func TestDecode_Uint8_Overflow(t *testing.T) {
	schema := Schema{Fields: []Field{{Type: TypeUint8}}}
	data := word(256) // raw wire value wider than uint8 can express

	if _, err := Decode(schema, data); err == nil {
		t.Error("Decode() want error for uint8 overflow, got nil")
	}
}

func TestDecode_Uint32(t *testing.T) {
	schema := Schema{Fields: []Field{{Type: TypeUint32}}}
	data := encodeTuple(schema.Fields, []any{uint32(123456)})

	vals, err := Decode(schema, data)
	if err != nil {
		t.Fatalf("Decode() error: %v", err)
	}
	if vals[0].(uint32) != 123456 {
		t.Errorf("Decode() = %v, want 123456", vals[0])
	}
}

func TestDecode_Uint32_Overflow(t *testing.T) {
	schema := Schema{Fields: []Field{{Type: TypeUint32}}}
	data := encodeTuple([]Field{{Type: TypeUint64}}, []any{uint64(1) << 33})

	if _, err := Decode(schema, data); err == nil {
		t.Error("Decode() want error for uint32 overflow, got nil")
	}
}

func TestDecode_Uint64(t *testing.T) {
	schema := Schema{Fields: []Field{{Type: TypeUint64}}}
	data := encodeTuple(schema.Fields, []any{uint64(9999999999)})

	vals, err := Decode(schema, data)
	if err != nil {
		t.Fatalf("Decode() error: %v", err)
	}
	if vals[0].(uint64) != 9999999999 {
		t.Errorf("Decode() = %v, want 9999999999", vals[0])
	}
}

func TestDecode_Uint256(t *testing.T) {
	schema := Schema{Fields: []Field{{Type: TypeUint256}}}
	want := new(big.Int)
	want.SetString("123456789012345678901234567890", 10)
	data := encodeTuple(schema.Fields, []any{want})

	vals, err := Decode(schema, data)
	if err != nil {
		t.Fatalf("Decode() error: %v", err)
	}
	if vals[0].(*big.Int).Cmp(want) != 0 {
		t.Errorf("Decode() = %v, want %v", vals[0], want)
	}
}

func TestDecode_Bytes32(t *testing.T) {
	schema := Schema{Fields: []Field{{Type: TypeBytes32}}}
	var want [32]byte
	copy(want[:], "0123456789abcdef0123456789abcdef")
	data := encodeTuple(schema.Fields, []any{want})

	vals, err := Decode(schema, data)
	if err != nil {
		t.Fatalf("Decode() error: %v", err)
	}
	if vals[0].([32]byte) != want {
		t.Errorf("Decode() = %v, want %v", vals[0], want)
	}
}

func TestDecode_Bytes(t *testing.T) {
	schema := Schema{Fields: []Field{{Type: TypeBytes}}}
	want := []byte("hello world, this is longer than one word")
	data := encodeTuple(schema.Fields, []any{want})

	vals, err := Decode(schema, data)
	if err != nil {
		t.Fatalf("Decode() error: %v", err)
	}
	if string(vals[0].([]byte)) != string(want) {
		t.Errorf("Decode() = %q, want %q", vals[0], want)
	}
}

func TestDecode_String(t *testing.T) {
	schema := Schema{Fields: []Field{{Type: TypeString}}}
	data := encodeTuple(schema.Fields, []any{"hello"})

	vals, err := Decode(schema, data)
	if err != nil {
		t.Fatalf("Decode() error: %v", err)
	}
	if vals[0].(string) != "hello" {
		t.Errorf("Decode() = %q, want %q", vals[0], "hello")
	}
}

func TestDecode_Bool(t *testing.T) {
	schema := Schema{Fields: []Field{{Type: TypeBool}, {Type: TypeBool}}}
	data := encodeTuple(schema.Fields, []any{true, false})

	vals, err := Decode(schema, data)
	if err != nil {
		t.Fatalf("Decode() error: %v", err)
	}
	if vals[0].(bool) != true || vals[1].(bool) != false {
		t.Errorf("Decode() = %v, want [true false]", vals)
	}
}

func TestDecode_Bool_Invalid(t *testing.T) {
	schema := Schema{Fields: []Field{{Type: TypeBool}}}
	data := encodeTuple([]Field{{Type: TypeUint64}}, []any{uint64(2)}) // not 0 or 1

	if _, err := Decode(schema, data); err == nil {
		t.Error("Decode() want error for invalid bool, got nil")
	}
}

func TestDecode_Address(t *testing.T) {
	schema := Schema{Fields: []Field{{Type: TypeAddress}}}
	addr := make([]byte, 20)
	for i := range addr {
		addr[i] = byte(i + 1)
	}
	data := encodeTuple(schema.Fields, []any{addr})

	vals, err := Decode(schema, data)
	if err != nil {
		t.Fatalf("Decode() error: %v", err)
	}
	got := vals[0].([]byte)
	if len(got) != 20 || string(got) != string(addr) {
		t.Errorf("Decode() = %x, want %x", got, addr)
	}
}

func TestDecode_Address_NonZeroPadding(t *testing.T) {
	schema := Schema{Fields: []Field{{Type: TypeAddress}}}
	// Corrupt the zero-padding region (first 12 bytes of the word) that a real
	// address encoding always leaves zero.
	data := word(0)
	data[0] = 0xFF

	if _, err := Decode(schema, data); err == nil {
		t.Error("Decode() want error for non-zero address padding, got nil")
	}
}

// ── struct (static and dynamic) ──────────────────────────────────────────────

func TestDecode_StaticStruct(t *testing.T) {
	sub := Schema{Fields: []Field{{Type: TypeUint8}, {Type: TypeBool}}}
	schema := Schema{Fields: []Field{{Type: TypeStruct, Sub: &sub}}}
	data := encodeTuple(schema.Fields, []any{[]any{uint8(7), true}})

	vals, err := Decode(schema, data)
	if err != nil {
		t.Fatalf("Decode() error: %v", err)
	}
	inner := vals[0].([]any)
	if inner[0].(uint8) != 7 || inner[1].(bool) != true {
		t.Errorf("Decode() inner = %v, want [7 true]", inner)
	}
}

func TestDecode_DynamicStruct(t *testing.T) {
	sub := Schema{Fields: []Field{{Type: TypeUint8}, {Type: TypeBytes}}}
	schema := Schema{Fields: []Field{{Type: TypeStruct, Sub: &sub}}}
	data := encodeTuple(schema.Fields, []any{[]any{uint8(3), []byte("payload")}})

	vals, err := Decode(schema, data)
	if err != nil {
		t.Fatalf("Decode() error: %v", err)
	}
	inner := vals[0].([]any)
	if inner[0].(uint8) != 3 || string(inner[1].([]byte)) != "payload" {
		t.Errorf("Decode() inner = %v, want [3 payload]", inner)
	}
}

// ── array ─────────────────────────────────────────────────────────────────────

func TestDecode_StaticArray(t *testing.T) {
	schema := Schema{Fields: []Field{{Type: TypeArray, Elem: &Field{Type: TypeUint8}}}}
	data := encodeTuple(schema.Fields, []any{[]any{uint8(1), uint8(2), uint8(3)}})

	vals, err := Decode(schema, data)
	if err != nil {
		t.Fatalf("Decode() error: %v", err)
	}
	arr := vals[0].([]any)
	if len(arr) != 3 || arr[0].(uint8) != 1 || arr[2].(uint8) != 3 {
		t.Errorf("Decode() array = %v, want [1 2 3]", arr)
	}
}

func TestDecode_DynamicArray(t *testing.T) {
	schema := Schema{Fields: []Field{{Type: TypeArray, Elem: &Field{Type: TypeBytes}}}}
	data := encodeTuple(schema.Fields, []any{[]any{[]byte("a"), []byte("longer value here")}})

	vals, err := Decode(schema, data)
	if err != nil {
		t.Fatalf("Decode() error: %v", err)
	}
	arr := vals[0].([]any)
	if len(arr) != 2 || string(arr[0].([]byte)) != "a" || string(arr[1].([]byte)) != "longer value here" {
		t.Errorf("Decode() array = %v", arr)
	}
}

func TestDecode_Array_LengthExceedsData(t *testing.T) {
	schema := Schema{Fields: []Field{{Type: TypeArray, Elem: &Field{Type: TypeUint8}}}}
	data := append(word(1000), make([]byte, 32)...) // claims 1000 elements but data is far too short

	if _, err := Decode(schema, data); err == nil {
		t.Error("Decode() want error for array length exceeding data, got nil")
	}
}

// ── error paths ──────────────────────────────────────────────────────────────

func TestDecode_InvalidDataLength(t *testing.T) {
	schema := Schema{Fields: []Field{{Type: TypeUint8}}}
	if _, err := Decode(schema, []byte{1, 2, 3}); err == nil {
		t.Error("Decode() want error for non-word-aligned data, got nil")
	}
}

func TestDecode_TruncatedTuple(t *testing.T) {
	schema := Schema{Fields: []Field{{Type: TypeUint8}, {Type: TypeUint8}}}
	data := word(1) // only one word, but schema expects two

	if _, err := Decode(schema, data); err == nil {
		t.Error("Decode() want error for truncated tuple, got nil")
	}
}

func TestDecode_DynamicOffsetOutOfBounds(t *testing.T) {
	schema := Schema{Fields: []Field{{Type: TypeBytes}}}
	data := word(9999) // offset points far past the buffer

	if _, err := Decode(schema, data); err == nil {
		t.Error("Decode() want error for out-of-bounds offset, got nil")
	}
}

func TestDecode_DynamicOffsetMisaligned(t *testing.T) {
	schema := Schema{Fields: []Field{{Type: TypeBytes}}}
	data := append(word(5), make([]byte, 32)...) // offset not a multiple of 32

	if _, err := Decode(schema, data); err == nil {
		t.Error("Decode() want error for misaligned offset, got nil")
	}
}

func TestDecode_BytesLengthOverflowsBuffer(t *testing.T) {
	schema := Schema{Fields: []Field{{Type: TypeBytes}}}
	head := word(32)   // offset to tail
	tail := word(1000) // claims length 1000
	data := append(head, tail...)

	if _, err := Decode(schema, data); err == nil {
		t.Error("Decode() want error for bytes length exceeding buffer, got nil")
	}
}

func TestDecode_UnknownFieldType(t *testing.T) {
	schema := Schema{Fields: []Field{{Type: Type(999)}}}
	if _, err := Decode(schema, word(0)); err == nil {
		t.Error("Decode() want error for unknown field type, got nil")
	}
}

func TestDecode_StructWithNilSub(t *testing.T) {
	schema := Schema{Fields: []Field{{Type: TypeStruct}}} // Sub is nil
	if _, err := Decode(schema, word(0)); err == nil {
		t.Error("Decode() want error for struct with nil Sub, got nil")
	}
}

// ── decodeWordUint64 / roundUp32 ─────────────────────────────────────────────

func TestRoundUp32(t *testing.T) {
	cases := []struct{ in, want uint64 }{
		{0, 0}, {1, 32}, {31, 32}, {32, 32}, {33, 64}, {64, 64},
	}
	for _, tc := range cases {
		if got := roundUp32(tc.in); got != tc.want {
			t.Errorf("roundUp32(%d) = %d, want %d", tc.in, got, tc.want)
		}
	}
}
