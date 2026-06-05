package ethabi

import (
	"encoding/hex"
	"fmt"
	"math/big"
	"strings"
)

// Opcodes and versions mirror UCS03-ZKGM com.rs constants.
const (
	OpcodeForward    uint8 = 0x00
	OpcodeCall       uint8 = 0x01
	OpcodeBatch      uint8 = 0x02
	OpcodeTokenOrder uint8 = 0x03

	InstrVersion0 uint8 = 0x00
	InstrVersion1 uint8 = 0x01
	InstrVersion2 uint8 = 0x02
)

// ── schemas ──────────────────────────────────────────────────────────────────

var zkgmPacketSchema = Schema{
	Fields: []Field{
		{Type: TypeBytes32},                         // salt
		{Type: TypeUint256},                         // path
		{Type: TypeStruct, Sub: &instructionSchema}, // instruction (dynamic)
	},
}

var instructionSchema = Schema{
	Fields: []Field{
		{Type: TypeUint8},  // version
		{Type: TypeUint8},  // opcode
		{Type: TypeBytes},  // operand
	},
}

// TokenOrderV1: version=1, opcode=3
var tokenOrderV1Schema = Schema{
	Fields: []Field{
		{Type: TypeBytes},   // sender
		{Type: TypeBytes},   // receiver
		{Type: TypeBytes},   // base_token
		{Type: TypeUint256}, // base_amount
		{Type: TypeString},  // base_token_symbol
		{Type: TypeString},  // base_token_name
		{Type: TypeUint8},   // base_token_decimals
		{Type: TypeUint256}, // base_token_path
		{Type: TypeBytes},   // quote_token
		{Type: TypeUint256}, // quote_amount
	},
}

// TokenOrderV2: version=2, opcode=3
var tokenOrderV2Schema = Schema{
	Fields: []Field{
		{Type: TypeBytes},   // sender
		{Type: TypeBytes},   // receiver
		{Type: TypeBytes},   // base_token
		{Type: TypeUint256}, // base_amount
		{Type: TypeBytes},   // quote_token
		{Type: TypeUint256}, // quote_amount
		{Type: TypeUint8},   // kind
		{Type: TypeBytes},   // metadata
	},
}

// ── decoded types ────────────────────────────────────────────────────────────

type ZkgmPacket struct {
	Salt        [32]byte
	Path        *big.Int
	Instruction Instruction
}

type Instruction struct {
	Version uint8
	Opcode  uint8
	Operand []byte
}

type TokenOrder struct {
	Sender      []byte
	Receiver    []byte
	BaseToken   []byte
	BaseAmount  *big.Int
	QuoteToken  []byte
	QuoteAmount *big.Int
}

// ── public API ───────────────────────────────────────────────────────────────

// DecodeZkgmPacket hex-decodes and ABI-decodes a ZkgmPacket.
func DecodeZkgmPacket(hexData string) (*ZkgmPacket, error) {
	data, err := fromHex(hexData)
	if err != nil {
		return nil, fmt.Errorf("zkgm: hex decode: %w", err)
	}
	data = padTo32(data)

	vals, err := Decode(zkgmPacketSchema, data)
	if err != nil {
		return nil, fmt.Errorf("zkgm: decode packet: %w", err)
	}
	if len(vals) != 3 {
		return nil, fmt.Errorf("zkgm: expected 3 fields, got %d", len(vals))
	}

	salt, ok := vals[0].([32]byte)
	if !ok {
		return nil, fmt.Errorf("zkgm: salt type mismatch")
	}
	path, ok := vals[1].(*big.Int)
	if !ok {
		return nil, fmt.Errorf("zkgm: path type mismatch")
	}
	instrVals, ok := vals[2].([]any)
	if !ok {
		return nil, fmt.Errorf("zkgm: instruction type mismatch")
	}
	instr, err := parseInstruction(instrVals)
	if err != nil {
		return nil, err
	}

	return &ZkgmPacket{Salt: salt, Path: path, Instruction: *instr}, nil
}

// DecodeTokenOrder decodes the operand of a TOKEN_ORDER instruction.
// Returns an error if the instruction is not a token order.
func (p *ZkgmPacket) DecodeTokenOrder() (*TokenOrder, error) {
	if p.Instruction.Opcode != OpcodeTokenOrder {
		return nil, fmt.Errorf("zkgm: not a token order (opcode=%d)", p.Instruction.Opcode)
	}
	operand := padTo32(p.Instruction.Operand)
	switch p.Instruction.Version {
	case InstrVersion1:
		return decodeTokenOrderV1(operand)
	case InstrVersion2:
		return decodeTokenOrderV2(operand)
	default:
		return nil, fmt.Errorf("zkgm: unknown token order version %d", p.Instruction.Version)
	}
}

// ── helpers ──────────────────────────────────────────────────────────────────

func decodeTokenOrderV1(operand []byte) (*TokenOrder, error) {
	vals, err := Decode(tokenOrderV1Schema, operand)
	if err != nil {
		return nil, fmt.Errorf("zkgm: decode token order v1: %w", err)
	}
	return &TokenOrder{
		Sender:      asBytes(vals[0]),
		Receiver:    asBytes(vals[1]),
		BaseToken:   asBytes(vals[2]),
		BaseAmount:  asBigInt(vals[3]),
		QuoteToken:  asBytes(vals[8]),
		QuoteAmount: asBigInt(vals[9]),
	}, nil
}

func decodeTokenOrderV2(operand []byte) (*TokenOrder, error) {
	vals, err := Decode(tokenOrderV2Schema, operand)
	if err != nil {
		return nil, fmt.Errorf("zkgm: decode token order v2: %w", err)
	}
	return &TokenOrder{
		Sender:      asBytes(vals[0]),
		Receiver:    asBytes(vals[1]),
		BaseToken:   asBytes(vals[2]),
		BaseAmount:  asBigInt(vals[3]),
		QuoteToken:  asBytes(vals[4]),
		QuoteAmount: asBigInt(vals[5]),
	}, nil
}

func parseInstruction(vals []any) (*Instruction, error) {
	if len(vals) != 3 {
		return nil, fmt.Errorf("zkgm: instruction: expected 3 fields, got %d", len(vals))
	}
	version, ok := vals[0].(uint8)
	if !ok {
		return nil, fmt.Errorf("zkgm: instruction version type mismatch")
	}
	opcode, ok := vals[1].(uint8)
	if !ok {
		return nil, fmt.Errorf("zkgm: instruction opcode type mismatch")
	}
	operand, ok := vals[2].([]byte)
	if !ok {
		return nil, fmt.Errorf("zkgm: instruction operand type mismatch")
	}
	return &Instruction{Version: version, Opcode: opcode, Operand: operand}, nil
}

func fromHex(s string) ([]byte, error) {
	s = strings.TrimPrefix(s, "0x")
	return hex.DecodeString(s)
}

func padTo32(data []byte) []byte {
	if rem := len(data) % wordSize; rem != 0 {
		padded := make([]byte, len(data)+wordSize-rem)
		copy(padded, data)
		return padded
	}
	return data
}

func asBytes(v any) []byte {
	if v == nil {
		return nil
	}
	b, _ := v.([]byte)
	return b
}

func asBigInt(v any) *big.Int {
	if n, ok := v.(*big.Int); ok && n != nil {
		return n
	}
	return new(big.Int)
}
