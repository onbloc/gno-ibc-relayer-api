package ethabi

import (
	"encoding/binary"
	"math/big"
)

// This file provides a minimal hand-rolled ABI encoder, the mirror image of
// decode.go, so tests can build valid fixtures for arbitrary schemas without
// depending on an external ABI library.

func word(v uint64) []byte {
	b := make([]byte, wordSize)
	binary.BigEndian.PutUint64(b[24:32], v)
	return b
}

func encodeStaticValue(f Field, v any) []byte {
	switch f.Type {
	case TypeUint8:
		return word(uint64(v.(uint8)))
	case TypeUint32:
		return word(uint64(v.(uint32)))
	case TypeUint64:
		return word(v.(uint64))
	case TypeUint256:
		b := make([]byte, wordSize)
		v.(*big.Int).FillBytes(b)
		return b
	case TypeBytes32:
		arr := v.([32]byte)
		return arr[:]
	case TypeBool:
		if v.(bool) {
			return word(1)
		}
		return word(0)
	case TypeAddress:
		b := make([]byte, wordSize)
		copy(b[12:], v.([]byte))
		return b
	case TypeStruct:
		return encodeTuple(f.Sub.Fields, v.([]any))
	default:
		panic("encodeStaticValue: unsupported type")
	}
}

func encodeDynamicValue(f Field, v any) []byte {
	switch f.Type {
	case TypeBytes:
		b := v.([]byte)
		out := word(uint64(len(b)))
		return append(out, padTo32(b)...)
	case TypeString:
		b := []byte(v.(string))
		out := word(uint64(len(b)))
		return append(out, padTo32(b)...)
	case TypeStruct:
		return encodeTuple(f.Sub.Fields, v.([]any))
	case TypeArray:
		return encodeArray(*f.Elem, v.([]any))
	default:
		panic("encodeDynamicValue: unsupported type")
	}
}

// encodeTuple encodes fields/vals using the same head+tail layout decode.go expects.
func encodeTuple(fields []Field, vals []any) []byte {
	head := make([][]byte, len(fields))
	var tails [][]byte
	dynTailIdx := make([]int, len(fields))
	for i := range dynTailIdx {
		dynTailIdx[i] = -1
	}

	for i, f := range fields {
		if isDynamic(f) {
			dynTailIdx[i] = len(tails)
			tails = append(tails, encodeDynamicValue(f, vals[i]))
		} else {
			head[i] = encodeStaticValue(f, vals[i])
		}
	}

	headSize := len(fields) * wordSize
	offsets := make([]int, len(tails))
	cum := 0
	for i, b := range tails {
		offsets[i] = headSize + cum
		cum += len(b)
	}
	for i := range fields {
		if dynTailIdx[i] >= 0 {
			head[i] = word(uint64(offsets[dynTailIdx[i]]))
		}
	}

	var out []byte
	for _, h := range head {
		out = append(out, h...)
	}
	for _, t := range tails {
		out = append(out, t...)
	}
	return out
}

func encodeArray(elem Field, vals []any) []byte {
	fields := make([]Field, len(vals))
	for i := range fields {
		fields[i] = elem
	}
	body := encodeTuple(fields, vals)
	return append(word(uint64(len(vals))), body...)
}
