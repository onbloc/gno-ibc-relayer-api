// Package ethabi is a Go port of gno.land/p/core/encoding/abi.
// It decodes Solidity ABI-encoded data using the abi_encode_params convention
// (no extra 0x20 head-offset prefix) used throughout the UCS03-ZKGM protocol.
package ethabi

import "errors"

// Type identifies a Solidity ABI field kind.
type Type int

const (
	TypeUint8 Type = iota
	TypeUint32
	TypeUint64
	TypeUint256
	TypeBytes32
	TypeBytes
	TypeString
	TypeBool
	TypeAddress
	TypeStruct
	TypeArray
)

var (
	ErrUnsupportedType = errors.New("abi: unsupported type")
	ErrInvalidSchema   = errors.New("abi: invalid schema")
	ErrInvalidData     = errors.New("abi: invalid data")
	ErrDepthLimit      = errors.New("abi: depth limit exceeded")
)

// Field describes one ABI tuple field.
type Field struct {
	Type Type
	Sub  *Schema // non-nil when Type == TypeStruct
	Elem *Field  // non-nil when Type == TypeArray
}

// Schema describes an ABI tuple (ordered list of fields).
type Schema struct {
	Fields []Field
}

const (
	wordSize       = 32
	maxDecodeDepth = 256
)

// isDynamic reports whether f requires an offset pointer in the tuple head.
func isDynamic(f Field) bool {
	switch f.Type {
	case TypeBytes, TypeString, TypeArray:
		return true
	case TypeStruct:
		if f.Sub == nil {
			return false
		}
		for _, sf := range f.Sub.Fields {
			if isDynamic(sf) {
				return true
			}
		}
		return false
	default:
		return false
	}
}

// staticHeadSize returns the number of bytes a static field occupies inline.
// Only call this when isDynamic(f) == false.
func staticHeadSize(f Field) (int, error) {
	switch f.Type {
	case TypeUint8, TypeUint32, TypeUint64, TypeUint256, TypeBytes32, TypeBool, TypeAddress:
		return wordSize, nil
	case TypeStruct:
		if f.Sub == nil {
			return 0, ErrInvalidSchema
		}
		total := 0
		for _, sf := range f.Sub.Fields {
			s, err := staticHeadSize(sf)
			if err != nil {
				return 0, err
			}
			total += s
		}
		return total, nil
	default:
		return 0, ErrUnsupportedType
	}
}
