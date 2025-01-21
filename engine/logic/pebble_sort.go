package logic

import (
	"encoding/binary"
	"fmt"
	"math"
)

const boolType byte = 0
const intType byte = 1
const floatType byte = 2
const stringType byte = 3

func CompareEncoded(a, b []byte) int {
	switch a[0] {
	case boolType:
		switch b[0] {
		case boolType:
			return CompareBooleans(DecodeBool(a), DecodeBool(b))
		default:
			return int(a[0] - b[0])
		}
	case intType:
		switch b[0] {
		case intType:
			return CompareInts(DecodeInt(a), DecodeInt(b))
		case floatType:
			return CompareAny(DecodeInt(a), DecodeFloat(b))
		default:
			return int(a[0] - b[0])
		}
	case floatType:
		switch b[0] {
		case floatType:
			return CompareFloats(DecodeFloat(a), DecodeFloat(b))
		case intType:
			return CompareAny(DecodeFloat(a), DecodeInt(b))
		default:
			return int(a[0] - b[0])
		}
	case stringType:
		switch b[0] {
		case stringType:
			return CompareStrings(DecodeString(a), DecodeString(b))
		default:
			return int(a[0] - b[0])
		}
	default:
		return int(a[0] - b[1])
	}

}

func EncodeInt(a int64) []byte {
	out := make([]byte, 9)
	out[0] = intType
	binary.LittleEndian.PutUint64(out[1:], uint64(a))
	return out
}

func EncodeFloat(a float64) []byte {
	out := make([]byte, 9)
	out[0] = floatType
	binary.LittleEndian.PutUint64(out[1:], math.Float64bits(a))
	return out
}

func EncodeString(a string) []byte {
	out := make([]byte, len(a)+1)
	out[0] = stringType
	copy(out[1:], a)
	return out
}

func EncodeBool(a bool) []byte {
	out := []byte{boolType, 0x00}
	if a {
		out[1] = 0x01
	}
	return out
}

func EncodeAny(k any) []byte {
	switch y := k.(type) {
	case int:
		return EncodeInt(int64(y))
	case int32:
		return EncodeInt(int64(y))
	case int16:
		return EncodeInt(int64(y))
	case int64:
		return EncodeInt(int64(y))
	case float32:
		return EncodeFloat(float64(y))
	case float64:
		return EncodeFloat(y)
	case string:
		return EncodeString(y)
	case bool:
		return EncodeBool(y)
	default:
		fmt.Printf("Missing type: %s\n", y)
		return []byte{}
	}
}

func DecodeAny(k []byte) any {
	switch k[0] {
	case boolType:
		return DecodeBool(k)
	case intType:
		return DecodeInt(k)
	case floatType:
		return DecodeFloat(k)
	case stringType:
		return DecodeString(k)
	}
	return nil
}

func DecodeFloat(k []byte) float64 {
	return math.Float64frombits(binary.LittleEndian.Uint64(k[1:]))
}

func DecodeInt(k []byte) int64 {
	return int64(binary.LittleEndian.Uint64(k[1:]))
}

func DecodeBool(k []byte) bool {
	return k[1] != 0
}

func DecodeString(k []byte) string {
	return string(k[1:])
}
