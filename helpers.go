package tarantool

import (
	"github.com/vmihailenco/msgpack/v5"
)

// IntKey is utility type for passing integer key to Select, Update and Delete.
// It serializes to array with single integer element.
type IntKey struct {
	I int64
}

func (k IntKey) EncodeMsgpack(enc *msgpack.Encoder) error {
	_ = enc.EncodeArrayLen(1)
	_ = enc.EncodeInt(k.I)
	return nil
}

// UintKey is utility type for passing unsigned integer key to Select, Update and Delete.
// It serializes to array with single integer element.
type UintKey struct {
	I uint64
}

func (k UintKey) EncodeMsgpack(enc *msgpack.Encoder) error {
	_ = enc.EncodeArrayLen(1)
	_ = enc.EncodeUint(k.I)
	return nil
}

// UintKey is utility type for passing string key to Select, Update and Delete.
// It serializes to array with single string element.
type StringKey struct {
	S string
}

func (k StringKey) EncodeMsgpack(enc *msgpack.Encoder) error {
	_ = enc.EncodeArrayLen(1)
	_ = enc.EncodeString(k.S)
	return nil
}

// IntIntKey is utility type for passing two integer keys to Select, Update and Delete.
// It serializes to array with two integer elements
type IntIntKey struct {
	I1, I2 int64
}

func (k IntIntKey) EncodeMsgpack(enc *msgpack.Encoder) error {
	_ = enc.EncodeArrayLen(2)
	_ = enc.EncodeInt(k.I1)
	_ = enc.EncodeInt(k.I2)
	return nil
}
