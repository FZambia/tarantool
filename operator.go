package tarantool

import (
	"github.com/vmihailenco/msgpack/v5"
)

type Op struct {
	encode func(enc *msgpack.Encoder) error
}

func (op Op) EncodeMsgpack(enc *msgpack.Encoder) error {
	return op.encode(enc)
}

// OpAdd ...
func OpAdd(field uint64, val interface{}) Op {
	return Op{encode: func(enc *msgpack.Encoder) error {
		_ = enc.EncodeArrayLen(3)
		_ = enc.EncodeString("+")
		_ = enc.EncodeUint(field)
		return enc.Encode(val)
	}}
}

// OpSub ...
func OpSub(field uint64, val interface{}) Op {
	return Op{encode: func(enc *msgpack.Encoder) error {
		_ = enc.EncodeArrayLen(3)
		_ = enc.EncodeString("-")
		_ = enc.EncodeUint(field)
		return enc.Encode(val)
	}}
}

// OpBitAND ...
func OpBitAND(field, val uint64) Op {
	return Op{encode: func(enc *msgpack.Encoder) error {
		_ = enc.EncodeArrayLen(3)
		_ = enc.EncodeString("&")
		_ = enc.EncodeUint(field)
		return enc.EncodeUint(val)
	}}
}

// OpBitXOR ...
func OpBitXOR(field, val uint64) Op {
	return Op{encode: func(enc *msgpack.Encoder) error {
		_ = enc.EncodeArrayLen(3)
		_ = enc.EncodeString("^")
		_ = enc.EncodeUint(field)
		return enc.EncodeUint(val)
	}}
}

// OpBitOR ...
func OpBitOR(field, val uint64) Op {
	return Op{encode: func(enc *msgpack.Encoder) error {
		_ = enc.EncodeArrayLen(3)
		_ = enc.EncodeString("|")
		_ = enc.EncodeUint(field)
		return enc.EncodeUint(val)
	}}
}

// OpDelete ...
func OpDelete(from, count uint64) Op {
	return Op{encode: func(enc *msgpack.Encoder) error {
		_ = enc.EncodeArrayLen(3)
		_ = enc.EncodeString("#")
		_ = enc.EncodeUint(from)
		return enc.EncodeUint(count)
	}}
}

// OpInsert ...
func OpInsert(before uint64, val interface{}) Op {
	return Op{encode: func(enc *msgpack.Encoder) error {
		_ = enc.EncodeArrayLen(3)
		_ = enc.EncodeString("!")
		_ = enc.EncodeUint(before)
		return enc.Encode(val)
	}}
}

// OpAssign ...
func OpAssign(field uint64, val interface{}) Op {
	return Op{encode: func(enc *msgpack.Encoder) error {
		_ = enc.EncodeArrayLen(3)
		_ = enc.EncodeString("=")
		_ = enc.EncodeUint(field)
		return enc.Encode(val)
	}}
}

// OpSplice ...
func OpSplice(field, offset, position uint64, replace string) Op {
	return Op{encode: func(enc *msgpack.Encoder) error {
		_ = enc.EncodeArrayLen(4)
		_ = enc.EncodeString(":")
		_ = enc.EncodeUint(field)
		_ = enc.EncodeUint(offset)
		_ = enc.EncodeUint(position)
		return enc.EncodeString(replace)
	}}
}
