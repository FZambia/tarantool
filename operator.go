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
func OpAdd(field uint64, arg int64) Op {
	return Op{encode: func(enc *msgpack.Encoder) error {
		_ = enc.EncodeArrayLen(3)
		_ = enc.EncodeString("+")
		_ = enc.EncodeUint(field)
		return enc.EncodeInt(arg)
	}}
}

// OpSub ...
func OpSub(field uint64, arg int64) Op {
	return Op{encode: func(enc *msgpack.Encoder) error {
		_ = enc.EncodeArrayLen(3)
		_ = enc.EncodeString("-")
		_ = enc.EncodeUint(field)
		return enc.EncodeInt(arg)
	}}
}

// OpBitAND ...
func OpBitAND(field, arg uint64) Op {
	return Op{encode: func(enc *msgpack.Encoder) error {
		_ = enc.EncodeArrayLen(3)
		_ = enc.EncodeString("&")
		_ = enc.EncodeUint(field)
		return enc.EncodeUint(arg)
	}}
}

// OpBitXOR ...
func OpBitXOR(field, arg uint64) Op {
	return Op{encode: func(enc *msgpack.Encoder) error {
		_ = enc.EncodeArrayLen(3)
		_ = enc.EncodeString("^")
		_ = enc.EncodeUint(field)
		return enc.EncodeUint(arg)
	}}
}

// OpBitOR ...
func OpBitOR(field, arg uint64) Op {
	return Op{encode: func(enc *msgpack.Encoder) error {
		_ = enc.EncodeArrayLen(3)
		_ = enc.EncodeString("|")
		_ = enc.EncodeUint(field)
		return enc.EncodeUint(arg)
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
func OpInsert(before uint64, arg interface{}) Op {
	return Op{encode: func(enc *msgpack.Encoder) error {
		_ = enc.EncodeArrayLen(3)
		_ = enc.EncodeString("!")
		_ = enc.EncodeUint(before)
		return enc.Encode(arg)
	}}
}

// OpAssign ...
func OpAssign(field uint64, arg interface{}) Op {
	return Op{encode: func(enc *msgpack.Encoder) error {
		_ = enc.EncodeArrayLen(3)
		_ = enc.EncodeString("=")
		_ = enc.EncodeUint(field)
		return enc.Encode(arg)
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
