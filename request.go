package tarantool

import (
	"github.com/vmihailenco/msgpack/v5"
)

// Request to be executed in Tarantool.
type Request struct {
	requestCode int32
	sendFunc    func(conn *Connection) (func(enc *msgpack.Encoder) error, error)
	push        func(*Response)
	pushTyped   func(func(interface{}) error)
}

func newRequest(requestCode int32, cb func(conn *Connection) (func(enc *msgpack.Encoder) error, error)) *Request {
	return &Request{
		requestCode: requestCode,
		sendFunc:    cb,
	}
}

// WithPush allows setting Push handler to Request.
func (req *Request) WithPush(pushCB func(*Response)) *Request {
	req.push = pushCB
	return req
}

// WithPushTyped allows setting typed Push handler to Request.
func (req *Request) WithPushTyped(pushTypedCB func(func(interface{}) error)) *Request {
	req.pushTyped = pushTypedCB
	return req
}

func (req *Request) pack(requestID uint32, h *smallWBuf, enc *msgpack.Encoder, body func(*msgpack.Encoder) error) (err error) {
	hl := len(*h)
	*h = append(*h, smallWBuf{
		0xce, 0, 0, 0, 0, // length
		0x82,                           // 2 element map
		KeyCode, byte(req.requestCode), // request code
		KeySync, 0xce,
		byte(requestID >> 24), byte(requestID >> 16),
		byte(requestID >> 8), byte(requestID),
	}...)

	if err = body(enc); err != nil {
		return
	}

	l := uint32(len(*h) - 5 - hl)
	(*h)[hl+1] = byte(l >> 24)
	(*h)[hl+2] = byte(l >> 16)
	(*h)[hl+3] = byte(l >> 8)
	(*h)[hl+4] = byte(l)

	return
}

// Ping sends empty request to Tarantool to check connection.
func Ping() *Request {
	return newRequest(PingRequest, func(conn *Connection) (func(enc *msgpack.Encoder) error, error) {
		return func(enc *msgpack.Encoder) error {
			_ = enc.EncodeMapLen(0)
			return nil
		}, nil
	})
}

// Select sends select request to Tarantool.
func Select(space, index interface{}, offset, limit, iterator uint32, key interface{}) *Request {
	return newRequest(SelectRequest, func(conn *Connection) (func(enc *msgpack.Encoder) error, error) {
		spaceNo, indexNo, err := conn.schema.resolveSpaceIndex(space, index)
		if err != nil {
			return nil, err
		}
		return func(enc *msgpack.Encoder) error {
			_ = enc.EncodeMapLen(6)
			fillIterator(enc, offset, limit, iterator)
			return fillSearch(enc, spaceNo, indexNo, key)
		}, nil
	})
}

// Insert sends insert action to Tarantool.
// Tarantool will reject Insert when tuple with same primary key exists.
func Insert(space interface{}, tuple interface{}) *Request {
	return newRequest(InsertRequest, func(conn *Connection) (func(enc *msgpack.Encoder) error, error) {
		spaceNo, _, err := conn.schema.resolveSpaceIndex(space, nil)
		if err != nil {
			return nil, err
		}
		return func(enc *msgpack.Encoder) error {
			_ = enc.EncodeMapLen(2)
			return fillInsert(enc, spaceNo, tuple)
		}, nil
	})
}

// Replace sends "insert or replace" action to Tarantool.
// If tuple with same primary key exists, it will be replaced.
func Replace(space interface{}, tuple interface{}) *Request {
	return newRequest(ReplaceRequest, func(conn *Connection) (func(enc *msgpack.Encoder) error, error) {
		spaceNo, _, err := conn.schema.resolveSpaceIndex(space, nil)
		if err != nil {
			return nil, err
		}
		return func(enc *msgpack.Encoder) error {
			_ = enc.EncodeMapLen(2)
			return fillInsert(enc, spaceNo, tuple)
		}, nil
	})
}

// Delete sends deletion action to Tarantool.
// Result will contain array with deleted tuple.
func Delete(space, index interface{}, key interface{}) *Request {
	return newRequest(DeleteRequest, func(conn *Connection) (func(enc *msgpack.Encoder) error, error) {
		spaceNo, indexNo, err := conn.schema.resolveSpaceIndex(space, index)
		if err != nil {
			return nil, err
		}
		return func(enc *msgpack.Encoder) error {
			_ = enc.EncodeMapLen(3)
			return fillSearch(enc, spaceNo, indexNo, key)
		}, nil
	})
}

// Update sends deletion of a tuple by key.
// Result will contain array with updated tuple.
func Update(space, index interface{}, key interface{}, ops []Op) *Request {
	return newRequest(UpdateRequest, func(conn *Connection) (func(enc *msgpack.Encoder) error, error) {
		spaceNo, indexNo, err := conn.schema.resolveSpaceIndex(space, index)
		if err != nil {
			return nil, err
		}
		return func(enc *msgpack.Encoder) error {
			_ = enc.EncodeMapLen(4)
			if err := fillSearch(enc, spaceNo, indexNo, key); err != nil {
				return err
			}
			_ = enc.EncodeInt(KeyTuple)
			return enc.Encode(ops)
		}, nil
	})
}

// Upsert sends "update or insert" action to Tarantool.
// Result will not contain any tuple.
func Upsert(space interface{}, tuple interface{}, ops []Op) *Request {
	return newRequest(UpsertRequest, func(conn *Connection) (func(enc *msgpack.Encoder) error, error) {
		spaceNo, _, err := conn.schema.resolveSpaceIndex(space, nil)
		if err != nil {
			return nil, err
		}
		return func(enc *msgpack.Encoder) error {
			_ = enc.EncodeMapLen(3)
			_ = enc.EncodeInt(KeySpaceNo)
			_ = enc.EncodeInt(int64(spaceNo))
			_ = enc.EncodeInt(KeyTuple)
			if err := enc.Encode(tuple); err != nil {
				return err
			}
			_ = enc.EncodeInt(KeyDefTuple)
			return enc.Encode(ops)
		}, nil
	})
}

// Call sends a call to registered Tarantool function.
// It uses request code for Tarantool 1.7, so future's result will not be converted
// (though, keep in mind, result is always array).
func Call(functionName string, args interface{}) *Request {
	return newRequest(Call17Request, func(conn *Connection) (func(enc *msgpack.Encoder) error, error) {
		return func(enc *msgpack.Encoder) error {
			_ = enc.EncodeMapLen(2)
			_ = enc.EncodeInt(KeyFunctionName)
			_ = enc.EncodeString(functionName)
			_ = enc.EncodeInt(KeyTuple)
			return enc.Encode(args)
		}, nil
	})
}

// Eval sends a lua expression for evaluation.
func Eval(expr string, args interface{}) *Request {
	return newRequest(EvalRequest, func(conn *Connection) (func(enc *msgpack.Encoder) error, error) {
		return func(enc *msgpack.Encoder) error {
			_ = enc.EncodeMapLen(2)
			_ = enc.EncodeInt(KeyExpression)
			_ = enc.EncodeString(expr)
			_ = enc.EncodeInt(KeyTuple)
			return enc.Encode(args)
		}, nil
	})
}
