package tarantool

import (
	"context"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

// Future allows to extract response from server as soon as it's ready.
type Future interface {
	Get() (*Response, error)
	GetTyped(result interface{}) error
}

// FutureContext allows to extract response from server as soon as it's ready with Context.
type FutureContext interface {
	GetContext(ctx context.Context) (*Response, error)
	GetTypedContext(ctx context.Context, result interface{}) error
}

// futureImpl is a handle for asynchronous request.
type futureImpl struct {
	requestID uint32
	timeout   time.Duration
	conn      *Connection
	req       *Request
	resp      *Response
	err       error
	ready     chan struct{}
	next      *futureImpl
}

// Get waits for future to be filled and returns Response and error.
//
// Response will contain deserialized result in Data field.
// It will be []interface{}, so if you want more performance, use GetTyped method.
//
// Note: Response could be equal to nil if ClientError is returned in error.
//
// "error" could be Error, if it is error returned by Tarantool,
// or ClientError, if something bad happens in a client process.
func (fut *futureImpl) Get() (*Response, error) {
	fut.wait()
	if fut.err != nil {
		return fut.resp, fut.err
	}
	fut.err = fut.resp.decodeBody()
	return fut.resp, fut.err
}

// GetTyped waits for future and decodes response into result if no error happens.
// This could be much faster than Get() function.
func (fut *futureImpl) GetTyped(result interface{}) error {
	fut.wait()
	if fut.err != nil {
		return fut.err
	}
	fut.err = fut.resp.decodeBodyTyped(result)
	return fut.err
}

// GetContext waits for future to be filled and returns Response and error.
func (fut *futureImpl) GetContext(ctx context.Context) (*Response, error) {
	fut.waitContext(ctx)
	if fut.err != nil {
		if fut.err == context.DeadlineExceeded || fut.err == context.Canceled {
			fut.conn.fetchFuture(fut.requestID)
		}
		return fut.resp, fut.err
	}
	fut.err = fut.resp.decodeBody()
	return fut.resp, fut.err
}

// GetTypedContext waits for futureImpl and calls msgpack.Decoder.Decode(result) if no error happens.
// It is could be much faster than GetContext() function.
func (fut *futureImpl) GetTypedContext(ctx context.Context, result interface{}) error {
	fut.waitContext(ctx)
	if fut.err != nil {
		if fut.err == context.DeadlineExceeded || fut.err == context.Canceled {
			fut.conn.fetchFuture(fut.requestID)
		}
		return fut.err
	}
	fut.err = fut.resp.decodeBodyTyped(result)
	return fut.err
}

func (fut *futureImpl) markPushReady(resp *Response) {
	if fut.req.push == nil && fut.req.pushTyped == nil {
		return
	}
	if fut.req.push != nil {
		err := resp.decodeBody()
		if err == nil {
			fut.req.push(resp)
		}
		return
	}
	fut.req.pushTyped(func(i interface{}) error {
		return resp.decodeBodyTyped(i)
	})
}

func (fut *futureImpl) markReady(conn *Connection) {
	close(fut.ready)
	if conn.rLimit != nil {
		<-conn.rLimit
	}
}

func (fut *futureImpl) waitContext(ctx context.Context) {
	if fut.ready == nil {
		return
	}
	select {
	case <-fut.ready:
	case <-ctx.Done():
		fut.err = ctx.Err()
	}
}

func (fut *futureImpl) wait() {
	if fut.ready == nil {
		return
	}
	<-fut.ready
}

func fillSearch(enc *msgpack.Encoder, spaceNo, indexNo uint32, key interface{}) error {
	_ = enc.EncodeInt(KeySpaceNo)
	_ = enc.EncodeInt(int64(spaceNo))
	_ = enc.EncodeInt(KeyIndexNo)
	_ = enc.EncodeInt(int64(indexNo))
	_ = enc.EncodeInt(KeyKey)
	return enc.Encode(key)
}

func fillIterator(enc *msgpack.Encoder, offset, limit, iterator uint32) {
	_ = enc.EncodeInt(KeyIterator)
	_ = enc.EncodeInt(int64(iterator))
	_ = enc.EncodeInt(KeyOffset)
	_ = enc.EncodeInt(int64(offset))
	_ = enc.EncodeInt(KeyLimit)
	_ = enc.EncodeInt(int64(limit))
}

func fillInsert(enc *msgpack.Encoder, spaceNo uint32, tuple interface{}) error {
	_ = enc.EncodeInt(KeySpaceNo)
	_ = enc.EncodeInt(int64(spaceNo))
	_ = enc.EncodeInt(KeyTuple)
	return enc.Encode(tuple)
}
