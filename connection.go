package tarantool

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/url"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

const requestsMap = 128

const packetLengthBytes = 5

const (
	connDisconnected = 0
	connConnected    = 1
	connClosed       = 2
)

type ConnEventKind int

const (
	// Connected signals that connection is established or reestablished.
	Connected ConnEventKind = iota + 1
	// Disconnected signals that connection is broken.
	Disconnected
	// ReconnectFailed signals that attempt to reconnect has failed.
	ReconnectFailed
	// Closed means either reconnect attempts exhausted, or explicit Close is called.
	Closed
)

type ConnLogKind int

const (
	// LogReconnectFailed is logged when reconnect attempt failed.
	LogReconnectFailed ConnLogKind = iota + 1
	// LogLastReconnectFailed is logged when last reconnect attempt failed,
	// connection will be closed after that.
	LogLastReconnectFailed
	// LogUnexpectedResultID is logged when response with unknown id were received.
	// Most probably it is due to request timeout.
	LogUnexpectedResultID
)

// DefaultConnectTimeout to Tarantool.
const DefaultConnectTimeout = time.Second

// DefaultReadTimeout to Tarantool.
const DefaultReadTimeout = 30 * time.Second

// DefaultWriteTimeout to Tarantool.
const DefaultWriteTimeout = 5 * time.Second

// ConnEvent is sent throw Notify channel specified in Opts.
type ConnEvent struct {
	Conn *Connection
	Kind ConnEventKind
	When time.Time
}

// Logger is logger type expected to be passed in options.
type Logger interface {
	Report(event ConnLogKind, conn *Connection, v ...interface{})
}

// Connection to Tarantool.
//
// It is created and configured with Connect function, and could not be
// reconfigured later.
//
// It is could be "Connected", "Disconnected", and "Closed".
//
// When "Connected" it sends queries to Tarantool.
//
// When "Disconnected" it rejects queries with ClientError{Code: ErrConnectionNotReady}
//
// When "Closed" it rejects queries with ClientError{Code: ErrConnectionClosed}
//
// Connection could become "Closed" when Connection.Close() method called,
// or when Tarantool disconnected and ReconnectDelay pause is not specified or
// MaxReconnects is specified and MaxReconnect reconnect attempts already performed.
//
// You may perform data manipulation operation by executing:
// Call, Insert, Replace, Update, Upsert, Eval.
//
// In any method that accepts `space` you my pass either space number or
// space name (in this case it will be looked up in schema). Same is true for `index`.
//
// ATTENTION: `tuple`, `key`, `ops` and `args` arguments for any method should be
// and array or should serialize to msgpack array.
type Connection struct {
	state     uint32 // Keep atomics on top to work on 32-bit architectures.
	requestID uint32

	c     net.Conn
	mutex sync.Mutex

	schema   *Schema
	greeting *Greeting

	shard      []connShard
	dirtyShard chan uint32

	control chan struct{}
	rLimit  chan struct{}
	opts    Opts
	dec     *msgpack.Decoder
	lenBuf  [packetLengthBytes]byte
}

type connShard struct {
	mu       sync.Mutex
	requests [requestsMap]struct {
		first *futureImpl
		last  **futureImpl
	}
	bufMu sync.Mutex
	buf   smallWBuf
	enc   *msgpack.Encoder
	_     [16]uint64
}

// Greeting is a message sent by tarantool on connect.
type Greeting struct {
	Version string
	auth    string
}

// Opts is a way to configure Connection.
type Opts struct {
	// User name for auth.
	User string
	// Password for auth.
	Password string

	// ConnectTimeout sets connect timeout. If not set then DefaultConnectTimeout will be used.
	ConnectTimeout time.Duration
	// RequestTimeout is a default requests timeout. Can be overridden
	// on per-operation basis using context.Context.
	RequestTimeout time.Duration
	// ReadTimeout used to setup underlying connection ReadDeadline.
	ReadTimeout time.Duration
	// WriteTimeout used to setup underlying connection WriteDeadline.
	WriteTimeout time.Duration
	// ReconnectDelay is a pause between reconnection attempts.
	// If specified, then when tarantool is not reachable or disconnected,
	// new connect attempt is performed after pause.
	// By default, no reconnection attempts are performed,
	// so once disconnected, connection becomes Closed.
	ReconnectDelay time.Duration
	// MaxReconnects is a maximum reconnect attempts.
	// After MaxReconnects attempts Connection becomes closed.
	MaxReconnects uint64

	// SkipSchema disables schema loading. Without disabling schema loading, there
	// is no way to create Connection for currently not accessible tarantool.
	SkipSchema bool

	// RateLimit limits number of 'in-fly' request, ie already put into
	// requests queue, but not yet answered by server or timed out.
	// It is disabled by default.
	// See RLimitAction for possible actions when RateLimit.reached.
	RateLimit uint32
	// RLimitAction tells what to do when RateLimit reached:
	//   RLimitDrop - immediately abort request,
	//   RLimitWait - waitContext during timeout period for some request to be answered.
	//                If no request answered during timeout period, this request
	//                is aborted.
	//                If no timeout period is set, it will waitContext forever.
	// It is required if RateLimit is specified.
	RLimitAction uint32
	// Concurrency is amount of separate mutexes for request
	// queues and buffers inside of connection.
	// It is rounded upto nearest power of 2.
	// By default it is runtime.GOMAXPROCS(-1) * 4
	Concurrency uint32

	// Notify is a channel which receives notifications about Connection status changes.
	Notify chan<- ConnEvent
	// Handle is user specified value, that could be retrieved with Handle() method.
	Handle interface{}
	// Logger is user specified logger used for log messages.
	Logger Logger

	network string
	address string
}

// Connect creates and configures new Connection.
//
// Address could be specified in following ways:
//
// 	host:port (no way to provide other options over DSN in this case)
// 	tcp://[[username[:password]@]host[:port][/?option1=value1&optionN=valueN]
// 	unix://[[username[:password]@]path[?option1=value1&optionN=valueN]
//
// TCP connections:
// 	- 127.0.0.1:3301
// 	- tcp://[fe80::1]:3301
// 	- tcp://user:pass@example.com:3301
// 	- tcp://user@example.com/?connect_timeout=5s&request_timeout=1s
// Unix socket:
// 	- unix:///var/run/tarantool/my_instance.sock
// 	- unix://user:pass@/var/run/tarantool/my_instance.sock?connect_timeout=5s
//
// Note: Connect will always return an error if first connection attempt failed even if
// reconnect configured.
func Connect(addr string, opts Opts) (conn *Connection, err error) {
	opts, err = optsFromAddr(addr, opts)
	if err != nil {
		return
	}
	conn = &Connection{
		requestID: 0,
		greeting:  &Greeting{},
		control:   make(chan struct{}),
		opts:      opts,
		dec:       getDecoder(&smallBuf{}),
	}
	maxProc := uint32(runtime.GOMAXPROCS(-1))
	if conn.opts.Concurrency == 0 || conn.opts.Concurrency > maxProc*128 {
		conn.opts.Concurrency = maxProc * 4
	}
	if c := conn.opts.Concurrency; c&(c-1) != 0 {
		for i := uint(1); i < 32; i *= 2 {
			c |= c >> i
		}
		conn.opts.Concurrency = c + 1
	}
	conn.dirtyShard = make(chan uint32, conn.opts.Concurrency*2)
	conn.shard = make([]connShard, conn.opts.Concurrency)
	for i := range conn.shard {
		shard := &conn.shard[i]
		for j := range shard.requests {
			shard.requests[j].last = &shard.requests[j].first
		}
	}

	if opts.RateLimit > 0 {
		conn.rLimit = make(chan struct{}, opts.RateLimit)
		if opts.RLimitAction != RLimitDrop && opts.RLimitAction != RLimitWait {
			return nil, errors.New("RLimitAction should be specified to RLimitDone nor RLimitWait")
		}
	}

	if err = conn.createConnection(false); err != nil {
		ter, ok := err.(Error)
		if conn.opts.ReconnectDelay <= 0 {
			return nil, err
		} else if ok && (ter.Code == ErrNoSuchUser ||
			ter.Code == ErrPasswordMismatch) {
			// Report auth errors immediately.
			return nil, err
		} else {
			// Without SkipSchema it is useless.
			go func(conn *Connection) {
				conn.mutex.Lock()
				defer conn.mutex.Unlock()
				if err := conn.createConnection(true); err != nil {
					_ = conn.closeConnection(err, true)
				}
			}(conn)
			err = nil
		}
	}

	go conn.pingRoutine()
	if conn.opts.RequestTimeout != 0 {
		go conn.timeouts()
	}

	return conn, err
}

// Greeting sent by Tarantool on connect.
func (conn *Connection) Greeting() *Greeting {
	return conn.greeting
}

// Schema loaded from Tarantool.
func (conn *Connection) Schema() *Schema {
	return conn.schema
}

// NetConn returns underlying net.Conn.
func (conn *Connection) NetConn() net.Conn {
	conn.mutex.Lock()
	defer conn.mutex.Unlock()
	return conn.c
}

// Exec Request.
func (conn *Connection) Exec(req *Request) (*Response, error) {
	return conn.newFuture(req, true).Get()
}

// Exec Request and decode it to typed result.
func (conn *Connection) ExecTyped(req *Request, result interface{}) error {
	return conn.newFuture(req, true).GetTyped(result)
}

// Exec Request with context.Context.
func (conn *Connection) ExecContext(ctx context.Context, req *Request) (*Response, error) {
	if _, ok := ctx.Deadline(); !ok && conn.opts.RequestTimeout > 0 {
		var cancel func()
		ctx, cancel = context.WithTimeout(ctx, conn.opts.RequestTimeout)
		defer cancel()
	}
	return conn.newFuture(req, false).GetContext(ctx)
}

// Exec Request with context.Context and decode it to typed result.
func (conn *Connection) ExecTypedContext(ctx context.Context, req *Request, result interface{}) error {
	if _, ok := ctx.Deadline(); !ok && conn.opts.RequestTimeout > 0 {
		var cancel func()
		ctx, cancel = context.WithTimeout(ctx, conn.opts.RequestTimeout)
		defer cancel()
	}
	return conn.newFuture(req, false).GetTypedContext(ctx, result)
}

// Exec Request but do not wait for result - it's then possible to get
// result from returned Future.
func (conn *Connection) ExecAsync(req *Request) Future {
	return conn.newFuture(req, true)
}

// Exec Request but do not wait for result - it's then possible to get
// result from returned FutureContext.
func (conn *Connection) ExecAsyncContext(req *Request) FutureContext {
	return conn.newFuture(req, false)
}

// Close closes Connection.
// After this method called, there is no way to reopen this Connection.
func (conn *Connection) Close() error {
	err := ClientError{ErrConnectionClosed, "connection closed by client"}
	conn.mutex.Lock()
	defer conn.mutex.Unlock()
	return conn.closeConnection(err, true)
}

var epoch = time.Now()

func (conn *Connection) timeouts() {
	timeout := conn.opts.RequestTimeout
	t := time.NewTimer(timeout)
	for {
		var nowEpoch time.Duration
		select {
		case <-conn.control:
			t.Stop()
			return
		case <-t.C:
		}
		minNext := time.Since(epoch) + timeout
		for i := range conn.shard {
			nowEpoch = time.Since(epoch)
			shard := &conn.shard[i]
			for pos := range shard.requests {
				shard.mu.Lock()
				pair := &shard.requests[pos]
				for pair.first != nil && pair.first.timeout > 0 && pair.first.timeout < nowEpoch {
					shard.bufMu.Lock()
					fut := pair.first
					pair.first = fut.next
					if fut.next == nil {
						pair.last = &pair.first
					} else {
						fut.next = nil
					}
					fut.err = ClientError{
						Code: ErrTimedOut,
						Msg:  "request timeout",
					}
					fut.markReady(conn)
					shard.bufMu.Unlock()
				}
				if pair.first != nil && pair.first.timeout > 0 && pair.first.timeout < minNext {
					minNext = pair.first.timeout
				}
				shard.mu.Unlock()
			}
		}
		nowEpoch = time.Since(epoch)
		if nowEpoch+time.Microsecond < minNext {
			t.Reset(minNext - nowEpoch)
		} else {
			t.Reset(time.Microsecond)
		}
	}
}

func optsFromAddr(addr string, opts Opts) (Opts, error) {
	u, err := url.Parse(addr)
	if err != nil {
		if _, _, err := net.SplitHostPort(addr); err == nil {
			opts.network = "tcp"
			opts.address = addr
			return opts, nil
		}
		return opts, errors.New("malformed connection address")
	}
	switch u.Scheme {
	case "tcp":
		opts.network = "tcp"
		opts.address = u.Host
	case "unix":
		opts.network = "unix"
		opts.address = u.Path
	default:
		return opts, errors.New("connection address should have tcp:// or unix:// scheme")
	}
	if u.User != nil {
		opts.User = u.User.Username()
		if pass, ok := u.User.Password(); ok {
			opts.Password = pass
		}
	}
	connectTimeout := u.Query().Get("connect_timeout")
	if connectTimeout != "" {
		if v, err := time.ParseDuration(connectTimeout); err != nil {
			return opts, errors.New("malformed connect_timeout parameter")
		} else {
			opts.ConnectTimeout = v
		}
	}
	requestTimeout := u.Query().Get("request_timeout")
	if requestTimeout != "" {
		if v, err := time.ParseDuration(requestTimeout); err != nil {
			return opts, errors.New("malformed request_timeout parameter")
		} else {
			opts.RequestTimeout = v
		}
	}
	writeTimeout := u.Query().Get("write_timeout")
	if writeTimeout != "" {
		if v, err := time.ParseDuration(writeTimeout); err != nil {
			return opts, errors.New("malformed write_timeout parameter")
		} else {
			opts.WriteTimeout = v
		}
	}
	readTimeout := u.Query().Get("read_timeout")
	if readTimeout != "" {
		if v, err := time.ParseDuration(readTimeout); err != nil {
			return opts, errors.New("malformed read_timeout parameter")
		} else {
			opts.ReadTimeout = v
		}
	}
	reconnectDelay := u.Query().Get("reconnect_delay")
	if reconnectDelay != "" {
		if v, err := time.ParseDuration(reconnectDelay); err != nil {
			return opts, errors.New("malformed reconnect_delay parameter")
		} else {
			opts.ReconnectDelay = v
		}
	}
	maxReconnects := u.Query().Get("max_reconnects")
	if maxReconnects != "" {
		if v, err := strconv.ParseUint(maxReconnects, 10, 64); err != nil {
			return opts, errors.New("malformed max_reconnects parameter")
		} else {
			opts.MaxReconnects = v
		}
	}
	skipSchema := u.Query().Get("skip_schema")
	if skipSchema == "true" {
		opts.SkipSchema = true
	}
	return opts, nil
}

func (conn *Connection) dial() (err error) {
	var connection net.Conn
	timeout := DefaultConnectTimeout
	if conn.opts.ConnectTimeout != 0 {
		timeout = conn.opts.ConnectTimeout
	}
	connection, err = net.DialTimeout(conn.opts.network, conn.opts.address, timeout)
	if err != nil {
		return
	}
	readTimeout := DefaultReadTimeout
	if conn.opts.ReadTimeout != 0 {
		readTimeout = conn.opts.ReadTimeout
	}
	writeTimeout := DefaultWriteTimeout
	if conn.opts.WriteTimeout != 0 {
		writeTimeout = conn.opts.WriteTimeout
	}
	dc := &deadlineIO{rto: readTimeout, wto: writeTimeout, c: connection}
	r := bufio.NewReaderSize(dc, 128*1024)
	w := bufio.NewWriterSize(dc, 128*1024)
	greeting := make([]byte, 128)
	_, err = io.ReadFull(r, greeting)
	if err != nil {
		_ = connection.Close()
		return
	}
	conn.greeting.Version = bytes.NewBuffer(greeting[:64]).String()
	conn.greeting.auth = bytes.NewBuffer(greeting[64:108]).String()

	// Auth.
	if conn.opts.User != "" {
		scr, err := scramble(conn.greeting.auth, conn.opts.Password)
		if err != nil {
			err = errors.New("auth: scrambling failure " + err.Error())
			_ = connection.Close()
			return err
		}
		if err = conn.writeAuthRequest(w, scr); err != nil {
			_ = connection.Close()
			return err
		}
		if err = conn.readAuthResponse(r); err != nil {
			_ = connection.Close()
			return err
		}
	}

	// Only if connected and authenticated.
	conn.lockShards()
	conn.c = connection
	atomic.StoreUint32(&conn.state, connConnected)
	conn.unlockShards()
	go conn.writer(w, connection)
	go conn.reader(r, connection)

	if !conn.opts.SkipSchema {
		if err = conn.loadSchema(); err != nil {
			_ = connection.Close()
			return err
		}
	}

	return
}

func (conn *Connection) writeAuthRequest(w *bufio.Writer, scramble []byte) (err error) {
	request := &Request{
		requestCode: AuthRequest,
	}
	var packet smallWBuf
	err = request.pack(0, &packet, msgpack.NewEncoder(&packet), func(enc *msgpack.Encoder) error {
		return enc.Encode(map[uint32]interface{}{
			KeyUserName: conn.opts.User,
			KeyTuple:    []interface{}{"chap-sha1", string(scramble)},
		})
	})
	if err != nil {
		return errors.New("auth: pack error " + err.Error())
	}
	if err := write(w, packet); err != nil {
		return errors.New("auth: write error " + err.Error())
	}
	if err = w.Flush(); err != nil {
		return errors.New("auth: flush error " + err.Error())
	}
	return
}

func (conn *Connection) readAuthResponse(r io.Reader) (err error) {
	respBytes, err := conn.read(r)
	if err != nil {
		return errors.New("auth: read error " + err.Error())
	}
	resp := Response{buf: smallBuf{b: respBytes}}
	err = resp.decodeHeader(conn.dec)
	if err != nil {
		return errors.New("auth: decode response header error " + err.Error())
	}
	err = resp.decodeBody()
	if err != nil {
		switch err.(type) {
		case Error:
			return err
		default:
			return errors.New("auth: decode response body error " + err.Error())
		}
	}
	return
}

func (conn *Connection) createConnection(reconnect bool) (err error) {
	var reconnects uint64
	for conn.c == nil && atomic.LoadUint32(&conn.state) == connDisconnected {
		now := time.Now()
		err = conn.dial()
		if err == nil || !reconnect {
			if err == nil {
				conn.notify(Connected)
			}
			return
		}
		if conn.opts.MaxReconnects > 0 && reconnects > conn.opts.MaxReconnects {
			if conn.opts.Logger != nil {
				conn.opts.Logger.Report(LogLastReconnectFailed, conn, err)
			}
			err = ClientError{ErrConnectionClosed, "last reconnect failed"}
			// mark connection as closed to avoid reopening by another goroutine.
			return
		}
		if conn.opts.Logger != nil {
			conn.opts.Logger.Report(LogReconnectFailed, conn, reconnects, err)
		}
		conn.notify(ReconnectFailed)
		reconnects++
		conn.mutex.Unlock()
		time.Sleep(time.Until(now.Add(conn.opts.ReconnectDelay)))
		conn.mutex.Lock()
	}
	if atomic.LoadUint32(&conn.state) == connClosed {
		err = ClientError{ErrConnectionClosed, "using closed connection"}
	}
	return
}

func (conn *Connection) closeConnection(netErr error, forever bool) (err error) {
	conn.lockShards()
	defer conn.unlockShards()
	if forever {
		if atomic.LoadUint32(&conn.state) != connClosed {
			close(conn.control)
			atomic.StoreUint32(&conn.state, connClosed)
			conn.notify(Closed)
		}
	} else {
		atomic.StoreUint32(&conn.state, connDisconnected)
		conn.notify(Disconnected)
	}
	if conn.c != nil {
		err = conn.c.Close()
		conn.c = nil
	}
	for i := range conn.shard {
		conn.shard[i].buf = conn.shard[i].buf[:0]
		requests := &conn.shard[i].requests
		for pos := range requests {
			fut := requests[pos].first
			requests[pos].first = nil
			requests[pos].last = &requests[pos].first
			for fut != nil {
				fut.err = netErr
				fut.markReady(conn)
				fut, fut.next = fut.next, nil
			}
		}
	}
	return
}

func (conn *Connection) reconnect(netErr error, c net.Conn) {
	conn.mutex.Lock()
	defer conn.mutex.Unlock()
	if conn.opts.ReconnectDelay > 0 {
		if c == conn.c {
			_ = conn.closeConnection(netErr, false)
			if err := conn.createConnection(true); err != nil {
				_ = conn.closeConnection(err, true)
			}
		}
	} else {
		_ = conn.closeConnection(netErr, true)
	}
}

func (conn *Connection) lockShards() {
	for i := range conn.shard {
		conn.shard[i].mu.Lock()
		conn.shard[i].bufMu.Lock()
	}
}

func (conn *Connection) unlockShards() {
	for i := range conn.shard {
		conn.shard[i].mu.Unlock()
		conn.shard[i].bufMu.Unlock()
	}
}

func (conn *Connection) pingRoutine() {
	var pingCmd = Ping()
	to := conn.opts.ReadTimeout
	if to == 0 {
		to = 3 * time.Second
	}
	t := time.NewTicker(to / 3)
	defer t.Stop()
	for {
		select {
		case <-conn.control:
			return
		case <-t.C:
		}
		_, _ = conn.Exec(pingCmd)
	}
}

func (conn *Connection) notify(kind ConnEventKind) {
	if conn.opts.Notify != nil {
		select {
		case conn.opts.Notify <- ConnEvent{Kind: kind, Conn: conn, When: time.Now()}:
		default:
		}
	}
}

func (conn *Connection) writer(w *bufio.Writer, c net.Conn) {
	var shardNum uint32
	var packet smallWBuf
	for atomic.LoadUint32(&conn.state) != connClosed {
		select {
		case shardNum = <-conn.dirtyShard:
		default:
			runtime.Gosched()
			if len(conn.dirtyShard) == 0 {
				if err := w.Flush(); err != nil {
					conn.reconnect(err, c)
					return
				}
			}
			select {
			case shardNum = <-conn.dirtyShard:
			case <-conn.control:
				return
			}
		}
		shard := &conn.shard[shardNum]
		shard.bufMu.Lock()
		if conn.c != c {
			conn.dirtyShard <- shardNum
			shard.bufMu.Unlock()
			return
		}
		packet, shard.buf = shard.buf, packet
		shard.bufMu.Unlock()
		if len(packet) == 0 {
			continue
		}
		if err := write(w, packet); err != nil {
			conn.reconnect(err, c)
			return
		}
		packet = packet[0:0]
	}
}

func (conn *Connection) reader(r *bufio.Reader, c net.Conn) {
	for atomic.LoadUint32(&conn.state) != connClosed {
		respBytes, err := conn.read(r)
		if err != nil {
			conn.reconnect(err, c)
			return
		}
		resp := &Response{buf: smallBuf{b: respBytes}}
		err = resp.decodeHeader(conn.dec)
		if err != nil {
			conn.reconnect(err, c)
			return
		}
		if resp.Code == KeyPush {
			if fut := conn.peekFuture(resp.RequestID); fut != nil {
				fut.markPushReady(resp)
			} else {
				if conn.opts.Logger != nil {
					conn.opts.Logger.Report(LogUnexpectedResultID, conn, resp)
				}
			}
			continue
		}
		if fut := conn.fetchFuture(resp.RequestID); fut != nil {
			fut.resp = resp
			fut.markReady(conn)
		} else {
			if conn.opts.Logger != nil {
				conn.opts.Logger.Report(LogUnexpectedResultID, conn, resp)
			}
		}
	}
}

func (conn *Connection) newFuture(req *Request, withTimeout bool) (fut *futureImpl) {
	fut = &futureImpl{req: req, conn: conn}
	if conn.rLimit != nil && conn.opts.RLimitAction == RLimitDrop {
		select {
		case conn.rLimit <- struct{}{}:
		default:
			fut.err = ClientError{ErrRateLimited, "request is rate limited on client"}
			return
		}
	}
	fut.ready = make(chan struct{})
	fut.requestID = conn.nextRequestID()
	shardNum := fut.requestID & (conn.opts.Concurrency - 1)
	shard := &conn.shard[shardNum]
	shard.mu.Lock()

	switch atomic.LoadUint32(&conn.state) {
	case connClosed:
		fut.err = ClientError{ErrConnectionClosed, "using closed connection"}
		fut.ready = nil
		shard.mu.Unlock()
		return
	case connDisconnected:
		fut.err = ClientError{ErrConnectionNotReady, "client connection is not ready"}
		fut.ready = nil
		shard.mu.Unlock()
		return
	}
	pos := (fut.requestID / conn.opts.Concurrency) & (requestsMap - 1)
	pair := &shard.requests[pos]
	*pair.last = fut
	pair.last = &fut.next
	if withTimeout && conn.opts.RequestTimeout > 0 {
		fut.timeout = time.Since(epoch) + conn.opts.RequestTimeout
	}
	shard.mu.Unlock()
	if conn.rLimit != nil && conn.opts.RLimitAction == RLimitWait {
		select {
		case conn.rLimit <- struct{}{}:
		default:
			runtime.Gosched()
			select {
			case conn.rLimit <- struct{}{}:
			case <-fut.ready:
				if fut.err == nil {
					panic("fut.ready is closed, but err is nil")
				}
			}
		}
	}
	body, err := req.sendFunc(conn)
	if err != nil {
		fut.err = err
		fut.ready = nil
		return
	}
	conn.putFuture(fut, body)
	return
}

func (conn *Connection) putFuture(fut *futureImpl, body func(*msgpack.Encoder) error) {
	shardNum := fut.requestID & (conn.opts.Concurrency - 1)
	shard := &conn.shard[shardNum]
	shard.bufMu.Lock()
	select {
	case <-fut.ready:
		shard.bufMu.Unlock()
		return
	default:
	}
	firstWritten := len(shard.buf) == 0
	if cap(shard.buf) == 0 {
		shard.buf = make(smallWBuf, 0, 128)
		enc := msgpack.NewEncoder(&shard.buf)
		enc.UseArrayEncodedStructs(true)
		shard.enc = enc
	}
	bufLen := len(shard.buf)
	if err := fut.req.pack(fut.requestID, &shard.buf, shard.enc, body); err != nil {
		shard.buf = shard.buf[:bufLen]
		shard.bufMu.Unlock()
		if f := conn.fetchFuture(fut.requestID); f == fut {
			fut.markReady(conn)
			fut.err = err
		} else if f != nil {
			/* in theory, it is possible. In practice, you have
			 * to have race condition that lasts hours */
			panic("unknown future")
		} else {
			fut.wait()
			if fut.err == nil {
				panic("future removed from queue without error")
			}
			if _, ok := fut.err.(ClientError); ok {
				// packing error is more important than connection
				// error, because it is indication of programmer's
				// mistake.
				fut.err = err
			}
		}
		return
	}
	shard.bufMu.Unlock()
	if firstWritten {
		conn.dirtyShard <- shardNum
	}
}

func (conn *Connection) fetchFuture(reqID uint32) (fut *futureImpl) {
	shard := &conn.shard[reqID&(conn.opts.Concurrency-1)]
	shard.mu.Lock()
	fut = conn.fetchFutureImp(reqID)
	shard.mu.Unlock()
	return fut
}

func (conn *Connection) peekFuture(reqID uint32) (fut *futureImpl) {
	shard := &conn.shard[reqID&(conn.opts.Concurrency-1)]
	shard.mu.Lock()
	fut = conn.peekFutureImp(reqID)
	shard.mu.Unlock()
	return fut
}

func (conn *Connection) peekFutureImp(reqID uint32) *futureImpl {
	shard := &conn.shard[reqID&(conn.opts.Concurrency-1)]
	pos := (reqID / conn.opts.Concurrency) & (requestsMap - 1)
	pair := &shard.requests[pos]
	root := &pair.first
	for {
		fut := *root
		if fut == nil {
			return nil
		}
		if fut.requestID == reqID {
			return fut
		}
		root = &fut.next
	}
}

func (conn *Connection) fetchFutureImp(reqID uint32) *futureImpl {
	shard := &conn.shard[reqID&(conn.opts.Concurrency-1)]
	pos := (reqID / conn.opts.Concurrency) & (requestsMap - 1)
	pair := &shard.requests[pos]
	root := &pair.first
	for {
		fut := *root
		if fut == nil {
			return nil
		}
		if fut.requestID == reqID {
			*root = fut.next
			if fut.next == nil {
				pair.last = root
			} else {
				fut.next = nil
			}
			return fut
		}
		root = &fut.next
	}
}

func write(w io.Writer, data []byte) (err error) {
	l, err := w.Write(data)
	if err != nil {
		return
	}
	if l != len(data) {
		return errors.New("wrong length written")
	}
	return
}

func (conn *Connection) read(r io.Reader) (response []byte, err error) {
	var length int

	if _, err = io.ReadFull(r, conn.lenBuf[:]); err != nil {
		return
	}
	if conn.lenBuf[0] != 0xce {
		err = errors.New("wrong response header")
		return
	}
	length = (int(conn.lenBuf[1]) << 24) +
		(int(conn.lenBuf[2]) << 16) +
		(int(conn.lenBuf[3]) << 8) +
		int(conn.lenBuf[4])

	if length == 0 {
		err = errors.New("response should not be 0 length")
		return
	}
	response = make([]byte, length)
	_, err = io.ReadFull(r, response)

	return
}

func (conn *Connection) nextRequestID() (requestID uint32) {
	return atomic.AddUint32(&conn.requestID, 1)
}
