package tarantool

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/vmihailenco/msgpack/v5"
)

type Member struct {
	Name  string
	Nonce string
	Val   uint
}

type Tuple2 struct {
	Cid     uint
	Orig    string
	Members []Member
}

func (m *Member) EncodeMsgpack(e *msgpack.Encoder) error {
	_ = e.EncodeArrayLen(2)
	_ = e.EncodeString(m.Name)
	_ = e.EncodeUint(uint64(m.Val))
	return nil
}

func (m *Member) DecodeMsgpack(d *msgpack.Decoder) error {
	var err error
	var l int
	if l, err = d.DecodeArrayLen(); err != nil {
		return err
	}
	if l != 2 {
		return fmt.Errorf("array len doesn't match: %d", l)
	}
	if m.Name, err = d.DecodeString(); err != nil {
		return err
	}
	if m.Val, err = d.DecodeUint(); err != nil {
		return err
	}
	return nil
}

func (c *Tuple2) EncodeMsgpack(e *msgpack.Encoder) error {
	_ = e.EncodeArrayLen(3)
	_ = e.EncodeUint(uint64(c.Cid))
	_ = e.EncodeString(c.Orig)
	return e.Encode(c.Members)
}

func (c *Tuple2) DecodeMsgpack(d *msgpack.Decoder) error {
	var err error
	var l int
	if l, err = d.DecodeArrayLen(); err != nil {
		return err
	}
	if l != 3 {
		return fmt.Errorf("array len doesn't match: %d", l)
	}
	if c.Cid, err = d.DecodeUint(); err != nil {
		return err
	}
	if c.Orig, err = d.DecodeString(); err != nil {
		return err
	}
	if l, err = d.DecodeArrayLen(); err != nil {
		return err
	}
	c.Members = make([]Member, l)
	for i := 0; i < l; i++ {
		if err := d.Decode(&c.Members[i]); err != nil {
			return err
		}
	}
	return nil
}

var server = "127.0.0.1:3301"
var spaceNo = uint32(512)
var spaceName = "test"
var indexNo = uint32(0)
var indexName = "primary"

var opts = Opts{
	RequestTimeout: 500 * time.Millisecond,
	User:           "test",
	Password:       "test",
}

const N = 500

func BenchmarkClientSerial(b *testing.B) {
	var err error

	conn, err := Connect(server, opts)
	if err != nil {
		b.Errorf("No connection available")
		return
	}
	defer func() { _ = conn.Close() }()

	_, err = conn.Exec(Replace(spaceNo, []interface{}{uint(1111), "hello", "world"}))
	if err != nil {
		b.Errorf("No connection available")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = conn.Exec(Select(spaceNo, indexNo, 0, 1, IterEq, []interface{}{uint(1111)}))
		if err != nil {
			b.Errorf("No connection available")
		}
	}
}

func BenchmarkClientSerialTyped(b *testing.B) {
	var err error

	conn, err := Connect(server, opts)
	if err != nil {
		b.Errorf("No connection available")
		return
	}
	defer func() { _ = conn.Close() }()

	_, err = conn.Exec(Replace(spaceNo, []interface{}{uint(1111), "hello", "world"}))
	if err != nil {
		b.Errorf("No connection available")
	}

	var r []Tuple
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err = conn.ExecTypedContext(context.Background(), Select(spaceNo, indexNo, 0, 1, IterEq, IntKey{I: 1111}), &r)
		if err != nil {
			b.Errorf("No connection available")
		}
	}
}

func BenchmarkClientFuture(b *testing.B) {
	var err error

	conn, err := Connect(server, opts)
	if err != nil {
		b.Error(err)
		return
	}
	defer func() { _ = conn.Close() }()

	_, err = conn.Exec(Replace(spaceNo, []interface{}{uint(1111), "hello", "world"}))
	if err != nil {
		b.Error(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i += N {
		var fs [N]Future
		for j := 0; j < N; j++ {
			fs[j] = conn.ExecAsync(Select(spaceNo, indexNo, 0, 1, IterEq, []interface{}{uint(1111)}))
		}
		for j := 0; j < N; j++ {
			_, err = fs[j].Get()
			if err != nil {
				b.Error(err)
			}
		}

	}
}

func BenchmarkClientFutureTyped(b *testing.B) {
	var err error

	conn, err := Connect(server, opts)
	if err != nil {
		b.Errorf("No connection available")
		return
	}
	defer func() { _ = conn.Close() }()

	_, err = conn.Exec(Replace(spaceNo, []interface{}{uint(1111), "hello", "world"}))
	if err != nil {
		b.Errorf("No connection available")
	}
	b.ResetTimer()
	for i := 0; i < b.N; i += N {
		var fs [N]Future
		for j := 0; j < N; j++ {
			fs[j] = conn.ExecAsync(Select(spaceNo, indexNo, 0, 1, IterEq, IntKey{I: 1111}))
		}
		var r []Tuple
		for j := 0; j < N; j++ {
			err = fs[j].GetTyped(&r)
			if err != nil {
				b.Error(err)
			}
			if len(r) != 1 || r[0].ID != 1111 {
				b.Errorf("Doesn't match %v", r)
			}
		}
	}
}

func BenchmarkClientFutureParallel(b *testing.B) {
	var err error

	conn, err := Connect(server, opts)
	if err != nil {
		b.Errorf("No connection available")
		return
	}
	defer func() { _ = conn.Close() }()

	_, err = conn.Exec(Replace(spaceNo, []interface{}{uint(1111), "hello", "world"}))
	if err != nil {
		b.Errorf("No connection available")
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		exit := false
		for !exit {
			var fs [N]Future
			var j int
			for j = 0; j < N && pb.Next(); j++ {
				fs[j] = conn.ExecAsync(Select(spaceNo, indexNo, 0, 1, IterEq, []interface{}{uint(1111)}))
			}
			exit = j < N
			for j > 0 {
				j--
				_, err := fs[j].Get()
				if err != nil {
					b.Error(err)
					break
				}
			}
		}
	})
}

func BenchmarkClientFutureParallelTyped(b *testing.B) {
	var err error

	conn, err := Connect(server, opts)
	if err != nil {
		b.Errorf("No connection available")
		return
	}
	defer func() { _ = conn.Close() }()

	_, err = conn.Exec(Replace(spaceNo, []interface{}{uint(1111), "hello", "world"}))
	if err != nil {
		b.Errorf("No connection available")
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		exit := false
		for !exit {
			var fs [N]Future
			var j int
			for j = 0; j < N && pb.Next(); j++ {
				fs[j] = conn.ExecAsync(Select(spaceNo, indexNo, 0, 1, IterEq, IntKey{I: 1111}))
			}
			exit = j < N
			var r []Tuple
			for j > 0 {
				j--
				err := fs[j].GetTyped(&r)
				if err != nil {
					b.Error(err)
					break
				}
				if len(r) != 1 || r[0].ID != 1111 {
					b.Errorf("Doesn't match %v", r)
					break
				}
			}
		}
	})
}

func BenchmarkClientParallelTimeouts(b *testing.B) {
	var err error

	var options = Opts{
		RequestTimeout: time.Millisecond,
		User:           "test",
		Password:       "test",
		SkipSchema:     true,
	}

	conn, err := Connect(server, options)
	if err != nil {
		b.Errorf("No connection available")
		return
	}
	defer func() { _ = conn.Close() }()

	b.SetParallelism(1024)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := conn.Exec(Call("timeout", [][]interface{}{}))
			if err.(ClientError).Code != ErrTimedOut {
				b.Fatal(err.Error())
			}
		}
	})
}

func BenchmarkClientParallel(b *testing.B) {
	conn, err := Connect(server, opts)
	if err != nil {
		b.Errorf("No connection available")
		return
	}
	defer func() { _ = conn.Close() }()

	_, err = conn.Exec(Replace(spaceNo, []interface{}{uint(1111), "hello", "world"}))
	if err != nil {
		b.Errorf("No connection available")
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := conn.Exec(Select(spaceNo, indexNo, 0, 1, IterEq, []interface{}{uint(1111)}))
			if err != nil {
				b.Errorf("No connection available")
				break
			}
		}
	})
}

func BenchmarkClientParallelMassive(b *testing.B) {
	conn, err := Connect(server, opts)
	if err != nil {
		b.Errorf("No connection available")
		return
	}
	defer func() { _ = conn.Close() }()

	_, err = conn.Exec(Replace(spaceNo, []interface{}{uint(1111), "hello", "world"}))
	if err != nil {
		b.Errorf("No connection available")
	}

	var wg sync.WaitGroup
	limit := make(chan struct{}, 128*1024)
	for i := 0; i < 512; i++ {
		go func() {
			var r []Tuple
			for {
				if _, ok := <-limit; !ok {
					break
				}
				err = conn.ExecTyped(Select(spaceNo, indexNo, 0, 1, IterEq, IntKey{I: 1111}), &r)
				wg.Done()
				if err != nil {
					b.Errorf("No connection available")
				}
			}
		}()
	}
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		limit <- struct{}{}
	}
	wg.Wait()
	close(limit)
}

func BenchmarkClientParallelMassiveUntyped(b *testing.B) {
	conn, err := Connect(server, opts)
	if err != nil {
		b.Errorf("No connection available")
		return
	}
	defer func() { _ = conn.Close() }()

	_, err = conn.Exec(Replace(spaceNo, []interface{}{uint(1111), "hello", "world"}))
	if err != nil {
		b.Errorf("No connection available")
	}

	var wg sync.WaitGroup
	limit := make(chan struct{}, 128*1024)
	for i := 0; i < 512; i++ {
		go func() {
			for {
				if _, ok := <-limit; !ok {
					break
				}
				_, err = conn.Exec(Select(spaceNo, indexNo, 0, 1, IterEq, []interface{}{uint(1111)}))
				wg.Done()
				if err != nil {
					b.Errorf("No connection available")
				}
			}
		}()
	}
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		limit <- struct{}{}
	}
	wg.Wait()
	close(limit)
}

func TestClientBoxInfoCall(t *testing.T) {
	var resp *Response
	var err error
	var conn *Connection

	conn, err = Connect(server, opts)
	if err != nil {
		t.Errorf("Failed to connect: %s", err.Error())
		return
	}
	if conn == nil {
		t.Errorf("conn is nil after Connect")
		return
	}
	defer func() { _ = conn.Close() }()

	resp, err = conn.Exec(Call("box.info", []interface{}{"box.schema.SPACE_ID"}))
	if err != nil {
		t.Errorf("Failed to Call: %s", err.Error())
		return
	}
	if resp == nil {
		t.Errorf("Response is nil after Call")
		return
	}
	if resp.Data == nil {
		t.Errorf("Data is nil after Call")
		return
	}
	if len(resp.Data) < 1 {
		t.Errorf("Response.Data is empty after Call")
	}
}

func TestClient(t *testing.T) {
	var resp *Response
	var err error
	var conn *Connection

	conn, err = Connect(server, opts)
	if err != nil {
		t.Errorf("Failed to connect: %s", err.Error())
		return
	}
	if conn == nil {
		t.Errorf("conn is nil after Connect")
		return
	}
	defer func() { _ = conn.Close() }()

	// Ping.
	resp, err = conn.Exec(Ping())
	if err != nil {
		t.Errorf("Failed to Ping: %s", err.Error())
		return
	}
	if resp == nil {
		t.Errorf("Response is nil after Ping")
		return
	}

	// Insert.
	resp, err = conn.Exec(Insert(spaceNo, []interface{}{uint(1), "hello", "world"}))
	if err != nil {
		t.Errorf("Failed to Insert: %s", err.Error())
		return
	}
	if resp == nil {
		t.Errorf("Response is nil after Insert")
		return
	}
	if resp.Data == nil {
		t.Errorf("Data is nil after Insert")
		return
	}
	if len(resp.Data) != 1 {
		t.Errorf("Response Body len != 1")
	}
	if tpl, ok := resp.Data[0].([]interface{}); !ok {
		t.Errorf("Unexpected body of Insert")
	} else {
		if len(tpl) != 3 {
			t.Errorf("Unexpected body of Insert (tuple len)")
		}
		if id, ok := tpl[0].(int64); !ok || id != 1 {
			t.Errorf("Unexpected body of Insert (0)")
		}
		if h, ok := tpl[1].(string); !ok || h != "hello" {
			t.Errorf("Unexpected body of Insert (1)")
		}
	}
	_, err = conn.Exec(Insert(spaceNo, &Tuple{ID: 1, Msg: "hello", Name: "world"}))
	if tntErr, ok := err.(Error); !ok || tntErr.Code != ErrTupleFound {
		t.Errorf("Expected ErrTupleFound but got: %v", err)
		return
	}

	// Delete.
	resp, err = conn.Exec(Delete(spaceNo, indexNo, []interface{}{uint(1)}))
	if err != nil {
		t.Errorf("Failed to Delete: %s", err.Error())
		return
	}
	if resp == nil {
		t.Errorf("Response is nil after Delete")
		return
	}
	if resp.Data == nil {
		t.Errorf("Data is nil after Delete")
		return
	}
	if len(resp.Data) != 1 {
		t.Errorf("Response Body len != 1")
	}
	if tpl, ok := resp.Data[0].([]interface{}); !ok {
		t.Errorf("Unexpected body of Delete")
	} else {
		if len(tpl) != 3 {
			t.Errorf("Unexpected body of Delete (tuple len)")
		}
		if id, ok := tpl[0].(int64); !ok || id != 1 {
			t.Errorf("Unexpected body of Delete (0)")
		}
		if h, ok := tpl[1].(string); !ok || h != "hello" {
			t.Errorf("Unexpected body of Delete (1)")
		}
	}
	resp, err = conn.Exec(Delete(spaceNo, indexNo, []interface{}{uint(101)}))
	if err != nil {
		t.Errorf("Failed to Delete: %s", err.Error())
		return
	}
	if resp == nil {
		t.Errorf("Response is nil after Delete")
		return
	}
	if resp.Data == nil {
		t.Errorf("Data is nil after Insert")
		return
	}
	if len(resp.Data) != 0 {
		t.Errorf("Response Data len != 0")
	}

	// Replace.
	resp, err = conn.Exec(Replace(spaceNo, []interface{}{uint(2), "hello", "world"}))
	if err != nil {
		t.Errorf("Failed to Replace: %s", err.Error())
	}
	if resp == nil {
		t.Errorf("Response is nil after Replace")
	}
	resp, err = conn.Exec(Replace(spaceNo, []interface{}{uint(2), "hi", "planet"}))
	if err != nil {
		t.Errorf("Failed to Replace (duplicate): %s", err.Error())
		return
	}
	if resp == nil {
		t.Errorf("Response is nil after Replace (duplicate)")
		return
	}
	if resp.Data == nil {
		t.Errorf("Data is nil after Insert")
		return
	}
	if len(resp.Data) != 1 {
		t.Errorf("Response Data len != 1")
	}
	if tpl, ok := resp.Data[0].([]interface{}); !ok {
		t.Errorf("Unexpected body of Replace")
	} else {
		if len(tpl) != 3 {
			t.Errorf("Unexpected body of Replace (tuple len)")
		}
		if id, ok := tpl[0].(int64); !ok || id != 2 {
			t.Errorf("Unexpected body of Replace (0)")
		}
		if h, ok := tpl[1].(string); !ok || h != "hi" {
			t.Errorf("Unexpected body of Replace (1)")
		}
	}

	// Update.
	resp, err = conn.Exec(Update(spaceNo, indexNo, []interface{}{uint(2)}, []Op{OpAssign(1, "bye"), OpDelete(2, 1)}))
	if err != nil {
		t.Errorf("Failed to Update: %s", err.Error())
		return
	}
	if resp == nil {
		t.Errorf("Response is nil after Update")
		return
	}
	if resp.Data == nil {
		t.Errorf("Data is nil after Insert")
		return
	}
	if len(resp.Data) != 1 {
		t.Errorf("Response Data len != 1")
	}
	if tpl, ok := resp.Data[0].([]interface{}); !ok {
		t.Errorf("Unexpected body of Update")
	} else {
		if len(tpl) != 2 {
			t.Errorf("Unexpected body of Update (tuple len)")
		}
		if id, ok := tpl[0].(int64); !ok || id != 2 {
			t.Errorf("Unexpected body of Update (0)")
		}
		if h, ok := tpl[1].(string); !ok || h != "bye" {
			t.Errorf("Unexpected body of Update (1)")
		}
	}

	// Upsert.
	if strings.Compare(conn.greeting.Version, "Tarantool 1.6.7") >= 0 {
		resp, err = conn.Exec(Upsert(spaceNo, []interface{}{uint(3), 1}, []Op{OpAdd(1, 1)}))
		if err != nil {
			t.Errorf("Failed to Upsert (insert): %s", err.Error())
		}
		if resp == nil {
			t.Errorf("Response is nil after Upsert (insert)")
		}
		resp, err = conn.Exec(Upsert(spaceNo, []interface{}{uint(3), 1}, []Op{OpAdd(1, 1)}))
		if err != nil {
			t.Errorf("Failed to Upsert (update): %s", err.Error())
		}
		if resp == nil {
			t.Errorf("Response is nil after Upsert (update)")
		}
	}

	// Select.
	for i := 10; i < 20; i++ {
		_, err = conn.Exec(Replace(spaceNo, []interface{}{uint(i), fmt.Sprintf("val %d", i), "bla"}))
		if err != nil {
			t.Errorf("Failed to Replace: %s", err.Error())
		}
	}
	resp, err = conn.Exec(Select(spaceNo, indexNo, 0, 1, IterEq, []interface{}{uint(10)}))
	if err != nil {
		t.Errorf("Failed to Select: %s", err.Error())
		return
	}
	if resp == nil {
		t.Errorf("Response is nil after Select")
		return
	}
	if resp.Data == nil {
		t.Errorf("Data is nil after Select")
		return
	}
	if len(resp.Data) != 1 {
		t.Errorf("Response Data len != 1")
	}
	if tpl, ok := resp.Data[0].([]interface{}); !ok {
		t.Errorf("Unexpected body of Select")
	} else {
		if id, ok := tpl[0].(int64); !ok || id != 10 {
			t.Errorf("Unexpected body of Select (0)")
		}
		if h, ok := tpl[1].(string); !ok || h != "val 10" {
			t.Errorf("Unexpected body of Select (1)")
		}
	}

	// Select empty.
	resp, err = conn.Exec(Select(spaceNo, indexNo, 0, 1, IterEq, []interface{}{uint(30)}))
	if err != nil {
		t.Errorf("Failed to Select: %s", err.Error())
		return
	}
	if resp == nil {
		t.Errorf("Response is nil after Select")
		return
	}
	if resp.Data == nil {
		t.Errorf("Data is nil after Select")
		return
	}
	if len(resp.Data) != 0 {
		t.Errorf("Response Data len != 0")
	}

	// Select Typed.
	var tpl []Tuple
	err = conn.ExecTypedContext(context.Background(), Select(spaceNo, indexNo, 0, 1, IterEq, []interface{}{uint(10)}), &tpl)
	if err != nil {
		t.Errorf("Failed to SelectTyped: %s", err.Error())
	}
	if len(tpl) != 1 {
		t.Errorf("Result len of SelectTyped != 1")
	} else {
		if tpl[0].ID != 10 {
			t.Errorf("Bad value loaded from SelectTyped")
		}
	}

	// Select Typed for one tuple.
	var tpl1 [1]Tuple
	err = conn.ExecTypedContext(context.Background(), Select(spaceNo, indexNo, 0, 1, IterEq, []interface{}{uint(10)}), &tpl1)
	if err != nil {
		t.Errorf("Failed to SelectTyped: %s", err.Error())
	}
	if len(tpl) != 1 {
		t.Errorf("Result len of SelectTyped != 1")
	} else {
		if tpl[0].ID != 10 {
			t.Errorf("Bad value loaded from SelectTyped")
		}
	}

	// Select Typed Empty.
	var tpl2 []Tuple
	err = conn.ExecTypedContext(context.Background(), Select(spaceNo, indexNo, 0, 1, IterEq, []interface{}{uint(30)}), &tpl2)
	if err != nil {
		t.Errorf("Failed to SelectTyped: %s", err.Error())
	}
	if len(tpl2) != 0 {
		t.Errorf("Result len of SelectTyped != 1")
	}

	// Call.
	resp, err = conn.Exec(Call("simple_incr", []interface{}{1}))
	if err != nil {
		t.Errorf("Failed to Call: %s", err.Error())
		return
	}
	if resp.Data[0].(int64) != 2 {
		t.Errorf("result is not {{1}} : %v", resp.Data)
	}

	// Eval.
	resp, err = conn.Exec(Eval("return 5 + 6", []interface{}{}))
	if err != nil {
		t.Errorf("Failed to Eval: %s", err.Error())
		return
	}
	if resp == nil {
		t.Errorf("Response is nil after Eval")
		return
	}
	if resp.Data == nil {
		t.Errorf("Data is nil after Eval")
		return
	}
	if len(resp.Data) < 1 {
		t.Errorf("Response.Data is empty after Eval")
	}
	val := resp.Data[0].(int64)
	if val != 11 {
		t.Errorf("5 + 6 == 11, but got %v", val)
	}
}

func (schema *Schema) ResolveSpaceIndex(s interface{}, i interface{}) (spaceNo, indexNo uint32, err error) {
	return schema.resolveSpaceIndex(s, i)
}

func TestSchema(t *testing.T) {
	var err error
	var conn *Connection

	conn, err = Connect(server, opts)
	if err != nil {
		t.Errorf("Failed to connect: %s", err.Error())
		return
	}
	if conn == nil {
		t.Errorf("conn is nil after Connect")
		return
	}
	defer func() { _ = conn.Close() }()

	schema := conn.schema
	if schema.SpacesByID == nil {
		t.Errorf("schema.SpacesByID is nil")
	}
	if schema.Spaces == nil {
		t.Errorf("schema.Spaces is nil")
	}
	var space, space2 *Space
	var ok bool
	if space, ok = schema.SpacesByID[514]; !ok {
		t.Errorf("space with id = 514 was not found in schema.SpacesByID")
	}
	if space2, ok = schema.Spaces["schematest"]; !ok {
		t.Errorf("space with name 'schematest' was not found in schema.SpacesByID")
	}
	if space != space2 {
		t.Errorf("space with id = 514 and space with name schematest are different")
	}
	if space.ID != 514 {
		t.Errorf("space 514 has incorrect ID")
	}
	if space.Name != "schematest" {
		t.Errorf("space 514 has incorrect Name")
	}
	if !space.Temporary {
		t.Errorf("space 514 should be temporary")
	}
	if space.Engine != "memtx" {
		t.Errorf("space 514 engine should be memtx")
	}
	if space.FieldsCount != 7 {
		t.Errorf("space 514 has incorrect fields count")
	}

	if space.FieldsByID == nil {
		t.Errorf("space.FieldsByID is nill")
	}
	if space.Fields == nil {
		t.Errorf("space.Fields is nill")
	}
	if len(space.FieldsByID) != 6 {
		t.Errorf("space.FieldsByID len is incorrect")
	}
	if len(space.Fields) != 6 {
		t.Errorf("space.Fields len is incorrect")
	}

	var field1, field2, field5, field1n, field5n *Field
	if field1, ok = space.FieldsByID[1]; !ok {
		t.Errorf("field id = 1 was not found")
	}
	if field2, ok = space.FieldsByID[2]; !ok {
		t.Errorf("field id = 2 was not found")
	}
	if field5, ok = space.FieldsByID[5]; !ok {
		t.Errorf("field id = 5 was not found")
	}

	if field1n, ok = space.Fields["name1"]; !ok {
		t.Errorf("field name = name1 was not found")
	}
	if field5n, ok = space.Fields["name5"]; !ok {
		t.Errorf("field name = name5 was not found")
	}
	if field1 != field1n || field5 != field5n {
		t.Errorf("field with id = 1 and field with name 'name1' are different")
	}
	if field1.Name != "name1" {
		t.Errorf("field 1 has incorrect Name")
	}
	if field1.Type != "unsigned" {
		t.Errorf("field 1 has incorrect Type")
	}
	if field2.Name != "name2" {
		t.Errorf("field 2 has incorrect Name")
	}
	if field2.Type != "string" {
		t.Errorf("field 2 has incorrect Type")
	}

	if space.IndexesByID == nil {
		t.Errorf("space.IndexesByID is nill")
	}
	if space.Indexes == nil {
		t.Errorf("space.Indexes is nill")
	}
	if len(space.IndexesByID) != 2 {
		t.Errorf("space.IndexesByID len is incorrect")
	}
	if len(space.Indexes) != 2 {
		t.Errorf("space.Indexes len is incorrect")
	}

	var index0, index3, index0n, index3n *Index
	if index0, ok = space.IndexesByID[0]; !ok {
		t.Errorf("index id = 0 was not found")
	}
	if index3, ok = space.IndexesByID[3]; !ok {
		t.Errorf("index id = 3 was not found")
	}
	if index0n, ok = space.Indexes["primary"]; !ok {
		t.Errorf("index name = primary was not found")
	}
	if index3n, ok = space.Indexes["secondary"]; !ok {
		t.Errorf("index name = secondary was not found")
	}
	if index0 != index0n || index3 != index3n {
		t.Errorf("index with id = 3 and index with name 'secondary' are different")
	}
	if index3.ID != 3 {
		t.Errorf("index has incorrect ID")
	}
	if index0.Name != "primary" {
		t.Errorf("index has incorrect Name")
	}
	if index0.Type != "hash" || index3.Type != "tree" {
		t.Errorf("index has incorrect Type")
	}
	if !index0.Unique || index3.Unique {
		t.Errorf("index has incorrect Unique")
	}
	if index3.Fields == nil {
		t.Errorf("index.Fields is nil")
	}
	if len(index3.Fields) != 2 {
		t.Errorf("index.Fields len is incorrect")
	}

	iField1 := index3.Fields[0]
	iField2 := index3.Fields[1]
	if iField1 == nil || iField2 == nil {
		t.Errorf("index field is nil")
		return
	}
	if iField1.ID != 1 || iField2.ID != 2 {
		t.Errorf("index field has incorrect ID")
	}
	if (iField1.Type != "num" && iField1.Type != "unsigned") || (iField2.Type != "STR" && iField2.Type != "string") {
		t.Errorf("index field has incorrect Type '%s'", iField2.Type)
	}

	var rSpaceNo, rIndexNo uint32
	rSpaceNo, rIndexNo, err = schema.ResolveSpaceIndex(514, 3)
	if err != nil || rSpaceNo != 514 || rIndexNo != 3 {
		t.Errorf("numeric space and index params not resolved as-is")
	}
	rSpaceNo, _, err = schema.ResolveSpaceIndex(514, nil)
	if err != nil || rSpaceNo != 514 {
		t.Errorf("numeric space param not resolved as-is")
	}
	rSpaceNo, rIndexNo, err = schema.ResolveSpaceIndex("schematest", "secondary")
	if err != nil || rSpaceNo != 514 || rIndexNo != 3 {
		t.Errorf("symbolic space and index params not resolved")
	}
	rSpaceNo, _, err = schema.ResolveSpaceIndex("schematest", nil)
	if err != nil || rSpaceNo != 514 {
		t.Errorf("symbolic space param not resolved")
	}
	_, _, err = schema.ResolveSpaceIndex("schematest22", "secondary")
	if err == nil {
		t.Errorf("resolveSpaceIndex didn't returned error with not existing space name")
	}
	_, _, err = schema.ResolveSpaceIndex("schematest", "secondary22")
	if err == nil {
		t.Errorf("resolveSpaceIndex didn't returned error with not existing index name")
	}
}

func TestClientNamed(t *testing.T) {
	var resp *Response
	var err error
	var conn *Connection

	conn, err = Connect(server, opts)
	if err != nil {
		t.Errorf("Failed to connect: %s", err.Error())
		return
	}
	if conn == nil {
		t.Errorf("conn is nil after Connect")
		return
	}
	defer func() { _ = conn.Close() }()

	// Insert.
	_, err = conn.Exec(Insert(spaceName, []interface{}{uint(1001), "hello2", "world2"}))
	if err != nil {
		t.Errorf("Failed to Insert: %s", err.Error())
	}

	// Delete.
	resp, err = conn.Exec(Delete(spaceName, indexName, []interface{}{uint(1001)}))
	if err != nil {
		t.Errorf("Failed to Delete: %s", err.Error())
	}
	if resp == nil {
		t.Errorf("Response is nil after Delete")
	}

	// Replace.
	resp, err = conn.Exec(Replace(spaceName, []interface{}{uint(1002), "hello", "world"}))
	if err != nil {
		t.Errorf("Failed to Replace: %s", err.Error())
	}
	if resp == nil {
		t.Errorf("Response is nil after Replace")
	}

	// Update.
	resp, err = conn.Exec(Update(spaceName, indexName, []interface{}{uint(1002)}, []Op{OpAssign(1, "bye"), OpDelete(2, 1)}))
	if err != nil {
		t.Errorf("Failed to Update: %s", err.Error())
	}
	if resp == nil {
		t.Errorf("Response is nil after Update")
	}

	// Upsert.
	if strings.Compare(conn.greeting.Version, "Tarantool 1.6.7") >= 0 {
		resp, err = conn.Exec(Upsert(spaceName, []interface{}{uint(1003), 1}, []Op{OpAdd(1, 1)}))
		if err != nil {
			t.Errorf("Failed to Upsert (insert): %s", err.Error())
		}
		if resp == nil {
			t.Errorf("Response is nil after Upsert (insert)")
		}
		resp, err = conn.Exec(Upsert(spaceName, []interface{}{uint(1003), 1}, []Op{OpAdd(1, 1)}))
		if err != nil {
			t.Errorf("Failed to Upsert (update): %s", err.Error())
		}
		if resp == nil {
			t.Errorf("Response is nil after Upsert (update)")
		}
	}

	// Select.
	for i := 1010; i < 1020; i++ {
		_, err = conn.Exec(Replace(spaceName, []interface{}{uint(i), fmt.Sprintf("val %d", i), "bla"}))
		if err != nil {
			t.Errorf("Failed to Replace: %s", err.Error())
		}
	}
	resp, err = conn.Exec(Select(spaceName, indexName, 0, 1, IterEq, []interface{}{uint(1010)}))
	if err != nil {
		t.Errorf("Failed to Select: %s", err.Error())
	}
	if resp == nil {
		t.Errorf("Response is nil after Select")
	}

	// Select Typed.
	var tpl []Tuple
	err = conn.ExecTypedContext(context.Background(), Select(spaceName, indexName, 0, 1, IterEq, []interface{}{uint(1010)}), &tpl)
	if err != nil {
		t.Errorf("Failed to SelectTyped: %s", err.Error())
	}
	if len(tpl) != 1 {
		t.Errorf("Result len of SelectTyped != 1")
	}
}

func TestComplexStructs(t *testing.T) {
	var err error
	var conn *Connection

	conn, err = Connect(server, opts)
	if err != nil {
		t.Errorf("Failed to connect: %s", err.Error())
		return
	}
	if conn == nil {
		t.Errorf("conn is nil after Connect")
		return
	}
	defer func() { _ = conn.Close() }()

	tuple := Tuple2{Cid: 777, Orig: "orig", Members: []Member{{"lol", "", 1}, {"wut", "", 3}}}
	_, err = conn.Exec(Replace(spaceNo, &tuple))
	if err != nil {
		t.Errorf("Failed to insert: %s", err.Error())
		return
	}

	var tuples [1]Tuple2
	err = conn.ExecTypedContext(context.Background(), Select(spaceNo, indexNo, 0, 1, IterEq, []interface{}{777}), &tuples)
	if err != nil {
		t.Errorf("Failed to selectTyped: %s", err.Error())
		return
	}

	if tuple.Cid != tuples[0].Cid || len(tuple.Members) != len(tuples[0].Members) || tuple.Members[1].Name != tuples[0].Members[1].Name {
		t.Errorf("Failed to selectTyped: incorrect data")
		return
	}
}

func TestExecContext(t *testing.T) {
	assert := assert.New(t)

	var err error
	var connWithTimeout *Connection
	var connNoTimeout *Connection
	var result []interface{}

	var ctx context.Context
	var cancel context.CancelFunc

	// long request
	req := Eval("require('fiber').sleep(0.5)", []interface{}{})

	// connection w/o request timeout
	connNoTimeout, err = Connect(server, Opts{
		User:     opts.User,
		Password: opts.Password,
	})
	assert.Nil(err)
	assert.NotNil(connNoTimeout)

	defer connNoTimeout.Close()

	// exec without timeout - shouldn't fail
	err = connNoTimeout.ExecTypedContext(
		context.Background(),
		req,
		&result,
	)
	assert.Nil(err)

	_, err = connNoTimeout.ExecContext(
		context.Background(),
		req,
	)
	assert.Nil(err)

	// exec with timeout - should fail
	ctx, cancel = context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err = connNoTimeout.ExecTypedContext(
		ctx,
		req,
		&result,
	)

	assert.NotNil(err)
	assert.Contains(err.Error(), "context deadline exceeded")

	ctx, cancel = context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_, err = connNoTimeout.ExecContext(
		ctx,
		req,
	)
	assert.NotNil(err)
	assert.Contains(err.Error(), "context deadline exceeded")

	// connection w/ request timeout
	connWithTimeout, err = Connect(server, Opts{
		User:           opts.User,
		Password:       opts.Password,
		RequestTimeout: 200 * time.Millisecond,
	})
	assert.Nil(err)
	assert.NotNil(connWithTimeout)

	defer connWithTimeout.Close()

	// exec without timeout - should fail
	err = connWithTimeout.ExecTypedContext(
		context.Background(),
		req,
		&result,
	)
	assert.NotNil(err)
	assert.Contains(err.Error(), "context deadline exceeded")

	_, err = connWithTimeout.ExecContext(
		context.Background(),
		req,
	)
	assert.NotNil(err)
	assert.Contains(err.Error(), "context deadline exceeded")

	// exec with timeout - should fail
	ctx, cancel = context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err = connWithTimeout.ExecTypedContext(
		ctx,
		req,
		&result,
	)

	assert.NotNil(err)
	assert.Contains(err.Error(), "context deadline exceeded")

	ctx, cancel = context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_, err = connWithTimeout.ExecContext(
		ctx,
		req,
	)
	assert.NotNil(err)
	assert.Contains(err.Error(), "context deadline exceeded")
}
