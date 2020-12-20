package tarantool

import (
	"context"
	"fmt"
	"time"
)

type Tuple struct {
	ID   uint
	Msg  string
	Name string
}

func exampleConnect() (*Connection, error) {
	conn, err := Connect(server, opts)
	if err != nil {
		return nil, err
	}
	_, err = conn.ExecContext(context.Background(), Replace(spaceNo, []interface{}{uint(1111), "hello", "world"}))
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	_, err = conn.ExecContext(context.Background(), Replace(spaceNo, []interface{}{uint(1112), "hallo", "werld"}))
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}

func ExampleConnection_Exec() {
	var conn *Connection
	conn, err := exampleConnect()
	if err != nil {
		fmt.Printf("error in prepare is %v", err)
		return
	}
	defer func() { _ = conn.Close() }()
	resp, err := conn.ExecContext(context.Background(), Select(512, 0, 0, 100, IterEq, []interface{}{uint(1111)}))
	if err != nil {
		fmt.Printf("error in select is %v", err)
		return
	}
	fmt.Printf("response is %#v\n", resp.Data)
	resp, err = conn.ExecContext(context.Background(), Select("test", "primary", 0, 100, IterEq, IntKey{1111}))
	if err != nil {
		fmt.Printf("error in select is %v", err)
		return
	}
	fmt.Printf("response is %#v\n", resp.Data)
	// Output:
	// response is []interface {}{[]interface {}{0x457, "hello", "world"}}
	// response is []interface {}{[]interface {}{0x457, "hello", "world"}}
}

func ExampleConnection_ExecTyped() {
	var conn *Connection
	conn, err := exampleConnect()
	if err != nil {
		fmt.Printf("error in prepare is %v", err)
		return
	}
	defer func() { _ = conn.Close() }()
	var res []Tuple
	err = conn.ExecTypedContext(context.Background(), Select(512, 0, 0, 100, IterEq, IntKey{1111}), &res)
	if err != nil {
		fmt.Printf("error in select is %v", err)
		return
	}
	fmt.Printf("response is %v\n", res)
	err = conn.ExecTypedContext(context.Background(), Select("test", "primary", 0, 100, IterEq, IntKey{1111}), &res)
	if err != nil {
		fmt.Printf("error in select is %v", err)
		return
	}
	fmt.Printf("response is %v\n", res)
	// Output:
	// response is [{1111 hello world}]
	// response is [{1111 hello world}]
}

func Example() {
	spaceNo := uint32(512)
	indexNo := uint32(0)

	server := "127.0.0.1:3301"
	opts := Opts{
		RequestTimeout: 50 * time.Millisecond,
		ReconnectDelay: 100 * time.Millisecond,
		MaxReconnects:  3,
		User:           "test",
		Password:       "test",
	}
	client, err := Connect(server, opts)
	if err != nil {
		fmt.Printf("failed to connect: %s", err.Error())
		return
	}

	resp, err := client.ExecContext(context.Background(), Ping())
	if err != nil {
		fmt.Printf("failed to ping: %s", err.Error())
		return
	}
	fmt.Println("Ping Code", resp.Code)
	fmt.Println("Ping Data", resp.Data)
	fmt.Println("Ping Error", err)

	// delete tuple for cleaning
	_, _ = client.ExecContext(context.Background(), Delete(spaceNo, indexNo, []interface{}{uint(10)}))
	_, _ = client.ExecContext(context.Background(), Delete(spaceNo, indexNo, []interface{}{uint(11)}))

	// insert new tuple { 10, 1 }
	resp, err = client.ExecContext(context.Background(), Insert(spaceNo, []interface{}{uint(10), "test", "one"}))
	fmt.Println("Insert Error", err)
	fmt.Println("Insert Code", resp.Code)
	fmt.Println("Insert Data", resp.Data)

	// insert new tuple { 11, 1 }
	resp, err = client.ExecContext(context.Background(), Insert("test", &Tuple{ID: 10, Msg: "test", Name: "one"}))
	fmt.Println("Insert Error", err)
	fmt.Println("Insert Code", resp.Code)
	fmt.Println("Insert Data", resp.Data)

	// delete tuple with primary key { 10 }
	resp, err = client.ExecContext(context.Background(), Delete(spaceNo, indexNo, []interface{}{uint(10)}))
	// or
	// resp, err = client.Delete("test", "primary", UintKey{10}})
	fmt.Println("Delete Error", err)
	fmt.Println("Delete Code", resp.Code)
	fmt.Println("Delete Data", resp.Data)

	// replace tuple with primary key 13
	// note, Tuple is defined within tests, and has EncodeMsgpack and DecodeMsgpack
	// methods
	resp, err = client.ExecContext(context.Background(), Replace(spaceNo, []interface{}{uint(13), 1}))
	fmt.Println("Replace Error", err)
	fmt.Println("Replace Code", resp.Code)
	fmt.Println("Replace Data", resp.Data)

	// update tuple with primary key { 13 }, incrementing second field by 3
	resp, err = client.ExecContext(context.Background(), Update("test", "primary", UintKey{13}, []Op{OpAdd(1, 3)}))
	// or
	// resp, err = client.Update(spaceNo, indexNo, []interface{}{uint(13)}, []interface{}{[]interface{}{"+", 1, 3}})
	fmt.Println("Update Error", err)
	fmt.Println("Update Code", resp.Code)
	fmt.Println("Update Data", resp.Data)

	// select just one tuple with primary key { 15 }
	resp, err = client.ExecContext(context.Background(), Select(spaceNo, indexNo, 0, 1, IterEq, []interface{}{uint(15)}))
	// or
	// resp, err = client.Select("test", "primary", 0, 1, IterEq, UintKey{15})
	fmt.Println("Select Error", err)
	fmt.Println("Select Code", resp.Code)
	fmt.Println("Select Data", resp.Data)

	// call function 'func_name' with arguments
	resp, err = client.ExecContext(context.Background(), Call("simple_incr", []interface{}{1}))
	fmt.Println("Call Error", err)
	fmt.Println("Call Code", resp.Code)
	fmt.Println("Call Data", resp.Data)

	// run raw lua code
	resp, err = client.ExecContext(context.Background(), Eval("return 1 + 2", []interface{}{}))
	fmt.Println("Eval Error", err)
	fmt.Println("Eval Code", resp.Code)
	fmt.Println("Eval Data", resp.Data)

	resp, err = client.ExecContext(context.Background(), Replace("test", &Tuple{ID: 11, Msg: "test", Name: "eleven"}))
	resp, err = client.ExecContext(context.Background(), Replace("test", &Tuple{ID: 12, Msg: "test", Name: "twelve"}))

	// Output:
	// Ping Code 0
	// Ping Data []
	// Ping Error <nil>
	// Insert Error <nil>
	// Insert Code 0
	// Insert Data [[10 test one]]
	// Insert Error Duplicate key exists in unique index 'primary' in space 'test' (0x3)
	// Insert Code 3
	// Insert Data []
	// Delete Error <nil>
	// Delete Code 0
	// Delete Data [[10 test one]]
	// Replace Error <nil>
	// Replace Code 0
	// Replace Data [[13 1]]
	// Update Error <nil>
	// Update Code 0
	// Update Data [[13 4]]
	// Select Error <nil>
	// Select Code 0
	// Select Data [[15 val 15 bla]]
	// Call Error <nil>
	// Call Code 0
	// Call Data [2]
	// Eval Error <nil>
	// Eval Code 0
	// Eval Data [3]
}
