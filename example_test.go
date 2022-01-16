package tarantool

import (
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
	_, err = conn.Exec(Replace(spaceNo, []interface{}{uint(1111), "hello", "world"}))
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	_, err = conn.Exec(Replace(spaceNo, []interface{}{uint(1112), "hallo", "werld"}))
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
	result, err := conn.Exec(Select(512, 0, 0, 100, IterEq, []interface{}{uint(1111)}))
	if err != nil {
		fmt.Printf("error in select is %v", err)
		return
	}
	fmt.Printf("result is %#v\n", result)
	result, err = conn.Exec(Select("test", "primary", 0, 100, IterEq, IntKey{1111}))
	if err != nil {
		fmt.Printf("error in select is %v", err)
		return
	}
	fmt.Printf("result is %#v\n", result)
	// Output:
	// result is []interface {}{[]interface {}{0x457, "hello", "world"}}
	// result is []interface {}{[]interface {}{0x457, "hello", "world"}}
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
	err = conn.ExecTyped(Select(512, 0, 0, 100, IterEq, IntKey{1111}), &res)
	if err != nil {
		fmt.Printf("error in select is %v", err)
		return
	}
	fmt.Printf("response is %v\n", res)
	err = conn.ExecTyped(Select("test", "primary", 0, 100, IterEq, IntKey{1111}), &res)
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

	result, err := client.Exec(Ping())
	if err != nil {
		fmt.Printf("failed to ping: %s", err.Error())
		return
	}
	fmt.Println("Ping Result", result)

	// Delete tuple for cleaning.
	_, _ = client.Exec(Delete(spaceNo, indexNo, []interface{}{uint(10)}))
	_, _ = client.Exec(Delete(spaceNo, indexNo, []interface{}{uint(11)}))

	// Insert new tuple { 10, 1 }.
	result, err = client.Exec(Insert(spaceNo, []interface{}{uint(10), "test", "one"}))
	fmt.Println("Insert Error", err)
	fmt.Println("Insert Result", result)

	// Insert new tuple { 11, 1 }.
	result, err = client.Exec(Insert("test", &Tuple{ID: 10, Msg: "test", Name: "one"}))
	fmt.Println("Insert Error", err)
	fmt.Println("Insert Result", result)

	// Delete tuple with primary key { 10 }.
	result, err = client.Exec(Delete(spaceNo, indexNo, []interface{}{uint(10)}))
	// or
	// result, err = client.Exec(Delete("test", "primary", UintKey{10}}))
	fmt.Println("Delete Error", err)
	fmt.Println("Delete Result", result)

	// Replace tuple with primary key 13.
	result, err = client.Exec(Replace(spaceNo, []interface{}{uint(13), 1}))
	fmt.Println("Replace Error", err)
	fmt.Println("Replace Result", result)

	// Update tuple with primary key { 13 }, incrementing second field by 3.
	result, err = client.Exec(Update("test", "primary", UintKey{13}, []Op{OpAdd(1, 3)}))
	// or
	// resp, err = client.Exec(Update(spaceNo, indexNo, []interface{}{uint(13)}, []Op{OpAdd(1, 3)}))
	fmt.Println("Update Error", err)
	fmt.Println("Update Result", result)

	// Select just one tuple with primary key { 15 }.
	result, err = client.Exec(Select(spaceNo, indexNo, 0, 1, IterEq, []interface{}{uint(15)}))
	// or
	// resp, err = client.Exec(Select("test", "primary", 0, 1, IterEq, UintKey{15}))
	fmt.Println("Select Error", err)
	fmt.Println("Select Result", result)

	// Call function 'func_name' with arguments.
	result, err = client.Exec(Call("simple_incr", []interface{}{1}))
	fmt.Println("Call Error", err)
	fmt.Println("Call Result", result)

	// Run raw lua code.
	result, err = client.Exec(Eval("return 1 + 2", []interface{}{}))
	fmt.Println("Eval Error", err)
	fmt.Println("Eval Result", result)

	// Output:
	// Ping Result []
	// Insert Error <nil>
	// Insert Result [[10 test one]]
	// Insert Error Duplicate key exists in unique index 'primary' in space 'test' (0x3)
	// Insert Result []
	// Delete Error <nil>
	// Delete Result [[10 test one]]
	// Replace Error <nil>
	// Replace Result [[13 1]]
	// Update Error <nil>
	// Update Result [[13 4]]
	// Select Error <nil>
	// Select Result [[15 val 15 bla]]
	// Call Error <nil>
	// Call Result [2]
	// Eval Error <nil>
	// Eval Result [3]
}
