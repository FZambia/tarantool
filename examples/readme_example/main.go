package main

import (
	"log"
	"time"

	"github.com/FZambia/tarantool"
)

type Row struct {
	ID    uint64
	Value string
}

func main() {
	opts := tarantool.Opts{
		RequestTimeout: 500 * time.Millisecond,
		User:           "guest",
	}
	conn, err := tarantool.Connect("127.0.0.1:3301", opts)
	if err != nil {
		log.Fatalf("Connection refused: %v", err)
	}
	defer func() { _ = conn.Close() }()

	_, err = conn.Exec(tarantool.Insert("examples", Row{ID: 999, Value: "hello"}))
	if err != nil {
		log.Fatalf("Insert failed: %v", err)
	}
	log.Println("Insert succeeded")
}
