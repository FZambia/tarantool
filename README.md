[![Build Status](https://github.com/FZambia/tarantool/workflows/build/badge.svg?branch=master)](https://github.com/FZambia/tarantool/actions)
[![GoDoc](https://pkg.go.dev/badge/FZambia/tarantool)](https://pkg.go.dev/github.com/FZambia/tarantool)

# Tarantool client in Go language

The `tarantool` package allows communicating with [Tarantool 1.7.1+](http://tarantool.org/).

This is an opinionated modification of [github.com/tarantool/go-tarantool](https://github.com/tarantool/go-tarantool) package. The original license kept unchanged here at the moment.

## Differences from the original package

* API changed, some non-obvious (mostly to me personally) API removed.
* This package uses the latest msgpack library [github.com/vmihailenco/msgpack/v5](https://github.com/vmihailenco/msgpack) instead of `v2` in original.
* Uses `enc.UseArrayEncodedStructs(true)` for `msgpack.Encoder` internally so there is no need to define `msgpack:",as_array"` struct tags.
* Supports out-of-bound pushes (see [box.session.push](https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_session/#box-session-push))
* Adds optional support for `context.Context` (though performance will suffer a bit, if you want a maximum performance then use non-context methods which use per-connection timeout).
* Uses sync.Pool for `*msgpack.Decoder` to reduce allocations on decoding stage a bit. Actually this package allocates a bit more than the original one, but allocations are small and overall performance is comparable to the original (based on observations from internal benchmarks). 
* No `multi` and `queue` packages.
* Only one version of `Call` which uses Tarantool 1.7 request code.
* Modified connection address behavior: refer to `Connect` function docs to see details.
* Per-request timeout detached from underlying connection read and write timeouts.
* `Op` type to express different update/upsert operations.
* Some other cosmetic changes including several linter fixes.
* No default `Logger` – developer needs to provide custom implementation explicitly.

The networking core of `github.com/tarantool/go-tarantool` kept mostly unchanged at the moment so this package should behave in similar way.

## Installation

```
$ go get github.com/FZambia/tarantool
```

## Status

This library is a prototype for [Centrifuge](https://github.com/centrifugal/centrifuge)/[Centrifugo](https://github.com/centrifugal/centrifugo) ongoing Tarantool Engine experiment.

**API is not stable here** and can have changes as experiment evolves. Also, there are no concrete plans at the moment regarding the package maintenance.

The versioning politics before v1 will be the following: patch version updates will only contain backwards compatible changes, minor version updates may have backwards incompatible changes.

## Quick start

Create `example.lua` file with content:

```lua
box.cfg{listen = 3301}
box.schema.space.create('examples', {id = 999})
box.space.examples:create_index('primary', {type = 'hash', parts = {1, 'unsigned'}})
box.schema.user.grant('guest', 'read,write', 'space', 'examples')
```

Run it with Tarantool:

```
tarantool example.lua
```

Then create `main.go` file:

```go
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
```

Finally, run it with:

```
go run main.go
```
