[![Build Status](https://github.com/FZambia/tarantool/workflows/build/badge.svg?branch=master)](https://github.com/FZambia/tarantool/actions)
[![GoDoc](https://pkg.go.dev/badge/FZambia/tarantool)](https://pkg.go.dev/github.com/FZambia/tarantool)

# Tarantool client in Go language

The `tarantool` package allows communicating with [Tarantool 1.7.1+](http://tarantool.org/).

This is an opinionated modification of [github.com/tarantool/go-tarantool](https://github.com/tarantool/go-tarantool) package, original license kept unchanged here at the moment.

Differences from the original package:

* API changed, some non-obvious (mostly to me personally) API removed.
* This package uses the latest msgpack library [github.com/vmihailenco/msgpack/v5](https://github.com/vmihailenco/msgpack) instead of `v2` in original.
* Uses `enc.UseArrayEncodedStructs(true)` for `msgpack.Encoder` internally so there is no need to define `msgpack:",as_array"` struct tags.
* Supports out-of-bound pushes (see [box.session.push](https://www.tarantool.io/ru/doc/2.5/reference/reference_lua/box_session/#box-session-push))
* Adds optional support for `context.Context` (though performance will suffer a bit, if you want a maximum performance then use non-context methods which use per-connection timeout).
* Uses sync.Pool for `*msgpack.Decoder` to reduce allocations on decoding stage a bit. Actually this package allocates a bit more than the original one, but allocations are small and overall performance is comparable to the original (based on observations from internal benchmarks). 
* No `multi` and `queue` packages.
* Only one version of `Call` which uses Tarantool 1.7 request code.
* Modified connection address behavior: refer to `Connect` function docs to see details.
* Per-request timeout detached from underlying connection read and write timeouts.

The networking core of `github.com/tarantool/go-tarantool` kept mostly unchanged at the moment so this package should behave in similar way.

## Installation

```
$ go get github.com/FZambia/tarantool
```

## Status

This library is a prototype for [Centrifuge](https://github.com/centrifugal/centrifuge)/[Centrifugo](https://github.com/centrifugal/centrifugo) ongoing Tarantool Engine experiment.

**API is not stable here** and can have changes as experiment evolves. Also, there are no concrete plans at the moment regarding the package maintenance.
