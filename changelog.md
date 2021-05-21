v0.2.2
======

* Up msgpack dependency

v0.2.1
======

* Fix type conversion causing panic on schema load - see [#3](https://github.com/FZambia/tarantool/issues/3)

v0.2.0
======

* Fix parsing connection address with non-ip host and without `tcp://` scheme, like `localhost:3301` - previously connecting with such an address resulted in configuration error

v0.1.1
======

* Fix calling ExecTypedContext w/o timeout - see [#1](https://github.com/FZambia/tarantool/pull/1)

v0.1.0
======

* Remove default logger - user must explicitly provide logger to get logs from a package

v0.0.1
======

This is an opinionated modification of [github.com/tarantool/go-tarantool](https://github.com/tarantool/go-tarantool) package.

Changes from the original:

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
