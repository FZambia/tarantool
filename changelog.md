v0.3.1
======

* Fix `panic: close of nil channel` after closing connection after an error - [#10](https://github.com/FZambia/tarantool/issues/10)

v0.3.0
======

* Removing `Response` type from public API – see [#8](https://github.com/FZambia/tarantool/pull/8) for details.

```
> gorelease -base v0.2.3 -version v0.3.0
# github.com/FZambia/tarantool
## incompatible changes
(*Connection).Exec: changed from func(*Request) (*Response, error) to func(*Request) ([]interface{}, error)
(*Connection).ExecContext: changed from func(context.Context, *Request) (*Response, error) to func(context.Context, *Request) ([]interface{}, error)
(*Request).WithPush: changed from func(func(*Response)) *Request to func(func([]interface{})) *Request
ErrorCodeBit: removed
Future.Get: changed from func() (*Response, error) to func() ([]interface{}, error)
FutureContext.GetContext: changed from func(context.Context) (*Response, error) to func(context.Context) ([]interface{}, error)
OkCode: removed
Response: removed

# summary
v0.3.0 is a valid semantic version for this release.
```

v0.2.3
======

* Add `DisableArrayEncodedStructs` option.

```
> gorelease -base v0.2.2 -version v0.2.3
github.com/FZambia/tarantool
----------------------------
Compatible changes:
- Opts.DisableArrayEncodedStructs: added

v0.2.3 is a valid semantic version for this release.
```

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
