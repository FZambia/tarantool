package tarantool

import (
	"io"
	"sync"

	"github.com/vmihailenco/msgpack/v5"
)

var decoderPool sync.Pool

func getDecoder(r io.Reader) *msgpack.Decoder {
	v := decoderPool.Get()
	if v == nil {
		d := msgpack.NewDecoder(r)
		d.SetMapDecoder(func(dec *msgpack.Decoder) (interface{}, error) {
			return dec.DecodeUntypedMap()
		})
		d.UseLooseInterfaceDecoding(true)
		return d
	}
	d := v.(*msgpack.Decoder)
	d.Reset(r)
	d.SetMapDecoder(func(dec *msgpack.Decoder) (interface{}, error) {
		return dec.DecodeUntypedMap()
	})
	d.UseLooseInterfaceDecoding(true)
	return d
}

func putDecoder(d *msgpack.Decoder) {
	decoderPool.Put(d)
}
