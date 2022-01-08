package tarantool

import (
	"fmt"

	"github.com/vmihailenco/msgpack/v5"
)

type response struct {
	requestID uint32
	code      uint32
	error     string
	buf       smallBuf

	// Deserialized data for untyped requests.
	data []interface{}
}

func (resp *response) smallInt(d *msgpack.Decoder) (i int, err error) {
	b, err := resp.buf.ReadByte()
	if err != nil {
		return
	}
	if b <= 127 {
		return int(b), nil
	}
	err = resp.buf.UnreadByte()
	if err != nil {
		return
	}
	return d.DecodeInt()
}

func (resp *response) decodeHeader(d *msgpack.Decoder) (err error) {
	var l int
	d.Reset(&resp.buf)
	if l, err = d.DecodeMapLen(); err != nil {
		return
	}
	for ; l > 0; l-- {
		var cd int
		if cd, err = resp.smallInt(d); err != nil {
			return
		}
		switch cd {
		case KeySync:
			var rid uint64
			if rid, err = d.DecodeUint64(); err != nil {
				return
			}
			resp.requestID = uint32(rid)
		case KeyCode:
			var respCode uint64
			if respCode, err = d.DecodeUint64(); err != nil {
				return
			}
			resp.code = uint32(respCode)
		default:
			if err = d.Skip(); err != nil {
				return
			}
		}
	}
	return nil
}

func (resp *response) decodeBody() (err error) {
	if resp.buf.Len() > 2 {
		var l int
		d := getDecoder(&resp.buf)
		defer putDecoder(d)
		if l, err = d.DecodeMapLen(); err != nil {
			return err
		}
		for ; l > 0; l-- {
			var cd int
			if cd, err = resp.smallInt(d); err != nil {
				return err
			}
			switch cd {
			case KeyData:
				var res interface{}
				var ok bool
				if res, err = d.DecodeInterface(); err != nil {
					return err
				}
				if resp.data, ok = res.([]interface{}); !ok {
					return fmt.Errorf("result is not array: %v", res)
				}
			case KeyError:
				if resp.error, err = d.DecodeString(); err != nil {
					return err
				}
			default:
				if err = d.Skip(); err != nil {
					return err
				}
			}
		}
		if resp.code == KeyPush {
			return
		}
		if resp.code != okCode {
			resp.code &^= errorCodeBit
			err = Error{resp.code, resp.error}
		}
	}
	return
}

func (resp *response) decodeBodyTyped(res interface{}) (err error) {
	if resp.buf.Len() > 0 {
		var l int
		d := getDecoder(&resp.buf)
		defer putDecoder(d)
		if l, err = d.DecodeMapLen(); err != nil {
			return err
		}
		for ; l > 0; l-- {
			var cd int
			if cd, err = resp.smallInt(d); err != nil {
				return err
			}
			switch cd {
			case KeyData:
				if err = d.Decode(res); err != nil {
					return err
				}
			case KeyError:
				if resp.error, err = d.DecodeString(); err != nil {
					return err
				}
			default:
				if err = d.Skip(); err != nil {
					return err
				}
			}
		}
		if resp.code == KeyPush {
			return
		}
		if resp.code != okCode {
			resp.code &^= errorCodeBit
			err = Error{resp.code, resp.error}
		}
	}
	return
}

// String implements Stringer interface.
func (resp *response) String() (str string) {
	if resp.code == okCode {
		return fmt.Sprintf("<%d OK %v>", resp.requestID, resp.data)
	} else if resp.code == KeyPush {
		return fmt.Sprintf("<%d PUSH %v>", resp.requestID, resp.data)
	}
	return fmt.Sprintf("<%d ERR 0x%x %s>", resp.requestID, resp.code, resp.error)
}
