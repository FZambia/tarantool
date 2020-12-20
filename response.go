package tarantool

import (
	"fmt"

	"github.com/vmihailenco/msgpack/v5"
)

type Response struct {
	RequestID uint32
	Code      uint32
	Error     string
	// Data contains deserialized data for untyped requests.
	Data []interface{}
	buf  smallBuf
}

func (resp *Response) smallInt(d *msgpack.Decoder) (i int, err error) {
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

func (resp *Response) decodeHeader(d *msgpack.Decoder) (err error) {
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
			resp.RequestID = uint32(rid)
		case KeyCode:
			var respCode uint64
			if respCode, err = d.DecodeUint64(); err != nil {
				return
			}
			resp.Code = uint32(respCode)
		default:
			if err = d.Skip(); err != nil {
				return
			}
		}
	}
	return nil
}

func (resp *Response) decodeBody() (err error) {
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
				if resp.Data, ok = res.([]interface{}); !ok {
					return fmt.Errorf("result is not array: %v", res)
				}
			case KeyError:
				if resp.Error, err = d.DecodeString(); err != nil {
					return err
				}
			default:
				if err = d.Skip(); err != nil {
					return err
				}
			}
		}
		if resp.Code == KeyPush {
			return
		}
		if resp.Code != OkCode {
			resp.Code &^= ErrorCodeBit
			err = Error{resp.Code, resp.Error}
		}
	}
	return
}

func (resp *Response) decodeBodyTyped(res interface{}) (err error) {
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
				if resp.Error, err = d.DecodeString(); err != nil {
					return err
				}
			default:
				if err = d.Skip(); err != nil {
					return err
				}
			}
		}
		if resp.Code == KeyPush {
			return
		}
		if resp.Code != OkCode {
			resp.Code &^= ErrorCodeBit
			err = Error{resp.Code, resp.Error}
		}
	}
	return
}

// String implements Stringer interface.
func (resp *Response) String() (str string) {
	if resp.Code == OkCode {
		return fmt.Sprintf("<%d OK %v>", resp.RequestID, resp.Data)
	} else if resp.Code == KeyPush {
		return fmt.Sprintf("<%d PUSH %v>", resp.RequestID, resp.Data)
	}
	return fmt.Sprintf("<%d ERR 0x%x %s>", resp.RequestID, resp.Code, resp.Error)
}
