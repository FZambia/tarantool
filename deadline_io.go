package tarantool

import (
	"net"
	"time"
)

type deadlineIO struct {
	rto time.Duration
	wto time.Duration
	c   net.Conn
}

func (d *deadlineIO) Write(b []byte) (n int, err error) {
	if d.wto > 0 {
		if err = d.c.SetWriteDeadline(time.Now().Add(d.wto)); err != nil {
			return 0, err
		}
	}
	n, err = d.c.Write(b)
	return
}

func (d *deadlineIO) Read(b []byte) (n int, err error) {
	if d.rto > 0 {
		if err = d.c.SetReadDeadline(time.Now().Add(d.rto)); err != nil {
			return 0, err
		}
	}
	n, err = d.c.Read(b)
	return
}
