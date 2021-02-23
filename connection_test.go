package tarantool

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRedisShard_OptsFromAddr(t *testing.T) {
	opts, err := optsFromAddr("127.0.0.1:3301", Opts{})
	require.NoError(t, err)
	require.Equal(t, "tcp", opts.network)
	require.Equal(t, "127.0.0.1:3301", opts.address)

	opts, err = optsFromAddr("localhost:3301", Opts{})
	require.NoError(t, err)
	require.Equal(t, "tcp", opts.network)
	require.Equal(t, "localhost:3301", opts.address)

	_, err = optsFromAddr("localhost:", Opts{})
	require.Error(t, err)

	opts, err = optsFromAddr("tcp://localhost:3301", Opts{})
	require.NoError(t, err)
	require.Equal(t, "tcp", opts.network)
	require.Equal(t, "localhost:3301", opts.address)

	opts, err = optsFromAddr("tcp://user:pass@localhost:3301", Opts{})
	require.NoError(t, err)
	require.Equal(t, "tcp", opts.network)
	require.Equal(t, "localhost:3301", opts.address)
	require.Equal(t, "user", opts.User)
	require.Equal(t, "pass", opts.Password)

	opts, err = optsFromAddr("tcp://localhost:3301/rest", Opts{})
	require.NoError(t, err)
	require.Equal(t, "tcp", opts.network)
	require.Equal(t, "localhost:3301", opts.address)
	require.Equal(t, "", opts.Password)

	opts, err = optsFromAddr("unix://user:pass@/var/run/tarantool/my_instance.sock", Opts{})
	require.NoError(t, err)
	require.Equal(t, "unix", opts.network)
	require.Equal(t, "/var/run/tarantool/my_instance.sock", opts.address)
	require.Equal(t, "pass", opts.Password)
	require.Equal(t, "user", opts.User)

	opts, err = optsFromAddr("tcp://[fe80::1]:3301", Opts{})
	require.NoError(t, err)
	require.Equal(t, "tcp", opts.network)
	require.Equal(t, "[fe80::1]:3301", opts.address)

	opts, err = optsFromAddr("redis://127.0.0.1:3301", Opts{})
	require.Error(t, err)

	opts, err = optsFromAddr("tcp://:pass@localhost:3301?connect_timeout=2s&read_timeout=3s&write_timeout=4s&request_timeout=5s&reconnect_delay=6s&max_reconnects=7&skip_schema=true", Opts{})
	require.NoError(t, err)
	require.Equal(t, "tcp", opts.network)
	require.Equal(t, "localhost:3301", opts.address)
	require.Equal(t, "pass", opts.Password)
	require.Equal(t, "", opts.User)
	require.Equal(t, 2*time.Second, opts.ConnectTimeout)
	require.Equal(t, 3*time.Second, opts.ReadTimeout)
	require.Equal(t, 4*time.Second, opts.WriteTimeout)
	require.Equal(t, 5*time.Second, opts.RequestTimeout)
	require.Equal(t, 6*time.Second, opts.ReconnectDelay)
	require.Equal(t, uint64(7), opts.MaxReconnects)
	require.True(t, opts.SkipSchema)
}
