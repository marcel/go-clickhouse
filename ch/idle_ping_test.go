package ch_test

import (
	"context"
	"io"
	"net"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/uptrace/go-clickhouse/ch"
)

// blackHoleProxy forwards TCP to a ClickHouse server until blackHole is
// called, after which the connections it already carries swallow every byte
// without closing, as a connection a NAT or load balancer forgot does. New
// connections still forward.
type blackHoleProxy struct {
	ln       net.Listener
	upstream string

	mu    sync.Mutex
	conns []*atomic.Bool
}

func newBlackHoleProxy(t *testing.T, upstream string) *blackHoleProxy {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	p := &blackHoleProxy{ln: ln, upstream: upstream}
	t.Cleanup(func() { _ = ln.Close() })
	go p.serve()
	return p
}

func (p *blackHoleProxy) serve() {
	for {
		client, err := p.ln.Accept()
		if err != nil {
			return
		}
		server, err := net.Dial("tcp", p.upstream)
		if err != nil {
			_ = client.Close()
			continue
		}
		dead := new(atomic.Bool)
		p.mu.Lock()
		p.conns = append(p.conns, dead)
		p.mu.Unlock()
		go pipe(server, client, dead)
		go pipe(client, server, dead)
	}
}

func pipe(dst io.Writer, src io.Reader, dead *atomic.Bool) {
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 && !dead.Load() {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func (p *blackHoleProxy) blackHole() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, dead := range p.conns {
		dead.Store(true)
	}
}

func (p *blackHoleProxy) dial() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.conns)
}

// proxiedDB connects through a black-hole proxy in front of the server in CH
// (default localhost:9000) with a single pooled connection.
func proxiedDB(t *testing.T, opts ...ch.Option) (*ch.DB, *blackHoleProxy) {
	dsn := os.Getenv("CH")
	if dsn == "" {
		dsn = "clickhouse://localhost:9000/default?sslmode=disable"
	}
	u, err := url.Parse(dsn)
	require.NoError(t, err)
	proxy := newBlackHoleProxy(t, u.Host)
	u.Host = proxy.ln.Addr().String()

	opts = append([]ch.Option{ch.WithDSN(u.String())}, opts...)
	opts = append(opts, ch.WithPoolSize(1), ch.WithMaxRetries(0))
	db := ch.Connect(opts...)
	t.Cleanup(func() { _ = db.Close() })
	return db, proxy
}

func selectOne(ctx context.Context, db *ch.DB) error {
	var n uint8
	return db.QueryRowContext(ctx, "SELECT 1").Scan(&n)
}

// idleLongEnough outlasts an idle-ping threshold of a second: the pool records
// use times in whole seconds.
const idleLongEnough = 2100 * time.Millisecond

func TestBlackHoledIdleConnHangsWithoutIdlePing(t *testing.T) {
	ctx := context.Background()
	db, proxy := proxiedDB(t, ch.WithReadTimeout(2*time.Second))
	require.NoError(t, selectOne(ctx, db))

	proxy.blackHole()
	time.Sleep(idleLongEnough)

	start := time.Now()
	err := selectOne(ctx, db)
	require.Error(t, err)
	require.Contains(t, err.Error(), "i/o timeout")
	require.GreaterOrEqual(t, time.Since(start), 2*time.Second,
		"the query waits out the read timeout on a conn that never answers")
}

func TestIdlePingReplacesBlackHoledConn(t *testing.T) {
	ctx := context.Background()
	db, proxy := proxiedDB(t,
		ch.WithReadTimeout(30*time.Second),
		ch.WithIdlePing(time.Second, 300*time.Millisecond))
	require.NoError(t, selectOne(ctx, db))
	require.Equal(t, 1, proxy.dial())

	proxy.blackHole()
	time.Sleep(idleLongEnough)

	start := time.Now()
	require.NoError(t, selectOne(ctx, db))
	require.Less(t, time.Since(start), 2*time.Second,
		"a failed ping costs its timeout, then the query runs on a fresh conn")
	require.Equal(t, 2, proxy.dial(), "the dead conn was replaced by a new dial")
}

func TestIdlePingSkipsRecentlyUsedConn(t *testing.T) {
	ctx := context.Background()
	db, proxy := proxiedDB(t,
		ch.WithIdlePing(time.Minute, 300*time.Millisecond))
	for i := 0; i < 3; i++ {
		require.NoError(t, selectOne(ctx, db))
	}
	require.Equal(t, 1, proxy.dial(), "a busy conn is reused without a ping")
}

func TestIdlePingKeepsLiveIdleConn(t *testing.T) {
	ctx := context.Background()
	db, proxy := proxiedDB(t,
		ch.WithIdlePing(time.Second, 300*time.Millisecond))
	require.NoError(t, selectOne(ctx, db))
	time.Sleep(idleLongEnough)
	require.NoError(t, selectOne(ctx, db))
	require.Equal(t, 1, proxy.dial(), "a live idle conn answers the ping and is kept")
}
