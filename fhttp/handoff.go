package fhttp

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"sync"

	"github.com/linfeip/fnet/internal/httpx"
)

// A request fhttp cannot handle on its buffered path (see errHandoff) is served by net/http: the worker detaches the
// connection from fnet (fnet.Conn.Detach) and hands it to an http.Server through a handoffListener, and that server
// does everything for it, reading the body as it arrives. Keep-alive is off on that server, so the connection closes
// after this request and the client's next connection is served by fnet again.

// handoff gives the connection to net/http from the request at the front of its unconsumed data on. The worker gets
// here only after writing the responses to the earlier requests, so the responses keep their order.
func (c *conn) handoff() {
	// Detach writes out what the engine still holds of those responses, which IdleTimeout bounds, as it does on the fnet
	// path: the client may not be reading.
	c.connection.SetDeadline(httpx.DeadlineAfter(c.srv.opts.IdleTimeout))
	nc, err := c.connection.Detach()
	if err != nil {
		return // the connection is closed or closing
	}
	c.endContext() // that of the earlier requests, whose Handlers have returned
	c.srv.handoffs.handoff(nc)
}

// validRequests serves with handler the requests net/http reads, but for the ones that fail what fhttp's parser checks
// beyond the net/http server (see validStdRequest): an invalid Host, a CONNECT to no port. They are answered 400.
func validRequests(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !validHost(r.Host) || !validConnectTarget(r) {
			http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
			return
		}
		handler.ServeHTTP(w, r)
	})
}

// tlsStateKey is the context key of a handed-off TLS connection's state, see serveTLSState.
type tlsStateKey struct{}

// serveTLSState has the stream server s set Request.TLS on the handed-off TLS connections, which wrap a *tls.Conn (see
// ftls.Conn.Detach): before Go 1.25, net/http only does so for a *tls.Conn itself.
func serveTLSState(s *http.Server) {
	s.ConnContext = func(ctx context.Context, c net.Conn) context.Context {
		state := c.(tlsConnection).ConnectionState()
		return context.WithValue(ctx, tlsStateKey{}, &state)
	}
	handler := s.Handler
	s.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.TLS = r.Context().Value(tlsStateKey{}).(*tls.ConnectionState)
		handler.ServeHTTP(w, r)
	})
}

// handoffListener is the net.Listener the stream server accepts the handed-off connections from.
type handoffListener struct {
	addr  net.Addr
	conns chan net.Conn
	done  chan struct{}
	once  sync.Once
}

func newHandoffListener(addr net.Addr) *handoffListener {
	return &handoffListener{addr: addr, conns: make(chan net.Conn), done: make(chan struct{})}
}

// handoff passes c to the stream server, or closes it once the server is closed.
func (l *handoffListener) handoff(c net.Conn) {
	select {
	case l.conns <- c:
	case <-l.done:
		c.Close()
	}
}

func (l *handoffListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.conns:
		return c, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}

func (l *handoffListener) Close() error {
	l.once.Do(func() { close(l.done) })
	return nil
}

func (l *handoffListener) Addr() net.Addr { return l.addr }
