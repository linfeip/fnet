package fhttp

import (
	"context"
	"net/http"
	"sync/atomic"
	"time"
)

// pooledContext is the context of the request a parsedRequest holds, kept inside it so that Request.Context needs no
// allocation: it passes everything on to the context of the requests of the connection the request is bound to (see
// conn.context), and is never canceled while unbound.
type pooledContext struct {
	connection atomic.Pointer[conn] // set while the request is served, see parsedRequest.bind
}

// context returns the context pooledContext passes on to.
func (c *pooledContext) context() context.Context {
	if conn := c.connection.Load(); conn != nil {
		return conn.context()
	}
	return context.Background()
}

func (c *pooledContext) Deadline() (time.Time, bool) { return time.Time{}, false }
func (c *pooledContext) Done() <-chan struct{}       { return c.context().Done() }
func (c *pooledContext) Err() error                  { return c.context().Err() }

// Value passes on to the connection's context too: that is how context.WithCancel, WithTimeout and AfterFunc find the
// cancelable context in it and register with it, instead of starting a goroutine for each context derived from the
// request's.
func (c *pooledContext) Value(key any) any { return c.context().Value(key) }

// connContext is the context of a connection's requests, canceled once the connection closes (see conn.onClose).
type connContext struct {
	context.Context
	cancel context.CancelFunc
}

// context returns the context of the connection's requests, making it when a Handler first asks for it. It is made
// while holding mu, against which onClose sets closed and cancels it.
//
// It holds the values the net/http server puts in a request's context, which some middleware relies on: the local
// address, and as the server the http.Server that serves the requests handed to net/http, with the same Handler.
func (c *conn) context() context.Context {
	if ctx := c.last.ctx.Load(); ctx != nil {
		return ctx.Context
	}
	values := context.WithValue(context.Background(), http.ServerContextKey, c.srv.streams)
	values = context.WithValue(values, http.LocalAddrContextKey, c.connection.LocalAddr()) // outside mu: a syscall
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.last.ctx.Load() == nil {
		ctx, cancel := context.WithCancel(values)
		c.last.ctx.Store(&connContext{Context: ctx, cancel: cancel})
		if c.closed.Load() {
			cancel()
		}
	}
	return c.last.ctx.Load().Context
}

// endContext cancels the context of the connection's requests once the connection has gone to a new protocol or to
// net/http, as on net/http, where a request's context ends with its Handler: fhttp has no request left that uses it, and
// onClose, which would cancel it otherwise, may never run. It is called by the worker, which made c.last.
func (c *conn) endContext() {
	if c.last != nil {
		if ctx := c.last.ctx.Load(); ctx != nil {
			ctx.cancel()
		}
	}
}
