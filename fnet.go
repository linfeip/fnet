// Package fnet is a networking framework on native event loops (epoll, kqueue,
// or the Windows emulation). Idle connections are parked on a poller and hold
// no goroutine.
//
// Server serves a message protocol over TCP: a Split function frames each
// connection's byte stream on the event loop, and OnOpen, OnMessage and
// OnClose run on the worker pool, one call at a time per connection, in
// order. Package fhttp serves HTTP/1.x and HTTPS behind the standard
// http.Handler API, package websocket serves WebSocket on the same
// connections, and package pool runs their business code.
//
// Layout: internal/netpoll is the platform layer and internal/reactor owns the
// event loops and connections; everything above them is a protocol.
package fnet

import "github.com/linfeip/fnet/internal/reactor"

// ErrWriteBufferFull is returned by a connection write when its outbound queue
// is full because the peer is not reading (see Server.MaxOutboundBytes).
var ErrWriteBufferFull = reactor.ErrWriteBufferFull
