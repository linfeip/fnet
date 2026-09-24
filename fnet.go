// Package fnet is a networking framework on native event loops (epoll, kqueue,
// or the Windows emulation). Idle connections are parked on a poller and hold
// no goroutine.
//
// Layout: internal/netpoll is the platform layer and internal/reactor owns the
// event loops and connections. Package fhttp serves HTTP/1.x and HTTPS on top
// of them behind the standard http.Handler API, package websocket serves
// WebSocket on the same connections, and package pool runs their business
// code.
package fnet

import "github.com/linfeip/fnet/internal/reactor"

// ErrWriteBufferFull is returned by a connection write when its outbound queue
// (16 MiB) is full because the peer is not reading.
var ErrWriteBufferFull = reactor.ErrWriteBufferFull
