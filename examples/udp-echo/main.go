// udp-echo is an example UDP echo server based on fnet.
//
//	cd examples && go run ./udp-echo -addr :9000
package main

import (
	"flag"
	"log/slog"
	"net/netip"

	"github.com/linfeip/fnet"
)

type echo struct{}

func (echo) OnPacket(c fnet.UDPConn, data []byte, from netip.AddrPort) {
	// WriteToUDPAddrPort never blocks on Linux/macOS: when the socket send buffer is full it returns EAGAIN or
	// ENOBUFS and drops the datagram rather than queueing it in user space.
	_, _ = c.WriteToUDPAddrPort(data, from)
}

func main() {
	addr := flag.String("addr", ":9000", "listen address")
	flag.Parse()

	// ReusePort creates one UDP socket per P on Linux so the kernel hashes peers across sockets for parallel
	// callbacks while keeping one peer's datagrams in order; on macOS it uses a single socket.
	srv, err := fnet.NewUDPServer(*addr, echo{}, fnet.UDPOptions{ReusePort: true})
	if err != nil {
		panic(err)
	}
	slog.Info("udp echo server listening", "addr", srv.Addr().String())
	panic(srv.Serve())
}
