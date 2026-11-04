// udp-punch demonstrates UDP hole punching (NAT traversal) with UDPServer.Conn:
// each peer sends outbound probes from its bound UDP port before receiving any packet.
//
// Public endpoints in real deployments are discovered via the application's own STUN/signaling service; fnet does not
// implement NAT discovery itself, and provides UDPServer.Conn so probes leave from the listening port.
//
// Run two instances in separate terminals (or in background):
//
//	cd examples && go run ./udp-punch -listen 127.0.0.1:9001 -peer 127.0.0.1:9002
//	cd examples && go run ./udp-punch -listen 127.0.0.1:9002 -peer 127.0.0.1:9001
package main

import (
	"errors"
	"flag"
	"log/slog"
	"net/netip"
	"sync/atomic"
	"time"

	"github.com/linfeip/fnet"
)

type punchHandler struct {
	acknowledged atomic.Bool
	done         chan struct{}
}

func (h *punchHandler) OnPacket(c fnet.UDPConn, data []byte, from netip.AddrPort) {
	msg := string(data)
	slog.Info("received packet", "local", c.LocalAddr().String(), "from", from.String(), "msg", msg)
	switch msg {
	case "punch":
		_, _ = c.WriteToUDPAddrPort([]byte("punch-ack"), from)
	case "punch-ack":
		if h.acknowledged.CompareAndSwap(false, true) {
			_, _ = c.WriteToUDPAddrPort([]byte("punch-ack"), from)
			close(h.done)
		}
	}
}

func main() {
	listenAddr := flag.String("listen", "127.0.0.1:9001", "local UDP listen address")
	peerFlag := flag.String("peer", "127.0.0.1:9002", "peer UDP address obtained from signaling/STUN")
	flag.Parse()

	peerAddr, err := netip.ParseAddrPort(*peerFlag)
	if err != nil {
		panic(err)
	}

	handler := &punchHandler{done: make(chan struct{})}
	srv, err := fnet.NewUDPServer(*listenAddr, handler, fnet.UDPOptions{})
	if err != nil {
		panic(err)
	}

	go func() {
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		// Send the first probe immediately before any packet has been received.
		_, _ = srv.Conn().WriteToUDPAddrPort([]byte("punch"), peerAddr)
		for {
			select {
			case <-ticker.C:
				_, _ = srv.Conn().WriteToUDPAddrPort([]byte("punch"), peerAddr)
			case <-handler.done:
				slog.Info("hole punch succeeded, shutting down", "local", srv.Addr().String(), "peer", peerAddr.String())
				_ = srv.Close()
				return
			}
		}
	}()

	slog.Info("punching toward peer", "local", srv.Addr().String(), "peer", peerAddr.String())
	if err := srv.Serve(); err != nil && !errors.Is(err, fnet.ErrServerClosed) {
		panic(err)
	}
}
