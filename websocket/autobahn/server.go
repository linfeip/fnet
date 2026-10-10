// autobahn is an echo server for the Autobahn TestSuite (fuzzingclient mode) to test websocket with; for usage see
// README.md in the same directory.
//
//	go run ./websocket/autobahn -addr :9001
package main

import (
	"flag"
	"log/slog"
	"net/http"

	"github.com/linfeip/fnet/fhttp"
	"github.com/linfeip/fnet/internal/units"
	"github.com/linfeip/fnet/websocket"

	"github.com/gobwas/ws"
)

type echo struct{}

func (echo) OnOpen(c *websocket.Conn)                               {}
func (echo) OnMessage(c *websocket.Conn, op ws.OpCode, data []byte) { c.WriteMessage(op, data) }
func (echo) OnClose(c *websocket.Conn, err error)                   {}

func main() {
	addr := flag.String("addr", ":9001", "listen address")
	flag.Parse()

	// The largest message in 9.* is 16MB, the default 1MB is not enough.
	opts := websocket.Options{MaxMessageSize: 16 * units.MB}
	srv, err := fhttp.NewServer(*addr, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		websocket.Upgrade(w, r, echo{}, opts)
	}), fhttp.Options{})
	if err != nil {
		panic(err)
	}
	slog.Info("autobahn echo server listening", "addr", srv.Addr().String())
	panic(srv.Serve())
}
