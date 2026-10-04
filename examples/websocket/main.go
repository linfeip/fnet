// websocket is an example WebSocket echo server based on fnet's websocket package.
//
//	cd examples && go run ./websocket -addr :8080
//	websocket ws://localhost:8080/ws
package main

import (
	"flag"
	"log/slog"
	"net/http"

	"github.com/linfeip/fnet/fhttp"
	"github.com/linfeip/fnet/websocket"

	"github.com/gobwas/ws"
)

type echo struct{}

func (echo) OnOpen(c *websocket.Conn)                               {}
func (echo) OnMessage(c *websocket.Conn, op ws.OpCode, data []byte) { c.WriteMessage(op, data) }
func (echo) OnClose(c *websocket.Conn, err error)                   {}

func main() {
	addr := flag.String("addr", ":8080", "监听地址")
	flag.Parse()

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		websocket.Upgrade(w, r, echo{}, websocket.Options{})
	})

	srv, err := fhttp.NewServer(*addr, mux, fhttp.Options{})
	if err != nil {
		panic(err)
	}
	slog.Info("websocket server listening", "addr", srv.Addr().String())
	panic(srv.Serve())
}
