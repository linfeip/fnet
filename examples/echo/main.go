// echo is an example TCP echo server based on fnet.
//
//	cd examples && go run ./echo -addr :9000
package main

import (
	"flag"
	"log/slog"

	"github.com/linfeip/fnet"
)

type echo struct{}

func (echo) OnOpen(c fnet.Conn) {}

func (echo) OnData(c fnet.Conn, data []byte) int {
	c.Write(data)
	return len(data)
}

func (echo) OnClose(c fnet.Conn, err error) {}

func main() {
	addr := flag.String("addr", ":9000", "监听地址")
	flag.Parse()

	srv, err := fnet.NewServer(*addr, echo{}, fnet.Options{})
	if err != nil {
		panic(err)
	}
	slog.Info("echo server listening", "addr", srv.Addr().String())
	panic(srv.Serve())
}
