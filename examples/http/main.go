// http is an example HTTP server based on fhttp, whose Handler is fully compatible with the standard library.
//
//	go run ./examples/http -addr :8080
//	curl localhost:8080/hello
package main

import (
	"flag"
	"io"
	"log/slog"
	"net/http"

	"github.com/linfeip/fnet/fhttp"
)

func main() {
	addr := flag.String("addr", ":8080", "监听地址")
	flag.Parse()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /hello", func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "hello world")
	})

	srv, err := fhttp.NewServer(*addr, mux, fhttp.Options{})
	if err != nil {
		panic(err)
	}
	slog.Info("http server listening", "addr", srv.Addr().String())
	panic(srv.Serve())
}
