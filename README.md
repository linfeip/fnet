# fnet

English | [中文](README.zh-CN.md)

A high-performance HTTP/HTTPS and WebSocket server for Go. I/O is driven by a native poller (**epoll** on Linux, **kqueue** on Darwin, **WSAPoll** on Windows). TLS and HTTP parsing reuse the Go standard library, so handlers stay `net/http` compatible.

## Features

- Drop-in `http.Handler` / `http.ServeMux` API (`ListenAndServe`, `ListenAndServeTLS`)
- HTTP and HTTPS, keep-alive, optional read/write/idle timeouts
- Event-driven WebSocket: idle connections hold **zero goroutines**; frames are dispatched when complete
- Handshake via [gobwas/ws](https://github.com/gobwas/ws) (RFC 6455)
- Linux, macOS, and Windows

## Install

```bash
go get github.com/linfeip/fnet
```

Requires Go 1.26+.

## HTTP

```go
package main

import (
	"io"
	"log"
	"net/http"

	"github.com/linfeip/fnet"
)

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "hello, fnet\n")
	})

	log.Fatal(fnet.ListenAndServe(":8080", mux))
}
```

HTTPS with a configured `Server`:

```go
srv := &fnet.Server{
	Addr:    ":8443",
	Handler: mux,
}
log.Fatal(srv.ListenAndServeTLS("cert.pem", "key.pem"))
```

Optional fields: `TLSConfig`, `ReadTimeout`, `WriteTimeout`, `IdleTimeout`.

## WebSocket (event-driven)

When `OnMessage` is set, `Upgrade` registers the connection on the poller and the HTTP handler can return immediately. Idle sockets do not occupy a goroutine.

```go
package main

import (
	"log"
	"net/http"

	"github.com/linfeip/fnet"
	"github.com/linfeip/fnet/websocket"
)

func main() {
	upgrader := &websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
		OnOpen: func(c *websocket.Conn) {
			log.Printf("open %s", c.RemoteAddr())
		},
		OnMessage: func(c *websocket.Conn, op websocket.OpCode, msg []byte) {
			_ = c.WriteMessage(op, msg)
		},
		OnClose: func(c *websocket.Conn, err error) {
			log.Printf("close: %v", err)
		},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		if _, err := upgrader.Upgrade(w, r); err != nil {
			log.Printf("upgrade: %v", err)
		}
	})

	log.Fatal(fnet.ListenAndServe(":8081", mux))
}
```

Without `OnMessage`, `Upgrade` returns a blocking `*websocket.Conn` for manual `ReadMessage` / `WriteMessage`. You can also call `UpgradeEvent` with an `EventHandler`.

## Examples

```bash
go run ./example                 # HTTP + HTTPS (self-signed cert)
go run ./example/websocket       # echo WebSocket + demo page at :8081
```

HTTP example listens on `127.0.0.1:8080` and `127.0.0.1:8443` (override with `FNET_HTTP_ADDR` / `FNET_HTTPS_ADDR`).

## License

MIT. See [LICENSE](LICENSE).
