# fnet

English | [中文](README.zh-CN.md)

A production-grade, ultra-high-performance HTTP/HTTPS and WebSocket engine for Go, engineered for **million-level concurrent connections (1M Conns)** with minimal memory footprint and extreme CPU efficiency.

I/O is driven by native multi-reactor pollers (**epoll** on Linux, **kqueue** on macOS, **WSAPoll** on Windows). It adheres strictly to RFC 6455 and Go's standard `net/http` contracts without compromising on real-world business requirements.

---

## ⚡ Highlights

- 🚀 **1 Million Concurrent Connections**: Built to hold a million live WebSocket connections in one process with a compact per-connection memory footprint.
- 🔥 **High Throughput & Low CPU**: Multi-reactor pollers keep echo and fan-out workloads fast while using few cores.
- ⚡ **Rapid Handshake**: Non-blocking `accept4` and a shared connection table keep mass connect storms cheap.
- 🧵 **Zero-Goroutine Idle Connections**: Idle sockets stay on the poller and do not occupy goroutines or stacks.
- 🛡️ **Full Standards & Business Compliance**: Complete RFC 6455 support (payload unmasking, Ping/Pong/Close control frames, Origin check, Subprotocols), full TLS/HTTPS support, and standard `http.Handler` compatibility.

---

## Features

- **Standard `net/http` API**: Direct drop-in for `http.Handler` / `http.ServeMux` via `fnet.ListenAndServe` and `fnet.ListenAndServeTLS`.
- **Multi-Reactor Architecture**: Main poller handles non-blocking accepts (`accept4` on Linux), distributing sockets across worker sub-reactors matching CPU cores.
- **Dual WebSocket Modes**:
  - **Event-Driven**: Zero goroutines while idle, parsing frames and dispatching `OnOpen`/`OnMessage`/`OnClose` directly on reactor events.
  - **Blocking/Goroutine**: Full `net.Conn` stream compatibility for traditional request-response and blocking loops.
- **Vector I/O (`writev`)**: Stack-allocated header framing merged with payload into single syscall writes to eliminate intermediate buffer copies.
- **Aggressive Memory Optimization**: Chunked lock-free connection tables, pooled response writers, lazy address resolution, and auto-compacting buffers.
- **Cross-Platform**: Linux (`epoll`), macOS/Darwin (`kqueue`), Windows (`WSAPoll`).

---

## Install

```bash
go get github.com/linfeip/fnet
```

Requires Go 1.21+.

---

## Quickstart

### HTTP & HTTPS

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
		io.WriteString(w, "hello from fnet\n")
	})

	// Plain HTTP
	log.Fatal(fnet.ListenAndServe(":8080", mux))
}
```

Configuring HTTPS with timeouts:

```go
srv := &fnet.Server{
	Addr:         ":8443",
	Handler:      mux,
	ReadTimeout:  5 * time.Second,
	WriteTimeout: 10 * time.Second,
	IdleTimeout:  60 * time.Second,
}
log.Fatal(srv.ListenAndServeTLS("cert.pem", "key.pem"))
```

---

### WebSocket (Event-Driven, Zero Goroutine)

Provide `OnMessage` callback in `websocket.Upgrader`. Idle connections do not hold any goroutines.

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
			log.Printf("connected: %s", c.RemoteAddr())
		},
		OnMessage: func(c *websocket.Conn, op websocket.OpCode, msg []byte) {
			// Echo frame back to peer
			_ = c.WriteMessage(op, msg)
		},
		OnClose: func(c *websocket.Conn, err error) {
			log.Printf("disconnected: %v", err)
		},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		if _, err := upgrader.Upgrade(w, r); err != nil {
			log.Printf("upgrade error: %v", err)
		}
		// Returning here hands the socket to the event loop.
	})

	log.Fatal(fnet.ListenAndServe(":8081", mux))
}
```

> **Traditional Goroutine Mode**: Omit `OnMessage` from `Upgrader`. `Upgrade(w, r)` then returns a standard `*websocket.Conn` supporting blocking `ReadMessage()` / `WriteMessage()` loops.

---

## Examples

Run the bundled examples:

```bash
go run ./example                 # HTTP on :8080 & HTTPS on :8443
go run ./example/websocket       # High-concurrency echo WebSocket on :8081
```

---

## License

[MIT](LICENSE)
