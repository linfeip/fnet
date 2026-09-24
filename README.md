# fnet

English | [中文](README.zh-CN.md)

A production-grade, ultra-high-performance HTTP/HTTPS and WebSocket engine for Go, engineered for **million-level concurrent connections (1M Conns)** with minimal memory footprint and extreme CPU efficiency.

I/O is driven by native multi-reactor pollers (**epoll** on Linux, **kqueue** on macOS; Windows runs an emulation for development). It adheres strictly to RFC 6455 and Go's standard `net/http` contracts without compromising on real-world business requirements.

---

## ⚡ Highlights

- 🚀 **1 Million Concurrent Connections**: Built to hold a million live WebSocket connections in one process with a compact per-connection memory footprint.
- 🔥 **High Throughput & Low CPU**: Multi-reactor pollers keep echo and fan-out workloads fast while using few cores.
- ⚡ **Rapid Handshake**: Non-blocking `accept4` and a shared connection table keep mass connect storms cheap.
- 🧵 **Zero-Goroutine Idle Connections**: Idle sockets stay on the poller and do not occupy goroutines or stacks.
- 🛡️ **Full Standards & Business Compliance**: Complete RFC 6455 support (payload unmasking, Ping/Pong/Close control frames, Origin check, Subprotocols), full TLS/HTTPS support, and standard `http.Handler` compatibility.

---

## 🏗️ Architecture

Four layers, each doing one job:

| Layer | Package | Responsibility |
|---|---|---|
| Platform | `internal/netpoll` | epoll / kqueue / Windows emulation, raw non-blocking socket calls. The only platform-specific code. |
| Reactor | `internal/reactor` | Acceptor, event loops, connection table, per-connection input/output buffering, backpressure, close. Moves bytes only. |
| Server + HTTP | `fnet` | `Server` lifecycle and listeners; HTTP/1.x: decide when a request is ready, run `ServeHTTP` on a worker, keep-alive, TLS, hijack. |
| WebSocket | `websocket` | Handshake, RFC 6455 framing and control frames, permessage-deflate, event-driven message queue. |

Protocols plug into the reactor through one interface, `reactor.Handler` (`OnData` / `OnClose`, run on the event loop). A connection's input is either delivered to its handler on the loop (idle, zero goroutines) or *detached* to a blocking reader on a worker (`net.Conn` semantics for `net/http` and TLS), and handed back with `Attach`.

```text
Client
  |
  v
Acceptor (accept4) --> Event loop (epoll / kqueue) ---- idle connection waits here, 0 goroutines
                          |
                          +-- HTTP: header complete --> Detach --> worker: ServeHTTP --> Attach (back to idle)
                          |
                          +-- WebSocket: frames parsed on the loop --> pooled message --> worker: OnMessage
                          |                  |
                          |                  +-- too many queued bytes --> pause reading
                          v
                    Output: direct (writev) write; kernel buffer full --> queued, flushed by the loop
```

---

## Core Design Principles

### 1. 1M Conns "Zero-Goroutine While Idle"
* **Idle Connections**: Live sockets wait on the event loop's epoll/kqueue set and take one pointer slot in the chunked connection table plus one `reactor.Conn` (256 B). Idle connections hold **no goroutine, no worker, and no read/write buffer**; the 64 KiB read buffer belongs to the event loop.
* **Lock-Free Chunked Table**: 2048-entry atomic pointer chunks give $O(1)$ lookups without global lock contention while growing.

### 2. HTTP
* **Dispatch On A Complete Header**: The event loop dispatches a connection to the worker pool as soon as a complete header (`\r\n\r\n`) is buffered, without waiting for the body. Headers that never finish are capped at 64 KiB.
* **Streaming Bodies**: While a worker owns the connection it reads with ordinary blocking `net.Conn` semantics, so `io.Copy(dst, req.Body)` streams with constant memory.
* **Back To Idle**: After a plaintext response the connection returns to the event loop and the worker goroutine is released; a pipelined request that is already buffered keeps the worker. A client half-close (request, then FIN) still gets its response.
* **Timeouts Without Goroutines**: While the event loop holds a connection, a per-loop timing wheel enforces the header deadline (`ReadHeaderTimeout`, default 30s, counted from accept or from a request's first byte, so a slow-loris drip cannot extend it) and the keep-alive idle deadline (`IdleTimeout`, default 2 min). A closing connection whose peer stops taking output is dropped after 30s without progress, or at the write deadline.
* **`Expect: 100-continue`**: answered with `100 Continue` when the handler first reads the body; a handler that replies without reading it gets the connection closed afterwards.

### 3. WebSocket
* **Parsing On The Loop, Business On Workers**: Frames are parsed and unmasked on the event loop, Ping/Close are answered there, and each complete message is copied into a pooled buffer and queued. `OnMessage` runs on the worker pool, one call at a time per connection, in order; `OnClose` follows the messages that arrived before the close.
* **Tiered Buffer Pool**: Pooled buffers from 128 B to 16 MiB; a large frame streams into its message buffer as bytes arrive, so memory follows what was received, not what a header claims.
* **Backpressure**: When a connection's queued message bytes exceed the threshold (default 64 KiB), reading pauses and TCP flow control slows the sender.

### 4. Output
* **Direct Writes**: Writers go straight to the socket; only what the kernel does not accept is queued (up to 16 MiB per connection) and flushed by the event loop. Queued output above 64 KiB pauses reading until it drains.
* **`writev`**: Frame or response headers and payloads leave in a single `writev` call without an intermediate copy.

---

## Features

- **Standard `net/http` API**: Direct drop-in for `http.Handler` / `http.ServeMux` via `fnet.ListenAndServe` and `fnet.ListenAndServeTLS`.
- **Multi-Reactor Architecture**: One acceptor (`accept4` on Linux) distributes sockets round-robin across event loops matching CPU cores.
- **Dual WebSocket Modes**:
  - **Event-Driven**: Zero goroutines while idle; frame parsing happens in the reactor, and business callbacks (`OnMessage`) are automatically offloaded to a high-performance sharded worker pool to keep the I/O event loop unblocked.
  - **Blocking/Goroutine**: Full `net.Conn` stream compatibility for traditional request-response and blocking loops.
- **Built-in High-Concurrency Worker Pool**: Zero external dependencies, multi-shard lock-free design, per-connection strict FIFO ordering, and automatic idle worker reclamation for 1M+ connections. Both HTTP business requests (`ServeHTTP`) and WebSocket messages (`OnMessage`) are processed by default on the worker pool, completely freeing the I/O Reactor threads.
- **Vector I/O (`writev`)**: Stack-allocated header framing merged with payload into single syscall writes to eliminate intermediate buffer copies.
- **Aggressive Memory Optimization**: Chunked lock-free connection tables, pooled response writers, lazy address resolution, and auto-compacting buffers.
- **Cross-Platform**: Linux (`epoll`), macOS/Darwin (`kqueue`). Windows runs a development emulation on top of the `net` package (one pump goroutine per socket).

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
