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

## 🏗️ Architecture

fnet employs a hybrid high-performance architecture: **Multi-Reactor Event-Driven I/O + VirtualConn Bridge + Sharded Concurrent Worker Pool**. It combines the near-zero resource cost of reactor-driven millions of concurrent connections with full compatibility for standard Go blocking business logic.

```text
Client
  |
  v
Main Reactor (accept)
  |
  v
Sub-Reactor (epoll / kqueue) ---- idle connection stays here, 0 goroutines
  |
  +-- HTTP: header complete (\r\n\r\n) --> Worker Pool --> ServeHTTP
  |
  +-- WebSocket: full frame --> unmask --> buffer pool --> Worker Pool --> OnMessage
  |                 |
  |                 +-- queued bytes too high --> pause read
  |
  v
Write: writev to the socket
  |
  +-- kernel buffer full --> queue, reactor flushes on writable
```

---

## Core Design Principles

### 1. 1M Conns "Zero-Goroutine While Idle"
* **Idle Connections**: Live sockets wait directly on the reactor's epoll/kqueue set, taking only a single pointer slot in the global chunked connection table. Idle connections hold **0 goroutines, 0 workers, and 0 read/write buffers**.
* **Lock-Free Chunked Table**: 2048-entry atomic pointer chunks allow $O(1)$ concurrent lookups for 1M+ active connections without global lock contention during scaling.

### 2. High-Performance HTTP & Streaming
* **Immediate Header Dispatch**: Sub-reactors continuously receive TCP streams and immediately dispatch the connection to the worker pool upon detecting a complete header (`\r\n\r\n`), without waiting for the body to finish downloading.
* **Large File & `io.Copy` Streaming**: `VirtualConn` provides true `net.Conn` semantics. In handlers, `io.Copy(dst, req.Body)` streams data directly to disk as chunks arrive, maintaining a constant memory footprint of ~32KB regardless of whether the file is 100MB or 1GB.

### 3. WebSocket Throughput & Large Frame Optimization
* **Zero-Allocation In-Place Framing**: Headers are decoded directly on the reactor's 64KB shared read buffer. Client payloads are unmasked in place using **SIMD (AVX2 / NEON)** vector instructions.
* **Tiered Slab Buffer Pool**: Comprehensive buffer pools cover sizes from 128B up to 16MB. Frames within 16MB require **0 heap allocations**.
* **Streaming Frame Assembler**: Large frames (>64KB) stream directly into dedicated slab buffers, completely avoiding repeated `append` reallocations. Buffer ownership is handed off to tasks and automatically returned upon callback exit with zero user burden.
* **Silent Backpressure**: If queued, unprocessed payload bytes exceed the threshold (default 4MB), `PauseRead` automatically suspends socket reads, using TCP sliding windows to throttle sender throughput and eliminate OOM risks.

### 4. Direct Output & Vector I/O (`writev`)
* **Fast-Path Direct Writes**: Senders prioritize direct non-blocking writes to the socket. As long as the kernel send buffer is not saturated, output bypasses the reactor entirely with 0 dispatch overhead.
* **Scatter-Gather Framing**: Stack-formatted 2~10 byte headers and payload slices are merged into single `writev` syscalls, eliminating user-space frame assembly buffer copies.

---

## Features

- **Standard `net/http` API**: Direct drop-in for `http.Handler` / `http.ServeMux` via `fnet.ListenAndServe` and `fnet.ListenAndServeTLS`.
- **Multi-Reactor Architecture**: Main poller handles non-blocking accepts (`accept4` on Linux), distributing sockets across worker sub-reactors matching CPU cores.
- **Dual WebSocket Modes**:
  - **Event-Driven**: Zero goroutines while idle; frame parsing happens in the reactor, and business callbacks (`OnMessage`) are automatically offloaded to a high-performance sharded worker pool to keep the I/O event loop unblocked.
  - **Blocking/Goroutine**: Full `net.Conn` stream compatibility for traditional request-response and blocking loops.
- **Built-in High-Concurrency Worker Pool**: Zero external dependencies, multi-shard lock-free design, per-connection strict FIFO ordering, and automatic idle worker reclamation for 1M+ connections. Both HTTP business requests (`ServeHTTP`) and WebSocket messages (`OnMessage`) are processed by default on the worker pool, completely freeing the I/O Reactor threads.
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
