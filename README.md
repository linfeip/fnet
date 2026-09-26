# fnet

English | [中文](README.zh-CN.md)

A production-grade, ultra-high-performance networking framework for Go — custom TCP protocols, HTTP/HTTPS and WebSocket — engineered for **million-level concurrent connections (1M Conns)** with minimal memory footprint and extreme CPU efficiency.

I/O is driven by native multi-reactor pollers. **Linux (epoll) is the production target**; macOS (kqueue) is supported for development, and Windows runs an emulation for development only. It adheres strictly to RFC 6455 and Go's standard `net/http` contracts without compromising on real-world business requirements, and serves your own TCP protocols (game servers, gateways, IM, RPC) with the same event loops.

---

## ⚡ Highlights

- 🚀 **1 Million Concurrent Connections**: Built to hold a million live WebSocket connections in one process with a compact per-connection memory footprint.
- 🔥 **High Throughput & Low CPU**: Multi-reactor pollers keep echo and fan-out workloads fast while using few cores.
- ⚡ **Rapid Handshake**: Non-blocking `accept4` and a shared connection table keep mass connect storms cheap.
- 🧵 **Zero-Goroutine Idle Connections**: Idle sockets stay on the poller and do not occupy goroutines or stacks.
- 🎮 **Custom TCP Protocols**: `fnet.Server` frames your protocol on the event loop (`Split`, e.g. a Netty-style `LengthField`) and runs `OnMessage` on workers, one message at a time per connection, so handlers may block on databases.
- 🛡️ **Full Standards & Business Compliance**: Complete RFC 6455 support (payload unmasking, Ping/Pong/Close control frames, Origin check, Subprotocols), full TLS/HTTPS support, and standard `http.Handler` compatibility.

---

## 🏗️ Architecture

Layers that each do one job, plus the worker pool that runs business code:

| Layer | Package | Responsibility |
|---|---|---|
| Platform | `internal/netpoll` | epoll / kqueue / Windows emulation, raw non-blocking socket calls. The only platform-specific code. |
| Reactor | `internal/reactor` | Acceptor, event loops, connection table, per-connection input/output buffering, backpressure, close. Moves bytes only. |
| TCP protocols | `fnet` | `Server` for message protocols: `Split` frames on the loop, `OnOpen` / `OnMessage` / `OnClose` on workers; backpressure, timeouts, half-close, graceful `Shutdown`. |
| HTTP | `fhttp` | `Server` lifecycle and listeners; HTTP/1.x: decide when a request is ready, run `ServeHTTP` on a worker, keep-alive, TLS, hijack. |
| WebSocket | `websocket` | Handshake, RFC 6455 framing and control frames, permessage-deflate, event-driven message queue. |
| Worker pool | `pool` | Sharded goroutine pool that runs `ServeHTTP` and `OnMessage`; shared by `fnet`, `fhttp` and `websocket`, no networking code. |

Protocols plug into the reactor through one interface, `reactor.Handler` (`OnData` / `OnClose`, run on the event loop). A connection's input is either delivered to its handler on the loop (idle, zero goroutines) or *detached* to a blocking reader on a worker (`net.Conn` semantics for `net/http` and TLS), and handed back with `Attach`.

```text
Client
  |
  v
Acceptor (accept4) --> Event loop (epoll / kqueue) ---- idle connection waits here, 0 goroutines
                          |
                          +-- HTTP: header complete --> Detach --> worker: ServeHTTP --> Attach (back to idle)
                          |
                          +-- TCP: Split cuts messages on the loop --> pooled message --> worker: OnMessage
                          |
                          +-- WebSocket: frames parsed on the loop --> pooled message --> worker: OnMessage
                          |                  |
                          |                  +-- too many queued bytes --> pause reading
                          v
                    Output: direct (writev) write; kernel buffer full --> queued, flushed by the loop
                            replies to a burst of messages --> held (<= 1 ms) --> one write
```

---

## Core Design Principles

### 1. 1M Conns "Zero-Goroutine While Idle"
* **Idle Connections**: Live sockets wait on the event loop's epoll/kqueue set and take one pointer slot in the chunked connection table plus one `reactor.Conn` (240 B). Idle connections hold **no goroutine, no worker, and no read/write buffer**; the 64 KiB read buffer belongs to the event loop.
* **Lock-Free Chunked Table**: 2048-entry atomic pointer chunks give $O(1)$ lookups without global lock contention while growing.

### 2. HTTP
* **Dispatch On A Complete Header**: The event loop dispatches a connection to the worker pool as soon as a complete header (`\r\n\r\n`) is buffered, without waiting for the body. A header over `MaxHeaderBytes` (64 KiB) gets `431`, in plaintext and TLS alike; a malformed request gets `400`, like `net/http`.
* **Blocking Semantics On The Worker**: While a worker owns the connection it reads and writes with ordinary blocking `net.Conn` semantics, and TCP flow control holds both directions: an upload the handler is not reading yet stops at 256 KiB buffered, and a download to a slow client makes the handler's `Write` wait instead of queueing without bound. `io.Copy` streams with constant memory either way.
* **`net/http` Behavior**: `Flush` and `http.ResponseController` (Server-Sent Events, streaming), `r.Context()` canceled when the client goes away, `Content-Length` enforced (`http.ErrContentLength`; a short body closes the connection), `1xx` interim responses such as `103 Early Hints`, handler panics logged to `ErrorLog`, and `Shutdown(ctx)` that lets the requests in progress finish.
* **Back To Idle**: After a response the connection returns to the event loop and the worker goroutine is released; a pipelined request that is already buffered keeps the worker. Over TLS the session idles on the poller too, keeping its TLS state (keep-alive is opt-in there: set `IdleTimeout`). A client half-close (request, then FIN) still gets its whole response.
* **Timeouts Without Goroutines**: While the event loop holds a connection, a per-loop timing wheel enforces the header deadline (`ReadHeaderTimeout`, default 30s, counted from accept or from a request's first byte, so a slow-loris drip cannot extend it) and the keep-alive idle deadline (`IdleTimeout`, default 2 min). A closing connection whose peer stops taking output is dropped after 30s without progress.
* **`Expect: 100-continue`**: answered with `100 Continue` when the handler first reads the body; a handler that replies without reading it gets the connection closed afterwards.

### 3. WebSocket
* **Parsing On The Loop, Business On Workers**: Frames are parsed and unmasked on the event loop, Ping/Close are answered there, and each complete message is copied into a pooled buffer and queued; the messages that arrive whole in one read share a buffer (up to 16 KiB), so a burst costs one pool round trip, not one per message. `OnMessage` runs on the worker pool, one call at a time per connection, in order; `OnClose` follows the messages that arrived before the close.
* **Tiered Buffer Pool**: Pooled buffers from 128 B to 16 MiB; a large frame streams into its message buffer as bytes arrive, so memory follows what was received, not what a header claims.
* **Backpressure**: When a connection's queued message bytes exceed the threshold (default 64 KiB), reading pauses and TCP flow control slows the sender.
* **Liveness**: On an event-driven connection `SetReadDeadline` closes the connection at the deadline unless it is moved, and `OnPong` sees the peer's Pongs: ping from the server, move the deadline on each Pong, and a vanished peer is dropped.

### 4. TCP Message Protocols
* **Framing On The Loop, Business On Workers**: `Split` (with `bufio.SplitFunc` semantics) runs on the event loop and only finds message boundaries; each complete message is copied into a pooled buffer, shared (up to 16 KiB) by the messages cut from one read. `OnOpen`, `OnMessage` and `OnClose` run on the worker pool, one call at a time per connection, in order. A partial message stays with the reactor (bounded by `MaxMessageSize`, default 1 MiB) and never holds a worker; a panicking or looping `Split` closes only its own connection.
* **Backpressure Both Ways**: Received messages waiting for `OnMessage` above `MaxPendingMessageBytes` (64 KiB) pause reading. Output queued for a peer that does not read is capped by `MaxOutboundBytes` (16 MiB by default; broadcast-heavy servers set it near 256 KiB to drop stalled players in seconds); `Write` fails with `ErrWriteBufferFull` instead of growing.
* **Timeouts Count Only The Peer**: `ReadTimeout` (30s) runs from a message's first byte, so trickling cannot extend it; `IdleTimeout` is a heartbeat timeout (off by default). Time spent in handlers or paused by backpressure never counts. TCP keep-alive (60s idle, then 4 probes 15s apart) drops peers that vanished without closing.
* **Closing**: A peer that closes or half-closes still has the messages it sent handled and the replies flushed; `OnClose` gets `io.EOF`. A close from this side drops messages not handled yet. `Shutdown(ctx)` stops accepting, lets every connection finish what it received, and returns once every `OnClose` has run.

### 5. Output
* **Direct Writes**: Writers go straight to the socket; only what the kernel does not accept is queued and flushed by the event loop. On an event-driven connection (TCP messages, event-driven WebSocket) a write never blocks: the queue is capped (16 MiB by default) and a full one fails with `ErrWriteBufferFull`, the sign of a peer too slow to keep up; a write into an empty queue is always taken whole, so a message is never cut short on the wire. While a worker owns the connection (HTTP, a hijacked or blocking WebSocket connection) writes wait as on a blocking socket.
* **Closing Without Resets**: A close flushes the queued output, then half-closes and gives the peer 500 ms to finish, reading and dropping what it still sends, so the close never turns into a reset that destroys a response the peer has not read yet.
* **Replies To A Burst Share A Write**: When several messages arrive together (one read yields a batch), the worker holds the connection's output while it handles them, so their replies, and anything another goroutine writes meanwhile, leave in one system call. Nothing is held longer than 1 ms or beyond 64 KiB: a slow handler in the batch holds back neither the replies before it nor a broadcast. A single message is answered with a direct write, as before.
* **`writev`**: Frame or response headers and payloads leave in a single `writev` call without an intermediate copy.

---

## Features

- **Standard `net/http` API**: Direct drop-in for `http.Handler` / `http.ServeMux` via `fhttp.ListenAndServe` and `fhttp.ListenAndServeTLS`.
- **Multi-Reactor Architecture**: One acceptor (`accept4` on Linux) distributes sockets round-robin across event loops matching CPU cores.
- **Dual WebSocket Modes**:
  - **Event-Driven**: Zero goroutines while idle; frame parsing happens in the reactor, and business callbacks (`OnMessage`) are automatically offloaded to a high-performance sharded worker pool to keep the I/O event loop unblocked.
  - **Blocking/Goroutine**: Full `net.Conn` stream compatibility for traditional request-response and blocking loops.
- **Built-in Worker Pool**: A bounded, elastic goroutine pool, split into per-core shards with a lock each so event loops and workers rarely contend. A worker that finishes a task takes the next one without parking, a parked worker is woken only for a task no other worker is coming for, a new one starts only while every worker is busy, and a worker about to park first takes a task another shard has waiting: a few hundred workers carry 100k busy WebSocket connections. Workers exit when idle, and tasks beyond `MaxWorkers` (shared out between the shards) wait in order instead of spawning goroutines. HTTP requests (`ServeHTTP`), WebSocket messages and TCP messages (`OnMessage`) run on it by default, keeping the event loops free; each connection runs its callbacks one at a time, in arrival order. Any pool plugs in as `WorkerPool`, e.g. `pool.Adapt(ants.Submit)`; an error refuses the task and closes that connection.
- **Vector I/O (`writev`)**: Stack-allocated header framing merged with payload into single syscall writes to eliminate intermediate buffer copies.
- **Small Idle Connections**: Chunked lock-free connection tables, state that exists only while a worker serves a connection, and auto-compacting buffers.
- **Platforms**: Linux (`epoll`) is what production runs on and what capacity and defaults are tuned for. macOS/Darwin (`kqueue`) behaves the same and is meant for development. Windows runs a development emulation on top of the `net` package (one pump goroutine per socket); it is not for production.

---

## Install

```bash
go get github.com/linfeip/fnet
```

Requires Go 1.26+ (the `go` directive in `go.mod`). Deploy on Linux; a million connections also need raised fd limits (`ulimit -n`, `fs.nr_open`, `fs.file-max`).

---

## Quickstart

### Custom TCP Protocol

A game server speaking `| len uint32 | payload |`:

```go
package main

import (
	"encoding/binary"
	"log"
	"time"

	"github.com/linfeip/fnet"
)

func main() {
	srv := &fnet.Server{
		Addr:        ":7001",
		Split:       fnet.LengthField{Size: 4, Strip: 4}.Split, // OnMessage gets the payload
		IdleTimeout: 30 * time.Second,                          // heartbeat timeout
		OnOpen:      func(c *fnet.Conn) { c.SetContext(newSession(c)) },
		OnMessage: func(c *fnet.Conn, msg []byte) {
			// A worker, one message at a time per connection: it may query a database.
			reply := c.Context().(*session).handle(msg)
			var hdr [4]byte
			binary.BigEndian.PutUint32(hdr[:], uint32(len(reply)))
			if _, err := c.Writev([][]byte{hdr[:], reply}); err != nil {
				c.Close()
			}
		},
		OnClose: func(c *fnet.Conn, err error) { c.Context().(*session).save() },
	}
	log.Fatal(srv.ListenAndServe())
}
```

`Split` answers one question on the event loop: is there a whole message at the start of the bytes received so far, and how long is it? `LengthField` covers length-prefixed headers the way Netty's `LengthFieldBasedFrameDecoder` does (`Offset`, `Size` 1/2/3/4/8, `Order`, `Adjust`, `Strip`), `bufio.ScanLines` covers line protocols, and any `bufio.SplitFunc` works. `msg` is only valid during `OnMessage`; decoding it (e.g. `proto.Unmarshal`) copies it. `Write` never blocks and may be called from any goroutine, e.g. a room broadcasting the same buffer to every player.

### HTTP & HTTPS

```go
package main

import (
	"io"
	"log"
	"net/http"

	"github.com/linfeip/fnet/fhttp"
)

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "hello from fnet\n")
	})

	// Plain HTTP
	log.Fatal(fhttp.ListenAndServe(":8080", mux))
}
```

Configuring HTTPS with timeouts:

```go
srv := &fhttp.Server{
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

	"github.com/linfeip/fnet/fhttp"
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

	log.Fatal(fhttp.ListenAndServe(":8081", mux))
}
```

> **Traditional Goroutine Mode**: Omit `OnMessage` from `Upgrader`. `Upgrade(w, r)` then returns a standard `*websocket.Conn` supporting blocking `ReadMessage()` / `WriteMessage()` loops.

---

## Examples

Run the bundled examples:

```bash
go run ./example                 # HTTP on :8080 & HTTPS on :8443
go run ./example/websocket       # High-concurrency echo WebSocket on :8081
go run ./example/tcp -bots 3     # Chat-room game server on :7001 with three bots
```

---

## License

[MIT](LICENSE)
