# fnet

English | [中文](README.zh-CN.md)

A high-performance, event-driven network library for Go, built for large numbers of concurrent connections with a small memory footprint: a TCP engine, an `net/http`-compatible HTTP/1.x server and a WebSocket server, all on the same set of event loops.

I/O is driven by a main/sub-reactor model on native pollers: **epoll on Linux, kqueue on macOS**. Other platforms (such as Windows) get the same API on top of the standard library, with one goroutine per connection.

---

## ⚡ Highlights

- 🚀 **Built for a million connections**: about 1KB of user-space memory per idle HTTP keep-alive connection in our benchmark.
- 🧵 **Zero-goroutine idle connections**: idle sockets stay on the poller and hold no goroutine and no buffer; the read buffer is borrowed only while a task is reading.
- ⚡ **Direct, concurrency-safe writes**: `Conn.Write` / `Writev` can be called from any goroutine, never block, and go straight to the socket (`writev` for header + payload).
- 🔌 **Custom TCP protocols**: implement `OnOpen` / `OnData` / `OnClose`, and tell the engine how many bytes you consumed.
- 🛡️ **Standards compatible**: use `http.Handler` / `http.ServeMux` as is; WebSocket follows RFC 6455 (frame parsing by gobwas/ws).
- 🔒 **TLS without a goroutine per connection**: HTTPS and WSS take one option (`fhttp.Options.TLSConfig`); the standard `crypto/tls` runs inside the connection's callbacks, and `ftls` adds TLS to any `fnet.Handler`.
- ⏱️ **Deadline-style timeouts**: slow clients cannot hold a connection by trickling bytes.

---

## 🏗️ Architecture

| Package | Responsibility |
| --- | --- |
| `fnet` | TCP engine: main/sub-reactors, one task per connection, concurrency-safe writes (`Handler`, `Conn`, `Options`) |
| `ftls` | TLS on `fnet`: wraps any `Handler`, running the standard `crypto/tls` inside the callbacks |
| `fhttp` | HTTP/1.x server on `fnet`: request dispatch, `http.Handler`, protocol upgrade, HTTPS (through `ftls`) |
| `websocket` | WebSocket server on `fhttp`: handshake, framing, message callbacks, write coalescing |
| `taskpool` | Per-CPU sharded lock-free goroutine pool, the default executor of connection tasks |
| `poll` | epoll / kqueue wrapper |
| `internal/bytepool` | Size-class byte buffer pool |

Dependencies are one-way: `websocket → fhttp → ftls → fnet → poll`, and `fnet → taskpool`.

```text
user Handler (the only place that sees application data)
   ↑ OnOpen / OnData / OnClose              ↓ Conn.Write (any goroutine, never blocks)
┌────────────────────────────────────────────────────────────────┐
│ connection task: one per connection, serial, no overlap        │
│ first: register → OnOpen; then flush → read → OnData → OnClose │
└────────────────────────────────────────────────────────────────┘
   ↑ ready: submit its task (at most one at a time, always the same shard)
┌────────────────────────────────────────────────────────────────┐
│ executor (taskpool): per-CPU sharded lock-free pool            │
└────────────────────────────────────────────────────────────────┘
   ↑ event ready, hand the task over
┌────────────────────────────────────────────────────────────────┐
│ sub-reactor × N: 1 epoll/kqueue + 1 worker goroutine           │  ← N = max(2, GOMAXPROCS/8)
│ worker: never runs user code; when idle it helps the other     │
│ sub-reactors, blocking only when all of them are idle          │
└────────────────────────────────────────────────────────────────┘
   ↑ accepted connection (round-robin by default; ReusePort: each accepts its own)
listener (one per address by default; one per sub-reactor with ReusePort)
   ↑ accept                                ↓ read / write socket
kernel: epoll / kqueue + socket

Conn.Write: writes straight to the socket when nothing is backed up; the rest goes
to the send buffer, which the connection task flushes when the socket is writable
```

- **Parallel accept**: with `Options.ReusePort`, on Linux every sub-reactor listens on the address with a socket of its own (`SO_REUSEPORT`) and keeps the connections it accepts, so accepts run in parallel and a connection stays with the worker that accepted it; otherwise, and always on macOS, one listener per address feeds the sub-reactors in round-robin order. On Linux keepalive and `TCP_NODELAY` (with `Options.NoDelay`) are set once on the listener, which the accepted sockets inherit.
- **One task per connection**: reading, callbacks and closing run serially in the connection's task, so the callbacks of one connection never overlap and stay in order. There is no goroutine per connection and no inbound queue.
- **Zero-copy reads, kernel backpressure**: data is handed to `OnData` straight from a borrowed buffer. A connection is not read again until its callback returns, so a slow handler is throttled by TCP flow control instead of piling up memory.
- **HTTP**: the callback only looks for the end of each request (the header terminator `\r\n\r\n`, then a `Content-Length` body of up to `MaxBufferedBodyBytes`, 1MB by default); parsing and the body are left to `http.ReadRequest`, and the Handler runs on a goroutine that exists only while the connection has pending requests. A larger or chunked body (say a big file upload) is streamed by `net/http` instead: the connection is detached from `fnet` (`Conn.Detach`) and handed to an internal `http.Server` running the same Handler, which closes it after that request.
- **WebSocket**: frames are split and unmasked inside the connection's task and passed to `OnMessage` without a message queue; replies written while handling a batch of frames are coalesced into one write.
- **TLS**: `ftls` implements no TLS of its own. The standard `crypto/tls` does the handshake and the encryption, over a `net.Conn` fed by the connection's callbacks. Its blocking handshake runs as a coroutine (`iter.Pull`) that `OnData` resumes with each piece of data, so no goroutine waits for the client; from then on `OnData` decrypts the records at hand and passes the plaintext on. An idle TLS connection holds no goroutine, and a streamed request body goes to `net/http` together with the TLS session.

---

## Install

```bash
go get github.com/linfeip/fnet
```

Requires Go 1.23+. For a large number of connections, raise the fd limits (`ulimit -n`, `fs.nr_open`, `fs.file-max`).

---

## Quickstart

### TCP

```go
type echo struct{}

func (echo) OnOpen(c fnet.Conn)                  {}
func (echo) OnData(c fnet.Conn, data []byte) int { c.Write(data); return len(data) } // returns the number of bytes consumed
func (echo) OnClose(c fnet.Conn, err error)      {}

srv, err := fnet.NewServer(":9000", echo{}, fnet.Options{})
if err != nil {
	log.Fatal(err)
}
log.Fatal(srv.Serve())
```

### HTTP

Handlers are plain `net/http` ones. A shorter version of [examples/http](examples/http/main.go), which also covers PUT/PATCH/DELETE/OPTIONS, multipart uploads and file downloads:

```go
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/linfeip/fnet/fhttp"
)

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /hello", func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "hello world")
	})
	mux.HandleFunc("GET /query", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "hello %s\n", r.URL.Query().Get("name"))
	})
	mux.HandleFunc("POST /form", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "name=%s lang=%s\n", r.FormValue("name"), r.FormValue("lang"))
	})
	mux.HandleFunc("POST /json", func(w http.ResponseWriter, r *http.Request) {
		var v map[string]any
		if err := json.NewDecoder(r.Body).Decode(&v); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"received": v})
	})
	mux.HandleFunc("/echo", func(w http.ResponseWriter, r *http.Request) {
		io.Copy(w, r.Body) // a chunked or large body is streamed by net/http
	})

	srv, err := fhttp.NewServer(":8080", mux, fhttp.Options{})
	if err != nil {
		log.Fatal(err)
	}
	log.Fatal(srv.Serve())
}
```

### HTTPS

The same server with a certificate; WSS takes the same option:

```go
cert, err := tls.LoadX509KeyPair("cert.pem", "key.pem")
if err != nil {
	log.Fatal(err)
}
srv, err := fhttp.NewServer(":8443", mux, fhttp.Options{
	TLSConfig: &tls.Config{Certificates: []tls.Certificate{cert}},
})
```

Any `tls.Config` works (`GetCertificate` for SNI or autocert, client certificates, ...). Only HTTP/1.1 is offered through ALPN, the handshake has to complete within `ReadHeaderTimeout`, and `r.TLS` is set as with `net/http`.

For a custom TCP protocol, wrap its handler: `fnet.NewServer(":9443", ftls.NewHandler(echo{}, config, 0), fnet.Options{})`, where the last argument is the handshake timeout (0 means 10s).

### WebSocket

An echo server, the same as [examples/websocket](examples/websocket/main.go):

```go
package main

import (
	"log"
	"net/http"

	"github.com/linfeip/fnet/fhttp"
	"github.com/linfeip/fnet/websocket"

	"github.com/gobwas/ws"
)

type echo struct{}

func (echo) OnOpen(c *websocket.Conn)                               {}
func (echo) OnMessage(c *websocket.Conn, op ws.OpCode, data []byte) { c.WriteMessage(op, data) } // a complete message
func (echo) OnClose(c *websocket.Conn, err error)                   {}

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		websocket.Upgrade(w, r, echo{}, websocket.Options{})
	})

	srv, err := fhttp.NewServer(":8080", mux, fhttp.Options{})
	if err != nil {
		log.Fatal(err)
	}
	log.Fatal(srv.Serve())
}
```

On Linux, `fhttp.Options{Engine: fnet.Options{ReusePort: true}}` gives every sub-reactor a listener of its own.

Callbacks run on the executor (`taskpool.DefaultTaskPool` by default, replaceable through `fnet.Options.Executor`), so they must return quickly. Timeouts and limits are set in `fnet.Options`, `fhttp.Options` and `websocket.Options`; see their doc comments.

---

## Examples

The examples are a separate module (`examples/go.mod`, built against the fnet in this repository), so their dependencies, such as fasthttp for the HTTP benchmark, stay out of fnet's own `go.mod`.

```bash
cd examples
go run ./echo        # TCP echo
go run ./http        # HTTP: GET, forms, JSON, PUT/PATCH/DELETE/OPTIONS, chunked, multipart uploads
go run ./http -addr :8443 -tls   # HTTPS, with a self-signed certificate generated at startup (curl -k)
go run ./websocket   # WebSocket echo
go test ./http -run XXX -bench GET   # GET benchmark: fhttp vs net/http vs fasthttp
```

---

## Limitations

- The send buffer has no upper bound and there is no write timeout: a client that never reads makes it grow.
- No HTTP/2: HTTPS is HTTP/1.1 only. Responses are fully buffered (no `http.Flusher` / `http.Hijacker`), except on connections handed to `net/http` for a streamed request body.
- TLS is server side only. `ftls` relies on how `crypto/tls` treats a temporary read error (the connection stays usable, and so does a record cut short), which `go test ./ftls` checks: run it after upgrading Go. A custom `fnet.Options.Executor` must not lock its goroutines to OS threads.
- WebSocket: no permessage-deflate; messages are always sent as a single frame.
- Half-close is not supported: after EOF the connection is closed once the buffered data has been sent.
- The kqueue path is fully tested on macOS; the epoll path has run HttpArena's WebSocket benchmarks on Linux and passes their validation. Run `go test -race ./...` on your Linux target before going live.

---

## License

[MIT](LICENSE)
