# fnet

English | [中文](README.zh-CN.md)

A high-performance, event-driven network library for Go, built for large numbers of concurrent connections with a small memory footprint: a TCP and UDP engine, `net/http`-compatible HTTP/1.x and HTTP/2 servers, and a WebSocket server, all on the same set of event loops.

I/O is driven by a main/sub-reactor model on native pollers: **epoll on Linux, kqueue on macOS**. Other platforms (such as Windows) get the same API on top of the standard library.

---

## ⚡ Highlights

- 🚀 **Built for a million connections**: about 1KB of user-space memory per idle HTTP keep-alive connection in our benchmark.
- 🧵 **Zero-goroutine idle connections**: idle sockets stay on the poller and hold no goroutine and no buffer; the read buffer is borrowed only while a task is reading.
- ⚡ **Direct, concurrency-safe writes**: `Conn.Write` / `Writev` can be called from any goroutine, never block, and go straight to the socket (`writev` for header + payload).
- 🔌 **Custom TCP protocols**: implement `OnOpen` / `OnData` / `OnClose`, and tell the engine how many bytes you consumed.
- 🛡️ **Standards compatible**: use `http.Handler` / `http.ServeMux` as is across HTTP/1.x and HTTP/2; HTTP/2 follows RFC 9113 and WebSocket follows RFC 6455.
- 🔒 **TLS without a goroutine per connection**: HTTPS and WSS take one option (`fhttp.Options.TLSConfig`); the standard `crypto/tls` runs inside the connection's callbacks, and `ftls` adds TLS to any `fnet.Handler`.
- ⏱️ **Deadline-style timeouts**: slow clients cannot hold a connection by trickling bytes.

---

## 🏗️ Architecture

| Package | Responsibility |
| --- | --- |
| `fnet` | TCP and UDP engine: shared reactor, `Server` (`Handler`, `Conn`, `Options`) and `UDPServer` (`UDPHandler`, `UDPConn`, `UDPOptions`) |
| `kcp` | Wire-compatible KCP reliable-UDP server on `fnet.UDPServer`: sharded zero-alloc session table, pooled ARQ buffers, shared ticker (`Server`, `Conn`, `Handler`, `Options`) |
| `ftls` | TLS on `fnet`: wraps any `Handler`, running the standard `crypto/tls` inside the callbacks |
| `fhttp` | HTTP/1.x server on `fnet`: request dispatch, `http.Handler`, protocol upgrade, HTTPS (through `ftls`) |
| `http2` | HTTP/2 server on `fnet`: ALPN `h2` or h2c prior knowledge, multiplexed streams, request dispatch through `http.Handler` |
| `websocket` | WebSocket server on `fhttp`: handshake, framing, message callbacks, write coalescing |
| `taskpool` | Per-CPU sharded lock-free goroutine pool, the default executor of connection tasks |
| `poll` | epoll / kqueue wrapper |
| `internal/bytepool` | Size-class byte buffer pool |

Dependencies are one-way: `websocket → fhttp → ftls → fnet → poll`, `http2 → ftls → fnet`, `kcp → fnet`, and `fnet`, `fhttp`, `http2` → `taskpool`.

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
- **Zero-copy reads, kernel backpressure**: data is handed to `OnData` straight from a borrowed buffer. A connection is not read again until its callback returns, nor while more than 64KB waits in its send buffer, so a slow handler, or a client that does not read its replies, is throttled by TCP flow control instead of piling up memory.
- **Graceful close**: a `ClosingHandler` gets to write before the engine closes a connection on its own (peer EOF, expired deadline, server shutdown); `ftls` sends TLS close_notify there.
- **HTTP**: the callback only looks for the end of each request (the header terminator `\r\n\r\n`, within `MaxHeaderBytes`, 16KB by default, and 500 field lines, then a body of up to `MaxBufferedBodyBytes`, 1MB by default, with `Content-Length` or plain chunks). Each request is parsed by the standard library's `http.ReadRequest`, and the Handler runs on a goroutine of the executor (`Options.Executor`, a private `taskpool.NewDefault()` by default), held only while the connection has pending requests. `Request.Context` is canceled when the client connection closes, or the client closes its write direction (it still gets the responses to the requests it sent, as with `net/http`). The responses to requests queued together (pipelining) go out in one write, and a client that does not take them (more than 64KB waiting, see `Conn.Writable`) is not answered further until it does. A response body over 64KB goes out while the Handler writes it, and its `Write` waits for the client the same way, so a large download to a slow client keeps no more than about 192KB of it in memory. A larger body (say a big file upload), or one framed in an unusual way, is streamed by `net/http` instead: the connection is detached from `fnet` (`Conn.Detach`) and handed to an internal `http.Server` running the same Handler, which closes it after that request. So are Server-Sent Events requests, which need `http.Flusher`.
- **HTTP/2**: framing, HPACK and connection-level control run serially inside each connection's `fnet` callback; complete requests are dispatched to `Options.Executor`, so concurrent streams share one connection without a read goroutine per stream. TLS with ALPN `h2` and cleartext h2c with prior knowledge are supported.
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

A handler can also implement optional interfaces: `ClosingHandler` (`OnClosing`, write something before the engine closes the connection on its own), `WritableHandler` (`OnWritable`, resume writing once `Conn.Writable` reports true again, Linux/macOS only), `HalfCloseHandler` (`OnHalfClose`, keep the connection open to answer a peer that closed its write direction, Linux/macOS only) and `ResumeHandler` (`OnResume`, pass on data it kept while reading was paused).

### UDP

```go
type udpEcho struct{}

func (udpEcho) OnPacket(c fnet.UDPConn, data []byte, from netip.AddrPort) {
	c.WriteToUDPAddrPort(data, from)
}

srv, err := fnet.NewUDPServer(":9000", udpEcho{}, fnet.UDPOptions{ReusePort: true})
if err != nil {
	log.Fatal(err)
}
log.Fatal(srv.Serve())
```

- **Socket parallelism (`UDPOptions.ReusePort`)**: `OnPacket` callbacks of a single UDP socket run serially. With `ReusePort: true` on Linux, `NewUDPServer` opens `GOMAXPROCS` sockets sharing the address via `SO_REUSEPORT` and spreads them across the sub-reactors; the kernel hashes the 4-tuple so one peer's datagrams stay in order on the same socket while different peers run in parallel. Without `ReusePort`, or on macOS, the server uses one socket.
- **Non-blocking sends (`EAGAIN` drop)**: on Linux/macOS `WriteToUDPAddrPort` may be called from any goroutine and never blocks. When the kernel send buffer is full it returns `EAGAIN` (or `ENOBUFS`) immediately and drops the datagram rather than queueing it, unlike `net.UDPConn` which blocks waiting for buffer space.
- **Dual-stack addresses**: an IPv4 peer sending to a dual-stack socket (such as `":9000"`) appears as an IPv4-mapped IPv6 address (`::ffff:a.b.c.d`, `from.Addr().Unmap()` yields the plain IPv4 address), and passing `from` straight back to `WriteToUDPAddrPort` replies to it.
- **Socket buffers and hole punching**: `UDPOptions.ReadBufferSize` / `WriteBufferSize` tune `SO_RCVBUF` / `SO_SNDBUF`, and `srv.Conn()` lets the application send outbound probes from the listening port before any packet has been received ([examples/udp-punch](examples/udp-punch/main.go), [examples/udp-echo](examples/udp-echo/main.go)).

### KCP

`kcp` layers wire-compatible KCP ARQ (interoperable with `kcp-go` and C `ikcp` peers, with zero third-party dependencies) on top of `fnet.UDPServer`, with 64-shard session management, pooled buffers, a session cap and shared idle eviction ([examples/udp-kcp](examples/udp-kcp/main.go)):

```go
type kcpEcho struct{}

func (kcpEcho) OnOpen(c *kcp.Conn)             {}
func (kcpEcho) OnData(c *kcp.Conn, data []byte) { c.Write(data) }
func (kcpEcho) OnClose(c *kcp.Conn, err error) {}

srv, err := kcp.NewServer(":9000", kcpEcho{}, kcp.Options{
	UDP:        fnet.UDPOptions{ReusePort: true},
	NoDelay:    true,
	StreamMode: true,
})
if err != nil {
	log.Fatal(err)
}
log.Fatal(srv.Serve())
```

### HTTP

Handlers are plain `net/http` ones. One difference: on the `fnet` path `Request.Context` ends with the connection (see above), not when `ServeHTTP` returns, so whatever waits on it to clean up after a request (a goroutine, `context.AfterFunc`) waits until then. A shorter version of [examples/http](examples/http/main.go), which also covers PUT/PATCH/DELETE/OPTIONS, multipart uploads and file downloads:

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
		io.Copy(w, r.Body) // a body over MaxBufferedBodyBytes is streamed by net/http
	})

	srv, err := fhttp.NewServer(":8080", mux, fhttp.Options{})
	if err != nil {
		log.Fatal(err)
	}
	log.Fatal(srv.Serve())
}
```

### HTTP/2

`http2.Server` accepts ordinary `net/http` handlers and supports HTTP/2 over TLS with ALPN `h2`, or cleartext h2c with prior knowledge. The full example, including streaming with `http.Flusher` and response trailers, is [examples/http2](examples/http2/main.go).

```go
cert, err := tls.LoadX509KeyPair("cert.pem", "key.pem")
if err != nil {
	log.Fatal(err)
}
srv, err := http2.NewServer(":8443", mux, http2.Options{
	TLSConfig: &tls.Config{Certificates: []tls.Certificate{cert}},
})
if err != nil {
	log.Fatal(err)
}
log.Fatal(srv.Serve())
```

For h2c, pass `http2.Options{}` and make clients use prior knowledge, for example `curl --http2-prior-knowledge http://localhost:8080/hello`. The package does not upgrade HTTP/1.1 connections; TLS clients negotiate `h2` through ALPN.

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

Any `tls.Config` works (`GetCertificate` for SNI or autocert, client certificates, ...). `h2` is dropped from ALPN (also from the config `GetConfigForClient` returns), so only HTTP/1.1 is negotiated (other protocols such as `acme-tls/1` are kept), the handshake has to complete within `ReadHeaderTimeout`, and `r.TLS` is set as with `net/http`.

For a custom TCP protocol, wrap its handler: `fnet.NewServer(":9443", ftls.NewHandler(echo{}, config, 0), fnet.Options{})`, where the last argument is the handshake timeout (0 means 10s).

### Gin

`*gin.Engine` implements `http.Handler`, so Gin routes, middleware and binding run on `fhttp.Server` unchanged (full version in [examples/http-gin](examples/http-gin/main.go)):

```go
package main

import (
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/linfeip/fnet/fhttp"
)

func main() {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())

	r.GET("/hello", func(c *gin.Context) {
		c.String(http.StatusOK, "hello world")
	})
	r.POST("/json", func(c *gin.Context) {
		var v map[string]any
		if err := c.ShouldBindJSON(&v); err != nil {
			c.String(http.StatusBadRequest, "%s\n", err.Error())
			return
		}
		c.JSON(http.StatusOK, gin.H{"received": v})
	})

	srv, err := fhttp.NewServer(":8080", r, fhttp.Options{})
	if err != nil {
		log.Fatal(err)
	}
	log.Fatal(srv.Serve())
}
```

### WebSocket

An echo server, the same as [examples/websocket](examples/websocket/main.go):

```go
package main

import (
	"log"
	"net/http"

	"github.com/linfeip/fnet/fhttp"
	"github.com/linfeip/fnet/websocket"
)

type echo struct{}

func (echo) OnOpen(c *websocket.Conn)                                      {}
func (echo) OnMessage(c *websocket.Conn, op websocket.OpCode, data []byte) { c.WriteMessage(op, data) } // a complete message
func (echo) OnClose(c *websocket.Conn, err error)                          {}

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

`fnet` callbacks and WebSocket `OnMessage` / `OnClose` run on fnet's private default executor (replaceable through `fnet.Options.Executor`), so they must return quickly; `http.Handler`, and with it WebSocket `OnOpen`, runs on fhttp's separate private default executor (replaceable through `fhttp.Options.Executor`), where it may block. Timeouts and limits are set in `fnet.Options`, `fhttp.Options` and `websocket.Options`; see their doc comments.

---

## Examples

The examples are a separate module (`examples/go.mod`, built against the fnet in this repository), so their dependencies, such as Gin, stay out of fnet's own `go.mod`.

```bash
cd examples
go run ./echo        # TCP echo
go run ./http        # HTTP: GET, forms, JSON, PUT/PATCH/DELETE/OPTIONS, chunked, multipart uploads
go run ./http -addr :8443 -tls   # HTTPS, with a self-signed certificate generated at startup (curl -k)
go run ./http2       # HTTP/2 over HTTPS (ALPN h2); add -tls=false for h2c
go run ./http-gin    # HTTP with Gin: routes, middleware, binding, multipart uploads (-tls for HTTPS)
go run ./websocket   # WebSocket echo
```

---

## Benchmarks

Results from [HttpArena](https://http-arena.com/) (Alpha Round, Linux, AMD Ryzen Threadripper PRO 3995WX, server container and load generator pinned to 32 physical cores / 64 threads each, best of 3 runs). Both [`fnet-h1`](https://http-arena.com/frameworks/fnet-h1/) and [`fnet-ws`](https://http-arena.com/frameworks/fnet-ws/) run in standard configuration with `ReusePort: true`.

### HTTP/1.1: `fnet-h1` vs `go-stdlib` (`net/http`)

`fnet-h1` parses requests with `http.ReadRequest`, serves standard `http.Handler` routes on `fhttp`, and runs `crypto/tls` via `ftls` for the HTTPS profiles. In the [head-to-head comparison](https://http-arena.com/compare/all/#fnet-h1-vs-go-stdlib), **`fnet-h1` leads on 11 of the 12 shared profiles** (`Latency-10K` is a fixed 10k req/sec rate test):

| Profile | Conns | `fnet-h1` req/sec | `go-stdlib` req/sec | Delta | `fnet-h1` p99 | `go-stdlib` p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Baseline | 4,096 | **2,404,492** | 504,598 | **+377%** | **8.09ms** | 60.80ms |
| Pipelined | 4,096 | **13,726,828** | 1,180,568 | **+1063%** | **21.70ms** | 1.86s |
| Short-lived | 4,096 | **2,010,211** | 169,377 | **+1087%** | **9.10ms** | 246.60ms |
| Latency-10K | 1,024 | 9,981 | **9,983** | -0% | 124.0us | **116.0us** |
| Latency-1M | 1,024 | **997,258** | 455,847 | **+119%** | **518.0us** | 10760192.0us |
| Latency-500K/8 | 1,024 | **404,289** | 213,531 | **+89%** | **4004864.0us** | 11309056.0us |
| JSON Comp | 4,096 | **230,866** | 192,578 | **+20%** | 248.80ms | **188.80ms** |
| JSON Comp | 16,384 | **228,590** | 193,771 | **+18%** | 701.70ms | **245.10ms** |
| JSON TLS | 4,096 | **457,968** | 369,093 | **+24%** | **801.33ms** | 1.89s |
| 8Gbit | 512 | **49,243** | 48,986 | **+1%** | **171.0us** | 2321.0us |
| Static TLS | 1,024 | **244,877** | 150,056 | **+63%** | 564.47ms | **370.15ms** |
| Async DB | 1,024 | **116,347** | 86,296 | **+35%** | **20.60ms** | 27.10ms |

`fnet-h1` also completes the 32,000-connection `Async Delay` profile at **1,494,896 req/sec** (p99 `41.20ms`, `841 MiB`).

### WebSocket: `fnet-ws`

From the **2026-10-12** [HttpArena WebSocket composite leaderboard](https://http-arena.com/#scope=ws&type=flagship,emerging,experimental&tuned=1) across `Flagship`, `Emerging`, and `Experimental` frameworks (including `Tuned`, 22 entries across languages), **`fnet-ws` ranked #1 overall** (composite score **2903**, #1 in Go):

| # | Framework | Language | Composite | Echo (msg/sec · Mem) | Echo Pipelined (msg/sec · Mem) | Echo Short-lived (msg/sec · Mem) |
| ---: | --- | --- | ---: | ---: | ---: | ---: |
| **1** | **fnet-ws** | Go | **2903** | 3.93M · 96 MiB | 56.80M · 91 MiB | 2.17M · 219 MiB |
| 2 | fib | Go | 2790 | 3.75M · 166 MiB | 48.86M · 167 MiB | 2.33M · 262 MiB |
| 3 | nilo | Zig | 2673 | 3.86M · 223 MiB | 56.87M · 234 MiB | 1.67M · 158 MiB |
| 4 | genhttp-ioxide | C# | 2440 | 3.40M · 1.60 GiB | 37.19M · 1.60 GiB | 2.20M · 1.50 GiB |
| 5 | wtx | Rust | 2346 | 4.01M · 179 MiB | 19.66M · 179 MiB | 2.35M · 143 MiB |
| 6 | fulmine.js | JS | 2310 | 3.66M · 2.27 GiB | 23.34M · 2.30 GiB | 2.32M · 2.25 GiB |
| 7 | uio-ws | Go | 2123 | 3.46M · 100 MiB | 46.80M · 100 MiB | 1.03M · 65 MiB |
| 8 | dusty | Zig | 1956 | 3.92M · 219 MiB | 5.08M · 194 MiB | 2.08M · 71 MiB |
| 9 | hical | C++ | 1847 | 3.53M · 235 MiB | 4.14M · 244 MiB | 2.10M · 118 MiB |
| 10 | morojs | JS | 1692 | 3.32M · 3.93 GiB | 4.35M · 4.03 GiB | 1.85M · 4.70 GiB |
| 11 | bun-websocket *(tuned)* | TS | 1667 | 3.59M · 688 MiB | 43.91M · 930 MiB | — |
| 12 | fiber | Go | 1634 | 3.06M · 549 MiB | 4.74M · 618 MiB | 1.85M · 543 MiB |

Profile columns report the mean throughput and memory across connection tiers (`512`, `4,096`, and `16,384` concurrent connections; `512` and `4,096` for `Echo Short-lived`).

---

## Limitations

- There is no write timeout, and the send buffer has no hard upper bound: a connection is not read while more than 64KB waits in it, so the replies to a client that never reads stop piling up, but what the application writes on its own (say a broadcast) still makes it grow, unless it checks `Conn.Writable` and waits for `WritableHandler.OnWritable`, as `fhttp` does (`websocket` has the same pair). Conversely, a client that writes more than the socket buffers hold before it starts reading deadlocks against a connection that echoes it, just as it would against a blocking server.
- A request `fhttp` hands to `net/http` has `net/http`'s default timeouts, none once its header has been read: a Handler reading a streamed body bounds it with `http.ResponseController.SetReadDeadline`.
- `fhttp` speaks HTTP/1.x only: no HTTP/2, and no `http.Flusher` / `http.Hijacker` (a response body over 64KB goes out while it is written, in pieces of 64KB), except on connections handed to `net/http`: those with a streamed request body, and Server-Sent Events (a request whose `Accept` mentions `text/event-stream`, as `EventSource` always sends), which then cost a goroutine each, as on `net/http`. Use `http2` for HTTP/2 over TLS (ALPN `h2`) or cleartext h2c with prior knowledge.
- A Handler writing a large body to a slow client holds a goroutine of the Handler executor while its `Write` waits, as any blocking Handler does. The default Handler executor is separate from the engine executor, so the engine can continue flushing connections while Handlers wait. If many large responses go to slow clients, configure a Handler executor with enough workers through `fhttp.Options.Executor`. On other platforms than Linux/macOS such a write blocks in the kernel, with no time limit.
- TLS is server side only. `ftls` relies on how `crypto/tls` treats a temporary read error (the connection stays usable, and so does a record cut short), which `go test ./ftls` checks: run it after upgrading Go. A custom `fnet.Options.Executor` must not lock its goroutines to OS threads.
- WebSocket: no permessage-deflate; messages are always sent as a single frame.
- Half-close is supported only by a `HalfCloseHandler` (as `fhttp` is, on Linux/macOS): for any other Handler, after EOF the connection is closed once the buffered data has been sent, and a reply another goroutine writes later is discarded.
- The kqueue path is fully tested on macOS; the epoll path has run HttpArena's HTTP/1.1 and WebSocket benchmarks on Linux and passes their validation. Run `go test -race ./...` on your Linux target before going live.

---

## License

[MIT](LICENSE)
