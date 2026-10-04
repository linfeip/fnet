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
- ⏱️ **Deadline-style timeouts**: slow clients cannot hold a connection by trickling bytes.

---

## 🏗️ Architecture

| Package | Responsibility |
| --- | --- |
| `fnet` | TCP engine: main/sub-reactors, one task per connection, concurrency-safe writes (`Handler`, `Conn`, `Options`) |
| `fhttp` | HTTP/1.x server on `fnet`: request dispatch, `http.Handler`, protocol upgrade |
| `websocket` | WebSocket server on `fhttp`: handshake, framing, message callbacks, write coalescing |
| `taskpool` | Per-CPU sharded lock-free goroutine pool, the default executor of connection tasks |
| `poll` | epoll / kqueue wrapper |
| `internal/bytepool` | Size-class byte buffer pool |

Dependencies are one-way: `websocket → fhttp → fnet → poll`, and `fnet → taskpool`.

```text
listener ─▶ main reactor (accept) ── round-robin ─▶ sub-reactor 0 … N-1   (each: 1 goroutine + 1 epoll/kqueue)
                                                          │ a connection has events
                                                          ▼
                 executor (taskpool): one task per connection at a time
                 read (borrowed buffer) → OnData → flush the send buffer → close, OnClose
```

- **One task per connection**: reading, callbacks and closing run serially in the connection's task, so the callbacks of one connection never overlap and stay in order. There is no goroutine per connection and no inbound queue.
- **Zero-copy reads, kernel backpressure**: data is handed to `OnData` straight from a borrowed buffer. A connection is not read again until its callback returns, so a slow handler is throttled by TCP flow control instead of piling up memory.
- **HTTP**: the callback only looks for the end of each request (the header terminator `\r\n\r\n`, then a `Content-Length` body of up to `MaxBufferedBodyBytes`, 1MB by default); parsing and the body are left to `http.ReadRequest`, and the Handler runs on a goroutine that exists only while the connection has pending requests. A larger or chunked body (say a big file upload) is streamed by `net/http` instead: the connection is detached from `fnet` (`Conn.Detach`) and handed to an internal `http.Server` running the same Handler, which closes it after that request.
- **WebSocket**: frames are split and unmasked inside the connection's task and passed to `OnMessage` without a message queue; replies written while handling a batch of frames are coalesced into one write.

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

```go
mux := http.NewServeMux()
mux.HandleFunc("GET /hello", func(w http.ResponseWriter, r *http.Request) {
	io.WriteString(w, "hello world")
})
log.Fatal(fhttp.ListenAndServe(":8080", mux))
```

### WebSocket

```go
type echo struct{}

func (echo) OnOpen(c *websocket.Conn)                               {}
func (echo) OnMessage(c *websocket.Conn, op ws.OpCode, data []byte) { c.WriteMessage(op, data) } // a complete message
func (echo) OnClose(c *websocket.Conn, err error)                   {}

mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
	websocket.Upgrade(w, r, echo{}, websocket.Options{})
})
```

Callbacks run on the executor (`taskpool.DefaultTaskPool` by default, replaceable through `fnet.Options.Executor`), so they must return quickly. Timeouts and limits are set in `fnet.Options`, `fhttp.Options` and `websocket.Options`; see their doc comments.

---

## Examples

The examples are a separate module (`examples/go.mod`, built against the fnet in this repository), so their dependencies, such as fasthttp for the HTTP benchmark, stay out of fnet's own `go.mod`.

```bash
cd examples
go run ./echo        # TCP echo
go run ./http        # HTTP: GET, forms, JSON, PUT/PATCH/DELETE/OPTIONS, chunked, multipart uploads
go run ./websocket   # WebSocket echo
go test ./http -run XXX -bench GET   # GET benchmark: fhttp vs net/http vs fasthttp
```

---

## Benchmarks

macOS, 10 cores, wrk and the server on the same machine, hello world Handler:

| Scenario | fhttp | net/http |
| --- | --- | --- |
| `wrk -t4 -c256 -d10s` throughput | about 211k req/s | about 209k req/s |
| 15000 idle keep-alive connections, RSS increase | 13MB (about 1KB/connection) | 271MB (about 19KB/connection) |
| Goroutines with 15000 idle connections | 12 | 15003 |

Throughput is on par because most CPU goes to system calls (the loopback stack); the gain is memory and goroutine count with many connections.

---

## Limitations

- The send buffer has no upper bound and there is no write timeout: a client that never reads makes it grow.
- No TLS or HTTP/2. Responses are fully buffered (no `http.Flusher` / `http.Hijacker`), except on connections handed to `net/http` for a streamed request body.
- WebSocket: no permessage-deflate; messages are always sent as a single frame.
- Half-close is not supported: after EOF the connection is closed once the buffered data has been sent.
- The kqueue path is fully tested on macOS; the epoll path is cross-compiled and passes `go vet` but **has not been run on Linux yet**. Run `go test -race ./...` on Linux before going live.

---

## License

[MIT](LICENSE)
