# fnet

[English](README.md) | 中文

面向大量并发连接、低内存占用的 Go 高性能事件驱动网络库：在同一套事件循环上提供 TCP 引擎、兼容 `net/http` 的 HTTP/1.x 服务器和 WebSocket 服务端。

底层采用主从 Reactor 模型，由原生 I/O 多路复用驱动：**Linux 使用 epoll，macOS 使用 kqueue**。其它平台（含 Windows）基于标准库、一连接一个 goroutine 实现同一套 API。

---

## ⚡ 核心亮点

- 🚀 **面向百万连接**：压测中每个空闲的 HTTP keep-alive 连接约占 1KB 用户态内存。
- 🧵 **空闲连接 0 协程常驻**：空闲套接字只挂在 Poller 上，不占用 goroutine，也不持有缓冲；读缓冲只在任务读取时借用。
- ⚡ **并发安全的直接写**：`Conn.Write` / `Writev` 可在任意 goroutine 调用，永不阻塞，直接写 socket（头部 + 负载用 `writev` 一次写出）。
- 🔌 **自定义 TCP 协议**：实现 `OnOpen` / `OnData` / `OnClose`，由你告诉引擎消费了多少字节。
- 🛡️ **符合标准**：直接使用 `http.Handler` / `http.ServeMux`；WebSocket 遵循 RFC 6455（帧解析使用 gobwas/ws）。
- ⏱️ **截止时间式超时**：慢速客户端无法靠一点一点送字节来占住连接。

---

## 🏗️ 架构

| 包 | 职责 |
| --- | --- |
| `fnet` | TCP 引擎：主从 Reactor、一连接一任务、并发安全的写（`Handler`、`Conn`、`Options`） |
| `fhttp` | 基于 `fnet` 的 HTTP/1.x 服务器：请求调度、`http.Handler`、协议升级 |
| `websocket` | 基于 `fhttp` 的 WebSocket 服务端：握手、帧处理、消息回调、合并写 |
| `taskpool` | 按 CPU 分片的无锁 goroutine 池，连接任务的默认执行器 |
| `poll` | epoll / kqueue 封装 |
| `internal/bytepool` | 按大小分级的字节缓冲池 |

依赖方向是单向的：`websocket → fhttp → fnet → poll`，以及 `fnet → taskpool`。

```text
listeners（SO_REUSEPORT）─▶ 子 Reactor 0 … N-1   （每个：自己的 listener、1 个 epoll/kqueue + 它的 worker；N = GOMAXPROCS）
                                  │ worker 探测到 listener 或连接就绪
                                  ▼
                 执行器（taskpool）：同一连接同一时刻一个任务
                 读取（借用缓冲）→ OnData → 续发发送缓冲 → 关闭、OnClose
```

- **并行 accept**：Linux 上每个子 Reactor 用自己的 socket 监听同一地址（`SO_REUSEPORT`），自己 accept 到的连接由自己处理，accept 并行进行，连接留在接受它的 worker 上；`TCP_NODELAY` 和 keepalive 只在 listener 上设置一次，accept 出来的 socket 直接继承。macOS 上每个地址一个 listener，按轮询分给各子 Reactor。
- **一连接一任务**：读取、回调、关闭都在连接的任务里串行完成，同一连接的回调不会重叠且保序。没有每连接一个 goroutine，也没有入站队列。
- **零拷贝读取，内核背压**：数据直接从借用的缓冲交给 `OnData`。回调返回之前不会再读取该连接，处理慢时由 TCP 流量控制限速，而不是在内存里堆积。
- **HTTP**：回调里只找每个请求的结束位置（头部结束符 `\r\n\r\n`，以及不超过 `MaxBufferedBodyBytes`（默认 1MB）的 `Content-Length` 请求体），解析和请求体交给 `http.ReadRequest`；Handler 运行在只在连接有待处理请求时才存在的 goroutine 中。更大的或 chunked 的请求体（例如大文件上传）改由 `net/http` 流式处理：连接从 `fnet` 摘下（`Conn.Detach`），交给运行同一个 Handler 的内部 `http.Server`，处理完这个请求后关闭连接。
- **WebSocket**：帧在连接的任务里切分、解掩码，直接回调 `OnMessage`，没有消息队列；处理同一批帧期间写出的回复会合并成一次写。

---

## 安装

```bash
go get github.com/linfeip/fnet
```

需要 Go 1.23+。连接数很大时，需调大 fd 限制（`ulimit -n`、`fs.nr_open`、`fs.file-max`）。

---

## 快速开始

### TCP

```go
type echo struct{}

func (echo) OnOpen(c fnet.Conn)                  {}
func (echo) OnData(c fnet.Conn, data []byte) int { c.Write(data); return len(data) } // 返回消费的字节数
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
func (echo) OnMessage(c *websocket.Conn, op ws.OpCode, data []byte) { c.WriteMessage(op, data) } // 完整消息
func (echo) OnClose(c *websocket.Conn, err error)                   {}

mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
	websocket.Upgrade(w, r, echo{}, websocket.Options{})
})
```

回调由执行器执行（默认 `taskpool.DefaultTaskPool`，可通过 `fnet.Options.Executor` 替换），因此必须快速返回。超时与各项上限在 `fnet.Options`、`fhttp.Options`、`websocket.Options` 中设置，详见其文档注释。

---

## 示例

示例是一个独立的 module（`examples/go.mod`，编译时使用本仓库里的 fnet），它的依赖（例如 HTTP 压测用到的 fasthttp）不会进入 fnet 自己的 `go.mod`。

```bash
cd examples
go run ./echo        # TCP echo
go run ./http        # HTTP：GET、表单、JSON、PUT/PATCH/DELETE/OPTIONS、chunked、multipart 上传
go run ./websocket   # WebSocket echo
go test ./http -run XXX -bench GET   # GET 压测：fhttp、net/http、fasthttp 对比
```

---

## 实测数据

macOS、10 核，wrk 与服务端运行在同一台机器上，hello world Handler：

| 场景 | fhttp | net/http |
| --- | --- | --- |
| `wrk -t4 -c256 -d10s` 吞吐 | 约 21.1 万 req/s | 约 20.9 万 req/s |
| 15000 个空闲 keep-alive 连接，RSS 增量 | 13MB（约 1KB/连接） | 271MB（约 19KB/连接） |
| 15000 个空闲连接时的 goroutine 数 | 12 | 15003 |

吞吐持平是因为 CPU 主要耗在系统调用（回环网络栈）上；收益在于大量连接下的内存与 goroutine 数量。

---

## 当前限制

- 发送缓冲没有上限，也没有写超时：只发请求却从不读取的客户端会让它持续增长。
- 不支持 TLS、HTTP/2。响应体完整缓存在内存中（不支持 `http.Flusher` / `http.Hijacker`），为流式请求体交给 `net/http` 的连接除外。
- WebSocket：不支持 permessage-deflate，发送的消息总是单帧。
- 不支持半关闭：读到 EOF 后，发完已缓冲的数据就关闭连接。
- kqueue 路径已在 macOS 上完整测试；epoll 路径已交叉编译并通过 `go vet`，但**尚未在 Linux 上实际运行**，上线前请在 Linux 上执行 `go test -race ./...`。

---

## 许可证

[MIT](LICENSE)
