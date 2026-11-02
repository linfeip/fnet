# fnet

[English](README.md) | 中文

面向大量并发连接、低内存占用的 Go 高性能事件驱动网络库：在同一套事件循环上提供 TCP 引擎、兼容 `net/http` 的 HTTP/1.x 服务器和 WebSocket 服务端。

底层采用主从 Reactor 模型，由原生 I/O 多路复用驱动：**Linux 使用 epoll，macOS 使用 kqueue**。其它平台（含 Windows）基于标准库实现同一套 API。

---

## ⚡ 核心亮点

- 🚀 **面向百万连接**：压测中每个空闲的 HTTP keep-alive 连接约占 1KB 用户态内存。
- 🧵 **空闲连接 0 协程常驻**：空闲套接字只挂在 Poller 上，不占用 goroutine，也不持有缓冲；读缓冲只在任务读取时借用。
- ⚡ **并发安全的直接写**：`Conn.Write` / `Writev` 可在任意 goroutine 调用，永不阻塞，直接写 socket（头部 + 负载用 `writev` 一次写出）。
- 🔌 **自定义 TCP 协议**：实现 `OnOpen` / `OnData` / `OnClose`，由你告诉引擎消费了多少字节。
- 🛡️ **符合标准**：直接使用 `http.Handler` / `http.ServeMux`；WebSocket 遵循 RFC 6455。
- 🔒 **TLS 不需要每连接一个 goroutine**：HTTPS 和 WSS 只需一个选项（`fhttp.Options.TLSConfig`）；标准库 `crypto/tls` 直接在连接的回调里运行，`ftls` 可以给任意 `fnet.Handler` 加上 TLS。
- ⏱️ **截止时间式超时**：慢速客户端无法靠一点一点送字节来占住连接。

---

## 🏗️ 架构

| 包 | 职责 |
| --- | --- |
| `fnet` | TCP 引擎：主从 Reactor、一连接一任务、并发安全的写（`Handler`、`Conn`、`Options`） |
| `ftls` | 基于 `fnet` 的 TLS：包装任意 `Handler`，在回调里运行标准库 `crypto/tls` |
| `fhttp` | 基于 `fnet` 的 HTTP/1.x 服务器：请求调度、`http.Handler`、协议升级、HTTPS（经由 `ftls`） |
| `websocket` | 基于 `fhttp` 的 WebSocket 服务端：握手、帧处理、消息回调、合并写 |
| `taskpool` | 按 CPU 分片的无锁 goroutine 池，连接任务的默认执行器 |
| `poll` | epoll / kqueue 封装 |
| `internal/bytepool` | 按大小分级的字节缓冲池 |

依赖方向是单向的：`websocket → fhttp → ftls → fnet → poll`，以及 `fnet`、`fhttp` → `taskpool`。

```text
用户 Handler（只有它看到业务数据）
   ↑ OnOpen / OnData / OnClose            ↓ Conn.Write（任意 goroutine，不阻塞）
┌────────────────────────────────────────────────────────────┐
│ 连接任务：一连接一任务，串行执行，回调不重叠               │
│ 首次 注册到 Poller → OnOpen；之后 续发缓冲 → 读 → OnData   │
└────────────────────────────────────────────────────────────┘
   ↑ 就绪事件：提交该连接的任务（同一连接同一时刻最多一个，固定同一分片）
┌────────────────────────────────────────────────────────────┐
│ 执行器 taskpool：按 CPU 分片的无锁 goroutine 池            │
└────────────────────────────────────────────────────────────┘
   ↑ 事件就绪，转交连接任务
┌────────────────────────────────────────────────────────────┐
│ 子 Reactor × N：1 个 epoll/kqueue + 1 个 worker goroutine  │  ← N = max(2, GOMAXPROCS/8)
│ worker 不运行用户代码，空闲时帮其它子 Reactor 轮询，       │
│ 所有子 Reactor 都空闲时才阻塞等待                          │
└────────────────────────────────────────────────────────────┘
   ↑ accept 新连接（默认轮询分发；ReusePort 时各自 accept 并保留连接）
listener（默认每地址 1 个；ReusePort 时每个子 Reactor 1 个）
   ↑ accept                                ↓ 读写 socket
内核：epoll / kqueue + socket

Conn.Write 没有积压时直接写 socket，其余进发送缓冲，可写时由连接任务续发
```

- **并行 accept**：开启 `Options.ReusePort` 时，Linux 上每个子 Reactor 用自己的 socket 监听同一地址（`SO_REUSEPORT`），自己 accept 到的连接由自己处理，accept 并行进行，连接留在接受它的 worker 上；否则（macOS 上总是如此）每个地址一个 listener，按轮询分给各子 Reactor。Linux 上 keepalive 和 `TCP_NODELAY`（开启 `Options.NoDelay` 时）只在 listener 上设置一次，accept 出来的 socket 直接继承。
- **一连接一任务**：读取、回调、关闭都在连接的任务里串行完成，同一连接的回调不会重叠且保序。没有每连接一个 goroutine，也没有入站队列。
- **零拷贝读取，内核背压**：数据直接从借用的缓冲交给 `OnData`。回调返回之前不会再读取该连接，发送缓冲积压超过 64KB 时也不读取，因此处理慢或客户端不读回复时由 TCP 流量控制限速，而不是在内存里堆积。
- **优雅关闭**：本端关闭的连接先只关闭写方向，把对端仍在发送的数据读掉丢弃，最多约两秒，避免内核回复 RST 导致对端丢掉最后一个响应（RFC 9112 9.6）。引擎自行关闭连接时（对端 EOF、截止时间到期、服务器关闭），`ClosingHandler` 还能先写出数据；`ftls` 在这里发送 TLS close_notify。
- **HTTP**：回调里只找每个请求的结束位置（头部结束符 `\r\n\r\n`，头部不超过 `MaxHeaderBytes`（默认 16KB）和 500 个字段行，以及不超过 `MaxBufferedBodyBytes`（默认 1MB）、带 `Content-Length` 或普通 chunked 的请求体）。每个请求都交给标准库 `http.ReadRequest` 解析；Handler 运行在 executor（`Options.Executor`，默认是该服务器私有的 `taskpool.NewDefault()`）的 goroutine 上，只在连接有待处理请求时占用。客户端连接关闭，或客户端关闭其写方向时，`Request.Context` 被取消（它发出的请求仍会得到响应，和 `net/http` 一致）。一起排队的多个请求（pipelining）的响应合并成一次写出；客户端不接收响应时（积压超过 64KB，见 `Conn.Writable`），在它接收之前不再继续应答。超过 64KB 的响应体在 Handler 写入时就边写边发，`Write` 同样会等客户端接收，所以向慢客户端发送大文件时，内存里最多只有约 192KB 的响应体。更大的或分帧方式特殊的请求体（例如大文件上传）改由 `net/http` 流式处理：连接从 `fnet` 摘下（`Conn.Detach`），交给运行同一个 Handler 的内部 `http.Server`，处理完这个请求后关闭连接。需要 `http.Flusher` 的 Server-Sent Events 请求也这样处理。
- **WebSocket**：帧在连接的任务里切分、解掩码，直接回调 `OnMessage`，没有消息队列；处理同一批帧期间写出的回复会合并成一次写。
- **TLS**：`ftls` 自己不实现 TLS 协议，握手和加解密都由标准库 `crypto/tls` 完成，它底下的 `net.Conn` 由连接的回调供给数据。阻塞式的握手放在协程（`iter.Pull`）里运行，`OnData` 每收到一段数据就恢复它一次，所以没有 goroutine 在等客户端；握手完成后，`OnData` 直接解密手头的 record，把明文交给业务。空闲的 TLS 连接不占 goroutine；流式请求体交给 `net/http` 时，TLS 会话一并移交。

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

Handler 还可以实现几个可选接口：`ClosingHandler`（`OnClosing`，引擎自行关闭连接前写出数据）、`WritableHandler`（`OnWritable`，`Conn.Writable` 重新为 true 时恢复写出，仅 Linux/macOS）、`HalfCloseHandler`（`OnHalfClose`，对端关闭写方向后保持连接以便继续应答，仅 Linux/macOS）和 `ResumeHandler`（`OnResume`，把暂停读取期间自己留下的数据继续交出）。

### HTTP

Handler 就是普通的 `net/http` Handler。有一点不同：在 `fnet` 路径上，`Request.Context` 随连接结束（见上文），而不是在 `ServeHTTP` 返回时结束，所以等它来做请求收尾的东西（goroutine、`context.AfterFunc`）要等到那时。下面是 [examples/http](examples/http/main.go) 的精简版，完整示例还包含 PUT/PATCH/DELETE/OPTIONS、multipart 上传和文件下载：

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

### HTTPS

同一个服务器加上证书即可；WSS 用的也是这个选项：

```go
cert, err := tls.LoadX509KeyPair("cert.pem", "key.pem")
if err != nil {
	log.Fatal(err)
}
srv, err := fhttp.NewServer(":8443", mux, fhttp.Options{
	TLSConfig: &tls.Config{Certificates: []tls.Certificate{cert}},
})
```

任何 `tls.Config` 都可以用（按 SNI 选证书或 autocert 用的 `GetCertificate`、客户端证书等）。ALPN 中会去掉 `h2`（`GetConfigForClient` 返回的配置同样如此），只协商 HTTP/1.1（`acme-tls/1` 等其它协议保留），握手须在 `ReadHeaderTimeout` 内完成，`r.TLS` 与 `net/http` 一样会被设置。

自定义 TCP 协议直接包装它的 handler：`fnet.NewServer(":9443", ftls.NewHandler(echo{}, config, 0), fnet.Options{})`，最后一个参数是握手超时（0 表示 10s）。

### WebSocket

一个 echo 服务端，与 [examples/websocket](examples/websocket/main.go) 相同：

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

Linux 上设置 `fhttp.Options{Engine: fnet.Options{ReusePort: true}}` 后，每个子 Reactor 都有自己的 listener。

`fnet` 的回调以及 WebSocket 的 `OnMessage` / `OnClose` 由 fnet 私有的默认执行器执行（可通过 `fnet.Options.Executor` 替换），因此必须快速返回；`http.Handler`（以及在其中运行的 WebSocket `OnOpen`）运行在 fhttp 独立的私有默认执行器上（可通过 `fhttp.Options.Executor` 替换），可以阻塞。超时与各项上限在 `fnet.Options`、`fhttp.Options`、`websocket.Options` 中设置，详见其文档注释。

---

## 示例

示例是一个独立的 module（`examples/go.mod`，编译时使用本仓库里的 fnet），它的依赖（例如 HTTP 压测用到的 fiber）不会进入 fnet 自己的 `go.mod`。

```bash
cd examples
go run ./echo        # TCP echo
go run ./http        # HTTP：GET、表单、JSON、PUT/PATCH/DELETE/OPTIONS、chunked、multipart 上传
go run ./http -addr :8443 -tls   # HTTPS，启动时自动生成自签名证书（curl 需加 -k）
go run ./websocket   # WebSocket echo
go test ./http -run XXX -bench GET   # GET 压测：fhttp、net/http、fiber对比
```

---

## 当前限制

- 没有写超时，发送缓冲也没有硬上限：积压超过 64KB 时连接暂停读取，从不读取的客户端不会让回复无限堆积；但应用自己主动写出的数据（例如广播）仍会让它增长，除非像 `fhttp` 那样先检查 `Conn.Writable`，再等待 `WritableHandler.OnWritable`（`websocket` 也提供同样的一对接口）。反过来，客户端在开始读取之前写入的数据超过 socket 缓冲能容纳的量，就会和回显它的连接互相等待而卡死，与阻塞式服务端的行为相同。
- `fhttp` 交给 `net/http` 的请求沿用 `net/http` 的默认超时，读完请求头后不再有超时：读取流式请求体的 Handler 请用 `http.ResponseController.SetReadDeadline` 自行限定。
- 不支持 HTTP/2：HTTPS 只有 HTTP/1.1。不支持 `http.Flusher` / `http.Hijacker`（超过 64KB 的响应体按 64KB 一段边写边发），交给 `net/http` 的连接除外：带流式请求体的请求，以及 Server-Sent Events（`Accept` 中带有 `text/event-stream` 的请求，`EventSource` 总会发送）。这类连接和在 `net/http` 上一样，每条占一个 goroutine。
- 向慢客户端写大响应体的 Handler，在 `Write` 等待期间会一直占用 Handler executor 的一个 goroutine，和其他阻塞的 Handler 一样。默认 Handler executor 与引擎执行器分开，所以 Handler 等待时引擎仍能继续刷新连接。如果有大量大响应要发给慢客户端，请通过 `fhttp.Options.Executor` 配置足够 worker。在 Linux/macOS 以外的平台，这样的写在内核里阻塞，没有时间限制。
- TLS 只支持服务端。`ftls` 依赖 `crypto/tls` 对临时性读错误的处理方式：连接继续可用，读到一半的 record 也保留，这一点由 `go test ./ftls` 检查，升级 Go 后请先跑一遍。自定义的 `fnet.Options.Executor` 不能把它的 goroutine 锁定到系统线程上（`LockOSThread`）。
- WebSocket：不支持 permessage-deflate，发送的消息总是单帧。
- 只有 `HalfCloseHandler` 支持半关闭（`fhttp` 就是，仅 Linux/macOS）：其他 Handler 读到 EOF 后，发完已缓冲的数据就关闭连接，其他 goroutine 之后才写的回复会被丢弃。
- kqueue 路径已在 macOS 上完整测试；epoll 路径已在 Linux 上跑过 HttpArena 的 WebSocket 压测，并通过其校验。上线前请在目标 Linux 环境执行 `go test -race ./...`。

---

## 许可证

[MIT](LICENSE)
