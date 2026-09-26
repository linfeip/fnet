# fnet

[English](README.md) | 中文

面向工业级场景的高性能 Go 网络框架：自定义 TCP 协议、HTTP/HTTPS 与 WebSocket，专为**百万级长连接（1M Connections）**、极致内存控制与超高 CPU 能效比设计。

底层 I/O 由多 Reactor 原生事件轮询器驱动。**生产环境以 Linux（epoll）为准**；macOS（kqueue）可用于开发，Windows 只是开发用的仿真实现。在完全遵循 RFC 6455 规范与 Go 标准库 `net/http` 契约的前提下，实现极高吞吐与极低长尾延迟，绝无牺牲业务功能的跑分妥协；游戏服、网关、IM、RPC 等自定义 TCP 协议也跑在同一套事件循环上。

---

## ⚡ 核心亮点

- 🚀 **百万级并发长连接**：单进程面向百万级 WebSocket 长连接设计，单连接内存脚印紧凑。
- 🔥 **高吞吐、低 CPU**：多 Reactor 轮询在回显与扇出场景下保持高吞吐，核心占用可控。
- ⚡ **高效建连**：Linux 上使用 `accept4`，配合全局共享连接表，应对大规模瞬时建连。
- 🧵 **空闲连接 0 协程常驻**：空闲套接字只挂在 Poller 上，不占用 Goroutine 栈和调度资源。
- 🎮 **自定义 TCP 协议**：`fnet.Server` 在事件循环上按你的协议拆包（`Split`，如 Netty 式的 `LengthField`），`OnMessage` 在 worker 上执行，同一连接一次一条、保序，业务可以放心查库。
- 🛡️ **严格符合业务规范与行业标准**：完整支持 RFC 6455（客户端掩码解密、Ping/Pong/Close 控制帧保活与协商、子协议与 Origin 鉴权）、完整兼容 TLS/HTTPS 与标准 `http.Handler`。

---

## 🏗️ 整体架构

各层各管一件事，另有一个执行业务代码的 worker 池：

| 层 | 包 | 职责 |
|---|---|---|
| 平台层 | `internal/netpoll` | epoll / kqueue / Windows 仿真，以及原始的非阻塞 socket 调用。唯一含平台差异的代码。 |
| Reactor 层 | `internal/reactor` | accept、事件循环、连接表、每连接的输入/输出缓冲、背压、关闭。只搬运字节。 |
| TCP 协议 | `fnet` | 消息协议的 `Server`：`Split` 在事件循环上拆包，`OnOpen` / `OnMessage` / `OnClose` 在 worker 上执行；背压、超时、半关、优雅停机 `Shutdown`。 |
| HTTP | `fhttp` | `Server` 生命周期与监听；HTTP/1.x：判断请求何时就绪、在 worker 上跑 `ServeHTTP`、keep-alive、TLS、Hijack。 |
| WebSocket | `websocket` | 握手、RFC 6455 帧与控制帧、permessage-deflate、事件驱动消息队列。 |
| Worker 池 | `pool` | 分片的有上限弹性协程池，执行 `ServeHTTP` 与 `OnMessage`；`fnet`、`fhttp` 与 `websocket` 共用，不含网络代码。 |

协议通过唯一的接口 `reactor.Handler`（`OnData` / `OnClose`，在事件循环上执行）挂到 reactor 上。连接的输入要么在事件循环上交给 handler（空闲态，0 协程），要么 *Detach* 给 worker 上的阻塞读者（为 `net/http` 与 TLS 提供 `net.Conn` 语义），用完再 `Attach` 交还。

```text
客户端
  |
  v
Acceptor（accept4）--> 事件循环（epoll / kqueue）---- 空闲连接停在这里，0 协程
                          |
                          +-- HTTP：头部收齐 --> Detach --> worker：ServeHTTP --> Attach（回到空闲）
                          |
                          +-- TCP：Split 在事件循环上拆包 --> 池化消息 --> worker：OnMessage
                          |
                          +-- WebSocket：在事件循环上解帧 --> 池化消息 --> worker：OnMessage
                          |                  |
                          |                  +-- 排队字节过多 --> 暂停读取
                          v
                    写出：直接 writev；内核缓冲满 --> 排队，由事件循环刷出
                          一批消息的回复 --> 暂存（<= 1 ms）--> 一次写出
```

---

## 核心设计与数据流转

### 1. 百万连接“零协程常驻”模型
* **空闲连接**：只挂在事件循环的 epoll/kqueue 上，占分块连接表一个指针槽位加一个 `reactor.Conn`（240 B）。不持有 Goroutine、不持有 Worker、不持有读写缓冲；64 KiB 读缓冲属于事件循环。
* **连接表设计**：分块（2048 槽位/块）原子指针数组，$O(1)$ 查找，扩容时无全局锁争用。

### 2. HTTP
* **头部就绪即调度**：头部（`\r\n\r\n`）收齐即交给 WorkerPool，不等 Body。超过 `MaxHeaderBytes`（64 KiB）的头部回 `431`，明文与 TLS 一致；畸形请求回 `400`，与 `net/http` 相同。
* **worker 上是阻塞语义**：worker 持有连接期间按普通阻塞 `net.Conn` 语义读写，两个方向都由 TCP 流控兜住：handler 还没读的上传最多缓冲 256 KiB，给慢客户端的下载让 handler 的 `Write` 等待，而不是无限排队。`io.Copy` 两个方向都是恒定内存。
* **`net/http` 行为**：`Flush` 与 `http.ResponseController`（SSE、流式响应）、客户端断开时取消 `r.Context()`、校验 `Content-Length`（多写返回 `http.ErrContentLength`，少写则关闭连接）、`103 Early Hints` 等 `1xx` 中间响应、handler panic 记入 `ErrorLog`，以及让进行中请求完成的 `Shutdown(ctx)`。
* **回到空闲**：响应写完后连接回到事件循环、释放 worker 协程；已缓冲的流水线请求则留在当前 worker。TLS 会话同样挂回 poller，保留 TLS 状态（TLS 的 keep-alive 需显式开启：设置 `IdleTimeout`）。客户端半关闭（发完请求即 FIN）照样收到完整响应。
* **超时不占协程**：连接在事件循环手里时，由每个事件循环的时间轮执行两个截止时间：请求头截止（`ReadHeaderTimeout`，默认 30s，从建连或该请求第一个字节算起，慢速滴灌无法续期）和 keep-alive 空闲截止（`IdleTimeout`，默认 2 分钟）。关闭时对端不再读走数据的连接，30s 无进展即强制关闭。
* **`Expect: 100-continue`**：handler 第一次读 body 时回 `100 Continue`；handler 不读 body 就回复的，回复后关闭连接。

### 3. WebSocket
* **事件循环解析，业务进 worker**：帧在事件循环上解析、解掩码，Ping/Close 在这里应答；完整消息拷贝进池化缓冲后排队。同一次读里完整到达的多条消息共用一块缓冲（最多 16 KiB），一批消息只走一次缓冲池，而不是每条一次。`OnMessage` 在 WorkerPool 上执行，同一连接串行且保序；`OnClose` 排在关闭前到达的消息之后。
* **分级缓冲池**：128 B 至 16 MiB 的池化缓冲；大帧随字节到达流式写入消息缓冲，内存随实际收到的字节增长，而不是随帧头声称的长度。
* **背压**：某连接排队的消息字节超过阈值（默认 64 KiB）即暂停读取，由 TCP 流控压制发送端。
* **活性检测**：事件驱动连接上 `SetReadDeadline` 到期即关闭连接（除非被推后），`OnPong` 收到对端的 Pong：服务端定期 Ping、每个 Pong 推后期限，失联的对端就会被回收。

### 4. TCP 消息协议
* **事件循环拆包，业务进 worker**：`Split`（`bufio.SplitFunc` 语义）在事件循环上只找消息边界，完整消息拷贝进池化缓冲，同一次读切出的消息共用一块（最多 16 KiB）；`OnOpen`、`OnMessage`、`OnClose` 在 WorkerPool 上执行，同一连接一次一个、保序。半包留在 reactor 里（上限 `MaxMessageSize`，默认 1 MiB），不占 worker；`Split` panic 或原地打转只关掉它自己的连接。
* **双向背压**：等待 `OnMessage` 的消息超过 `MaxPendingMessageBytes`（64 KiB）即暂停读取。对端不读时输出积压受 `MaxOutboundBytes` 限制（默认 16 MiB；广播为主的服务建议设 256 KiB 左右，几秒内就能发现卡住的玩家），超过后 `Write` 返回 `ErrWriteBufferFull`，不会无限增长。
* **超时只计对端的慢**：`ReadTimeout`（30s）从一条消息的第一个字节算起，慢速滴灌无法续期；`IdleTimeout` 是心跳超时（默认关闭）。handler 处理的时间、因背压暂停读的时间都不计。TCP keep-alive（空闲 60s 后每 15s 探测、4 次无应答断开）回收不告而别的对端。
* **关闭**：对端关闭或半关时，它发来的消息照常处理、回复刷出后再关，`OnClose` 收到 `io.EOF`；本端关闭丢弃尚未处理的消息。`Shutdown(ctx)` 停止 accept，让每个连接处理完已收到的消息，等所有 `OnClose` 执行完才返回。

### 5. 写出
* **直写优先**：写直接进 socket，内核没收下的部分才排队，由事件循环刷出。事件驱动连接（TCP 消息、事件驱动 WebSocket）上写永不阻塞：队列有上限（默认 16 MiB），满了返回 `ErrWriteBufferFull`，说明对端跟不上；写入空队列的一次写总是整条收下，消息不会在线上被截断。worker 持有连接期间（HTTP、被劫持或阻塞模式的 WebSocket）写像阻塞 socket 一样等待。
* **关闭不产生 RST**：关闭时先刷完排队的输出，再半关并给对端 500 ms 收尾，其间读到的数据直接丢弃，避免关闭变成 RST 冲掉对端还没读的响应。
* **一批消息的回复合并写**：几条消息一起到达（一次读出一批）时，worker 处理这批消息期间暂存该连接的输出，这批回复连同其间别的协程写给它的数据一次系统调用发出。暂存不超过 1 ms、不超过 64 KiB：批里有慢 handler 时，它前面的回复和广播都不会被它拖住。单条消息的回复照旧直接写出。
* **`writev`**：帧头/响应头与负载一次 `writev` 发出，无中间拷贝。

---

## 特性

- **标准 `net/http` 接口**：直接对接 `http.Handler` / `http.ServeMux`，支持 `fhttp.ListenAndServe` 与 `fhttp.ListenAndServeTLS`。
- **Multi-Reactor 多核扩展架构**：一个 Acceptor（Linux 上用 `accept4`）把连接轮询分发到各事件循环，由各循环自己注册到自己的 poller。默认每三个核一个事件循环：循环只负责读取和解帧，开得更多只会各自排队等调度器，还会把输入读到 worker 前头、积在进程里。
- **双模 WebSocket 支持**：
  - **事件驱动模式（推荐）**：连接空闲时 0 协程常驻，Reactor 读事件触发解析，业务数据包自动投递到内置工作协程池执行，绝不卡死 IO Reactor 事件循环。
  - **阻塞协程模式**：兼容传统业务模型，保留独立 Goroutine 阻塞 `ReadMessage()` / `WriteMessage()`。
- **内置工作协程池**：有上限的弹性协程池，按核分片、每片一把锁，事件循环与 worker 很少争锁；事件循环投递时从不等锁，分片锁被占就交给下一个空闲分片。worker 做完一个任务直接取下一个、不停车；停着的 worker 只为没人认领的任务唤醒，每个分片同时最多叫醒两个（其余任务按顺序等已在跑的 worker，每个 worker 取到任务时再叫醒下一个）；只有所有 worker 都忙时才新起一个；worker 停车前先去别的分片拿等着的任务——10 万条繁忙的 WebSocket 连接只需几百个 worker。worker 按需启动（上限 `MaxWorkers`，由各分片均分）、空闲超时退出，超出上限的任务按顺序排队而不是再起协程。HTTP 业务请求（`ServeHTTP`）、WebSocket 与 TCP 业务消息（`OnMessage`）默认都在这里执行，事件循环不被阻塞；同一连接的回调一次一个、按到达顺序执行。任何协程池都可以作为 `WorkerPool` 接入，例如 `pool.Adapt(ants.Submit)`；返回 error 即拒绝该任务并关闭对应连接。
- **向量化写入 (writev)**：将帧头部与数据负载通过单次系统调用直达网卡，杜绝内存拼包拷贝。
- **空闲连接内存小**：全局分块无锁连接表（O(1) 访问）、只在 worker 服务期间存在的连接状态、缓冲区自动收缩。
- **运行平台**：生产运行在 Linux (`epoll`) 上，容量和默认值都按 Linux 设计。macOS (`kqueue`) 行为一致，用于开发。Windows 为基于 `net` 包的开发用仿真（每个 socket 一个泵协程），不用于生产。

---

## 安装

```bash
go get github.com/linfeip/fnet
```

需要 Go 1.26 及以上版本（即 `go.mod` 里的 `go` 指令）。请部署在 Linux 上；百万连接还需要调高 fd 上限（`ulimit -n`、`fs.nr_open`、`fs.file-max`）。

---

## 快速上手

### 自定义 TCP 协议

一个 `| len uint32 | payload |` 格式的游戏服：

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
		Split:       fnet.LengthField{Size: 4, Strip: 4}.Split, // OnMessage 拿到的是包体
		IdleTimeout: 30 * time.Second,                          // 心跳超时
		OnOpen:      func(c *fnet.Conn) { c.SetContext(newSession(c)) },
		OnMessage: func(c *fnet.Conn, msg []byte) {
			// 在 worker 上执行，同一连接一次一条：可以查库、调下游
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

`Split` 在事件循环上只回答一个问题：目前收到的字节开头是不是一个完整的包、有多长。`LengthField` 按 Netty `LengthFieldBasedFrameDecoder` 的方式描述长度头（`Offset`、`Size` 1/2/3/4/8、`Order`、`Adjust`、`Strip`），`bufio.ScanLines` 对应按行的协议，任何 `bufio.SplitFunc` 都能用。`msg` 只在 `OnMessage` 调用期间有效，解码（如 `proto.Unmarshal`）本身就会拷贝。`Write` 从不阻塞、可在任意 goroutine 调用，比如房间 goroutine 把同一个缓冲广播给所有玩家。

### HTTP 与 HTTPS 服务

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

	// 启动普通 HTTP 服务
	log.Fatal(fhttp.ListenAndServe(":8080", mux))
}
```

配置 HTTPS 与超时参数：

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

### WebSocket 服务（事件驱动，0 协程模式）

在 `websocket.Upgrader` 中配置 `OnMessage` 回调，连接空闲时零协程开销：

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
			log.Printf("连接建立: %s", c.RemoteAddr())
		},
		OnMessage: func(c *websocket.Conn, op websocket.OpCode, msg []byte) {
			// 原样 Echo 回显
			_ = c.WriteMessage(op, msg)
		},
		OnClose: func(c *websocket.Conn, err error) {
			log.Printf("连接关闭: %v", err)
		},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		if _, err := upgrader.Upgrade(w, r); err != nil {
			log.Printf("升级失败: %v", err)
		}
		// 返回后连接自动交由 Reactor 事件循环托管
	})

	log.Fatal(fhttp.ListenAndServe(":8081", mux))
}
```

> **传统阻塞协程模式**：若未在 `Upgrader` 中指定 `OnMessage`，`Upgrade(w, r)` 将返回常规阻塞连接，可在独立 Goroutine 中使用 `for { msg, _ := conn.ReadMessage() }`。

---

## 示例程序

```bash
go run ./example                 # HTTP (:8080) 与 HTTPS (:8443) 示例
go run ./example/websocket       # 高并发 WebSocket Echo 示例 (:8081)
go run ./example/tcp -bots 3     # 聊天室游戏服示例 (:7001)，附带三个机器人
```

---

## 开源协议

[MIT](LICENSE)
