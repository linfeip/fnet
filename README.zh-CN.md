# fnet

[English](README.md) | 中文

面向工业级场景的高性能 HTTP/HTTPS 与 WebSocket 网络引擎，专为**百万级长连接（1M Connections）**、极致内存控制与超高 CPU 能效比设计。

底层 I/O 由多 Reactor 原生事件轮询器驱动（**Linux 使用 epoll，macOS 使用 kqueue；Windows 为开发用的仿真实现**）。在完全遵循 RFC 6455 规范与 Go 标准库 `net/http` 契约的前提下，实现极高吞吐与极低长尾延迟，绝无牺牲业务功能的跑分妥协。

---

## ⚡ 核心亮点

- 🚀 **百万级并发长连接**：单进程面向百万级 WebSocket 长连接设计，单连接内存脚印紧凑。
- 🔥 **高吞吐、低 CPU**：多 Reactor 轮询在回显与扇出场景下保持高吞吐，核心占用可控。
- ⚡ **高效建连**：Linux 上使用 `accept4`，配合全局共享连接表，应对大规模瞬时建连。
- 🧵 **空闲连接 0 协程常驻**：空闲套接字只挂在 Poller 上，不占用 Goroutine 栈和调度资源。
- 🛡️ **严格符合业务规范与行业标准**：完整支持 RFC 6455（客户端掩码解密、Ping/Pong/Close 控制帧保活与协商、子协议与 Origin 鉴权）、完整兼容 TLS/HTTPS 与标准 `http.Handler`。

---

## 🏗️ 整体架构

四层各管一件事，另有一个执行业务代码的 worker 池：

| 层 | 包 | 职责 |
|---|---|---|
| 平台层 | `internal/netpoll` | epoll / kqueue / Windows 仿真，以及原始的非阻塞 socket 调用。唯一含平台差异的代码。 |
| Reactor 层 | `internal/reactor` | accept、事件循环、连接表、每连接的输入/输出缓冲、背压、关闭。只搬运字节。 |
| Server + HTTP | `fhttp` | `Server` 生命周期与监听；HTTP/1.x：判断请求何时就绪、在 worker 上跑 `ServeHTTP`、keep-alive、TLS、Hijack。 |
| WebSocket | `websocket` | 握手、RFC 6455 帧与控制帧、permessage-deflate、事件驱动消息队列。 |
| Worker 池 | `pool` | 分片协程池，执行 `ServeHTTP` 与 `OnMessage`；`fhttp` 与 `websocket` 共用，不含网络代码。 |

协议通过唯一的接口 `reactor.Handler`（`OnData` / `OnClose`，在事件循环上执行）挂到 reactor 上。连接的输入要么在事件循环上交给 handler（空闲态，0 协程），要么 *Detach* 给 worker 上的阻塞读者（为 `net/http` 与 TLS 提供 `net.Conn` 语义），用完再 `Attach` 交还。

```text
客户端
  |
  v
Acceptor（accept4）--> 事件循环（epoll / kqueue）---- 空闲连接停在这里，0 协程
                          |
                          +-- HTTP：头部收齐 --> Detach --> worker：ServeHTTP --> Attach（回到空闲）
                          |
                          +-- WebSocket：在事件循环上解帧 --> 池化消息 --> worker：OnMessage
                          |                  |
                          |                  +-- 排队字节过多 --> 暂停读取
                          v
                    写出：直接 writev；内核缓冲满 --> 排队，由事件循环刷出
```

---

## 核心设计与数据流转

### 1. 百万连接“零协程常驻”模型
* **空闲连接**：只挂在事件循环的 epoll/kqueue 上，占分块连接表一个指针槽位加一个 `reactor.Conn`（256 B）。不持有 Goroutine、不持有 Worker、不持有读写缓冲；64 KiB 读缓冲属于事件循环。
* **连接表设计**：分块（2048 槽位/块）原子指针数组，$O(1)$ 查找，扩容时无全局锁争用。

### 2. HTTP
* **头部就绪即调度**：头部（`\r\n\r\n`）收齐即交给 WorkerPool，不等 Body。迟迟不结束的头部上限 64 KiB。
* **流式 Body**：worker 持有连接期间按普通阻塞 `net.Conn` 语义读，`io.Copy(dst, req.Body)` 内存恒定。
* **回到空闲**：明文响应写完后连接回到事件循环、释放 worker 协程；已缓冲的流水线请求则留在当前 worker。客户端半关闭（发完请求即 FIN）照样收到响应。
* **超时不占协程**：连接在事件循环手里时，由每个事件循环的时间轮执行两个截止时间：请求头截止（`ReadHeaderTimeout`，默认 30s，从建连或该请求第一个字节算起，慢速滴灌无法续期）和 keep-alive 空闲截止（`IdleTimeout`，默认 2 分钟）。关闭时对端不再读走数据的连接，30s 无进展即强制关闭。
* **`Expect: 100-continue`**：handler 第一次读 body 时回 `100 Continue`；handler 不读 body 就回复的，回复后关闭连接。

### 3. WebSocket
* **事件循环解析，业务进 worker**：帧在事件循环上解析、解掩码，Ping/Close 在这里应答；完整消息拷贝进池化缓冲后排队。`OnMessage` 在 WorkerPool 上执行，同一连接串行且保序；`OnClose` 排在关闭前到达的消息之后。
* **分级缓冲池**：128 B 至 16 MiB 的池化缓冲；大帧随字节到达流式写入消息缓冲，内存随实际收到的字节增长，而不是随帧头声称的长度。
* **背压**：某连接排队的消息字节超过阈值（默认 64 KiB）即暂停读取，由 TCP 流控压制发送端。

### 4. 写出
* **直写优先**：写直接进 socket，内核没收下的部分才排队（每连接上限 16 MiB），由事件循环刷出。排队超过 64 KiB 时暂停读取，刷到阈值以下再恢复。
* **`writev`**：帧头/响应头与负载一次 `writev` 发出，无中间拷贝。

---

## 特性

- **标准 `net/http` 接口**：直接对接 `http.Handler` / `http.ServeMux`，支持 `fhttp.ListenAndServe` 与 `fhttp.ListenAndServeTLS`。
- **Multi-Reactor 多核扩展架构**：一个 Acceptor（Linux 上用 `accept4`）把连接轮询分发到与 CPU 核数匹配的事件循环。
- **双模 WebSocket 支持**：
  - **事件驱动模式（推荐）**：连接空闲时 0 协程常驻，Reactor 读事件触发解析，业务数据包自动投递到内置工作协程池执行，绝不卡死 IO Reactor 事件循环。
  - **阻塞协程模式**：兼容传统业务模型，保留独立 Goroutine 阻塞 `ReadMessage()` / `WriteMessage()`。
- **内置高并发工作协程池**：零外部依赖，多分片架构，连接级严格保序（FIFO），空闲协程自动超时回收，完美支撑 100万+（1M）长连接。HTTP 业务请求（`ServeHTTP`）与 WebSocket 业务数据包（`OnMessage`）默认全部在业务协程池中调度执行，IO Reactor 彻底不阻塞。
- **向量化写入 (writev)**：将帧头部与数据负载通过单次系统调用直达网卡，杜绝内存拼包拷贝。
- **极致内存剪枝技术**：全局分块无锁连接表（O(1) 访问）、全链路 ResponseWriter 对象池化、紧凑延迟 IP 解析、缓冲区自动收缩。
- **跨平台支持**：Linux (`epoll`)、macOS (`kqueue`)。Windows 为基于 `net` 包的开发用仿真（每个 socket 一个泵协程）。

---

## 安装

```bash
go get github.com/linfeip/fnet
```

需要 Go 1.21 及以上版本。

---

## 快速上手

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
```

---

## 开源协议

[MIT](LICENSE)
