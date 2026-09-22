# fnet

[English](README.md) | 中文

面向工业级场景的高性能 HTTP/HTTPS 与 WebSocket 网络引擎，专为**百万级长连接（1M Connections）**、极致内存控制与超高 CPU 能效比设计。

底层 I/O 由多 Reactor 原生事件轮询器驱动（**Linux 使用 epoll，macOS 使用 kqueue，Windows 使用 WSAPoll**）。在完全遵循 RFC 6455 规范与 Go 标准库 `net/http` 契约的前提下，实现极高吞吐与极低长尾延迟，绝无牺牲业务功能的跑分妥协。

---

## ⚡ 核心亮点

- 🚀 **百万级并发长连接**：单进程面向百万级 WebSocket 长连接设计，单连接内存脚印紧凑。
- 🔥 **高吞吐、低 CPU**：多 Reactor 轮询在回显与扇出场景下保持高吞吐，核心占用可控。
- ⚡ **高效建连**：Linux 上使用 `accept4`，配合全局共享连接表，应对大规模瞬时建连。
- 🧵 **空闲连接 0 协程常驻**：空闲套接字只挂在 Poller 上，不占用 Goroutine 栈和调度资源。
- 🛡️ **严格符合业务规范与行业标准**：完整支持 RFC 6455（客户端掩码解密、Ping/Pong/Close 控制帧保活与协商、子协议与 Origin 鉴权）、完整兼容 TLS/HTTPS 与标准 `http.Handler`。

---

## 🏗️ 整体架构图

fnet 采用**“多 Reactor 事件驱动 I/O + VirtualConn 虚拟抽象桥梁 + 分片工作协程池”**的混合高性能架构。既享有 Reactor 驱动百万级并发连接的超轻量开销，又完整兼容 Go 标准库的阻塞式业务逻辑。

```text
客户端
  |
  v
Main Reactor（accept）
  |
  v
Sub-Reactor（epoll / kqueue）---- 空闲连接停在这里，0 协程
  |
  +-- HTTP：头部收齐（\r\n\r\n）--> 协程池 --> ServeHTTP
  |
  +-- WebSocket：完整帧 --> 解掩码 --> 缓冲池 --> 协程池 --> OnMessage
  |                 |
  |                 +-- 积压过高 --> 暂停读取
  |
  v
写出：writev 直达 socket
  |
  +-- 内核缓冲满 --> 排队，可写时由 Reactor 刷出
```

---

## 核心设计与数据流转

### 1. 百万连接“零协程常驻”模型
* **空闲连接**：仅挂在 Reactor 的 epoll/kqueue 事件树上，在全局分块连接表中仅占一个指针槽位。连接空闲时不持有任何 Goroutine、不持有 Worker、不持有读写缓冲区。
* **连接表设计**：采用分块（2048 槽位/块）原子指针数组，支持高达百万连接的并发 $O(1)$ 查找，杜绝高并发扩容锁争用。

### 2. HTTP 极速流式链路
* **头部就绪即调度**：Reactor 持续读取 TCP 数据，当检测到完整 HTTP 头部（`\r\n\r\n`）时立刻交割给 WorkerPool 执行 `ServeHTTP`，不等待 Body 接收完毕。
* **支持大文件与 `io.Copy`**：通过 `VirtualConn` 实现标准 `net.Conn` 语义。在 Handler 中进行 `io.Copy(dst, req.Body)` 时，数据边从网卡读入边刷盘，无需在内存全量缓存，内存始终保持恒定几十 KB。

### 3. WebSocket 高吞吐与大包优化
* **原地无分配解析**：直接在 Reactor 共享的 64KB 读缓冲上解析帧头，并调用 `gobwas/ws` 通过 **SIMD（AVX2 / NEON）向量指令**原地解掩码（Unmask）。
* **阶梯式分级对象池（Slab Pool）**：内置覆盖 128B 至 16MB 的多级缓冲区对象池，16MB 以内数据帧**完全零堆分配**（0 Heap Allocations）。
* **大包流式组装器（Streaming Assembler）**：当遇到大于 64KB 的大包时，直接将网卡数据流式组装至专用的池化大缓冲区中，**彻底消除多次 `append` 扩容与中间全量内存拷贝**。组装完毕所有权直接移交 Task，`OnMessage` 退出后由框架底层安全自动回收。
* **静默流控回压（Silent Backpressure）**：当某连接未处理的消息字节数超过阈值（默认 4MB）时，自动触发 `PauseRead` 暂停读取该连接 Socket，利用 TCP 滑动窗口物理压制发送端，从根本上杜绝大包瞬时并发导致的单机 OOM。

### 4. 输出直写与向量化写入 (`writev`)
* **直写优先（Fast Path）**：写出时优先执行非阻塞系统调用直达网卡，只要内核缓冲区未满，即可享受到 0 协程调度延迟的直出性能。
* **零拷贝拼包**：通过 `WriteVector` (`writev`) 将栈上编码的 2~10 字节帧头与业务 Payload 直接合成 `iovec` 发送，避免在用户态进行拼包内存拷贝。

---

## 特性

- **标准 `net/http` 接口**：直接对接 `http.Handler` / `http.ServeMux`，支持 `fnet.ListenAndServe` 与 `fnet.ListenAndServeTLS`。
- **Multi-Reactor 多核扩展架构**：主 Poller 采用 `accept4` 高效非阻塞接入，轮询均匀分发至与 CPU 核心匹配的 Sub-Reactor 事件循环。
- **双模 WebSocket 支持**：
  - **事件驱动模式（推荐）**：连接空闲时 0 协程常驻，Reactor 读事件触发解析，业务数据包自动投递到内置工作协程池执行，绝不卡死 IO Reactor 事件循环。
  - **阻塞协程模式**：兼容传统业务模型，保留独立 Goroutine 阻塞 `ReadMessage()` / `WriteMessage()`。
- **内置高并发工作协程池**：零外部依赖，多分片架构，连接级严格保序（FIFO），空闲协程自动超时回收，完美支撑 100万+（1M）长连接。HTTP 业务请求（`ServeHTTP`）与 WebSocket 业务数据包（`OnMessage`）默认全部在业务协程池中调度执行，IO Reactor 彻底不阻塞。
- **向量化写入 (writev)**：将帧头部与数据负载通过单次系统调用直达网卡，杜绝内存拼包拷贝。
- **极致内存剪枝技术**：全局分块无锁连接表（O(1) 访问）、全链路 ResponseWriter 对象池化、紧凑延迟 IP 解析、缓冲区自动收缩。
- **跨平台支持**：Linux (`epoll`)、macOS (`kqueue`)、Windows (`WSAPoll`)。

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

	"github.com/linfeip/fnet"
)

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "hello from fnet\n")
	})

	// 启动普通 HTTP 服务
	log.Fatal(fnet.ListenAndServe(":8080", mux))
}
```

配置 HTTPS 与超时参数：

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

### WebSocket 服务（事件驱动，0 协程模式）

在 `websocket.Upgrader` 中配置 `OnMessage` 回调，连接空闲时零协程开销：

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

	log.Fatal(fnet.ListenAndServe(":8081", mux))
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
