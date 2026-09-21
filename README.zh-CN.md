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

## 特性

- **标准 `net/http` 接口**：直接对接 `http.Handler` / `http.ServeMux`，支持 `fnet.ListenAndServe` 与 `fnet.ListenAndServeTLS`。
- **Multi-Reactor 多核扩展架构**：主 Poller 采用 `accept4` 高效非阻塞接入，轮询均匀分发至与 CPU 核心匹配的 Sub-Reactor 事件循环。
- **双模 WebSocket 支持**：
  - **事件驱动模式（推荐）**：连接空闲时 0 协程常驻，Reactor 读事件触发解析并直调 `OnOpen`/`OnMessage`/`OnClose`。
  - **阻塞协程模式**：兼容传统业务模型，保留独立 Goroutine 阻塞 `ReadMessage()` / `WriteMessage()`。
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
