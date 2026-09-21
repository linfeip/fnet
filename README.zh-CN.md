# fnet

[English](README.md) | 中文

面向工业级场景的高性能 HTTP/HTTPS 与 WebSocket 网络引擎，专为**百万级长连接（1M Connections）**、极致内存控制与超高 CPU 能效比设计。

底层 I/O 由多 Reactor 原生事件轮询器驱动（**Linux 使用 epoll，macOS 使用 kqueue，Windows 使用 WSAPoll**）。在完全遵循 RFC 6455 规范与 Go 标准库 `net/http` 契约的前提下，实现极高吞吐与极低长尾延迟，绝无牺牲业务功能的跑分妥协。

---

## ⚡ 核心亮点

- 🚀 **百万级并发长连接 (1M Connections)**：单进程轻松维持 1,000,000 条活跃 WebSocket 连接，物理内存占用仅 **~880 MB**（单连接约 880 字节），内存表现超越主流 Go 框架，逼近 C/C++ 实现。
- 🔥 **超高吞吐与低 CPU 开销**：1KB 数据包 Echo 回显达到 **80,000+ TPS**，平均仅消耗 **< 3 个 CPU 核心 (284%)**，能效比（EER > 281）居行业领先水平。
- ⚡ **闪电建连速率**：建立 1,000,000 条 WebSocket 连接仅需 **19.18 秒**（**52,000+ 连接/秒**），建连速度超越 C++ 库 uWebSockets 26% 以上。
- 🧵 **空闲连接 0 协程常驻**：事件驱动模式下，百万空闲连接纯由 Poller 事件唤醒，不占用 Goroutine 栈及运行时调度资源。
- 🛡️ **严格符合业务规范与行业标准**：完整支持 RFC 6455（客户端掩码解密、Ping/Pong/Close 控制帧保活与协商、子协议与 Origin 鉴权）、完整兼容 TLS/HTTPS 与标准 `http.Handler`。

---

## 📊 百万长连接权威压测数据 (1 Million Connections)

基于业内公认的 [go-websocket-benchmark](https://github.com/lesismal/go-websocket-benchmark) 在 **100 万长连接** 场景下测试，同台对比纯 Go 框架 `nbio` 与 C++/Go 包装库 `uWebSockets (uws_events)`：

### 1. 消息回显吞吐与延迟 (100 万连接, 10,000 并发, 1KB Payload, 200 万次请求响应)

| 框架 | 吞吐量 (TPS) | 能效比 (EER) | 平均延迟 | TP90 延迟 | TP99 延迟 | CPU 平均 | 内存平均 (MEM Avg) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **fnet** | **80,080** | **281.05** | **124.79ms** | **473.56ms** | **531.11ms** | **284.93% (全场最低)** | **881.52 MB (Go 语言第一)** |
| *nbio_nonblocking* | 82,030 | 219.87 | 121.83ms | 469.53ms | 528.79ms | 373.09% | 956.44 MB |
| *uws_events* (C++) | 93,369 | 282.23 | 106.93ms | 444.48ms | 516.08ms | 330.82% | 839.49 MB |

### 2. 百万并发建连性能 (1,000,000 连接建立, 2,000 并发)

| 框架 | 建连 TPS | 总耗时 (Used) | 平均建连延迟 | TP90 延迟 |
| :--- | :---: | :---: | :---: | :---: |
| **fnet** | **52,128** | **19.18s** | **38.28ms** | **49ns** |
| *nbio_nonblocking* | 71,058 | 14.07s | 28.11ms | 39ns |
| *uws_events* (C++) | 38,653 | 25.87s | 51.60ms | 42ns |

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

需要 Go 1.26 及以上版本。

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
