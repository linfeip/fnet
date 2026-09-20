# fnet

[English](README.md) | 中文

基于原生 poller 的高性能 HTTP/HTTPS 与 WebSocket 服务端：**Linux 使用 epoll，macOS 使用 kqueue，Windows 使用 WSAPoll**。TLS 与 HTTP 解析复用 Go 标准库，业务处理函数仍是 `net/http.Handler`。

## 特性

- 与 `net/http` 风格对齐：`ListenAndServe` / `ListenAndServeTLS`，可直接使用 `http.ServeMux`
- 支持 HTTP、HTTPS、keep-alive，以及读写/空闲超时
- 事件驱动 WebSocket：连接空闲时 **0 协程常驻**，帧到齐后再派发
- 握手使用 [gobwas/ws](https://github.com/gobwas/ws)（RFC 6455）
- 支持 Linux、macOS、Windows

## 安装

```bash
go get github.com/linfeip/fnet
```

需要 Go 1.26 及以上。

## HTTP

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
		io.WriteString(w, "hello, fnet\n")
	})

	log.Fatal(fnet.ListenAndServe(":8080", mux))
}
```

HTTPS：

```go
srv := &fnet.Server{
	Addr:    ":8443",
	Handler: mux,
}
log.Fatal(srv.ListenAndServeTLS("cert.pem", "key.pem"))
```

可选字段：`TLSConfig`、`ReadTimeout`、`WriteTimeout`、`IdleTimeout`。

## WebSocket（事件驱动）

设置 `OnMessage` 后，`Upgrade` 会把连接挂到 poller 上，HTTP Handler 应立即返回。空闲连接不占用协程。

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
			log.Printf("open %s", c.RemoteAddr())
		},
		OnMessage: func(c *websocket.Conn, op websocket.OpCode, msg []byte) {
			_ = c.WriteMessage(op, msg)
		},
		OnClose: func(c *websocket.Conn, err error) {
			log.Printf("close: %v", err)
		},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		if _, err := upgrader.Upgrade(w, r); err != nil {
			log.Printf("upgrade: %v", err)
		}
	})

	log.Fatal(fnet.ListenAndServe(":8081", mux))
}
```

未设置 `OnMessage` 时，`Upgrade` 返回阻塞式 `*websocket.Conn`，由调用方自行 `ReadMessage` / `WriteMessage`。也可以用 `UpgradeEvent` 传入 `EventHandler`。

## 示例

```bash
go run ./example                 # HTTP + HTTPS（自签证书）
go run ./example/websocket       # Echo WebSocket，页面 http://127.0.0.1:8081
```

HTTP 示例默认监听 `127.0.0.1:8080` 与 `127.0.0.1:8443`，可用环境变量 `FNET_HTTP_ADDR` / `FNET_HTTPS_ADDR` 覆盖。

## 许可证

MIT，详见 [LICENSE](LICENSE)。
