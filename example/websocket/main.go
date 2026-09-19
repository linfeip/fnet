package main

import (
	"fmt"
	"io"
	"log"
	"net/http"

	"fnet"
	"fnet/websocket"
)

func main() {
	mux := http.NewServeMux()

	// 1. 普通 HTTP 接口
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = io.WriteString(w, `<!DOCTYPE html>
<html>
<head><title>fnet WebSocket Demo</title></head>
<body>
	<h2>fnet + gobwas/ws 高性能 WebSocket 演示</h2>
	<input id="input" type="text" value="Hello fnet!" />
	<button onclick="send()">发送</button>
	<div id="log" style="white-space: pre; margin-top: 10px; border: 1px solid #ccc; padding: 10px;"></div>
	<script>
		const log = document.getElementById("log");
		const ws = new WebSocket("ws://" + location.host + "/ws");
		ws.onopen = () => log.innerText += "[系统] 连接成功\n";
		ws.onmessage = (e) => log.innerText += "[收到服务端 Echo]: " + e.data + "\n";
		ws.onclose = () => log.innerText += "[系统] 连接关闭\n";
		function send() {
			const val = document.getElementById("input").value;
			ws.send(val);
			log.innerText += "[发送]: " + val + "\n";
		}
	</script>
</body>
</html>`)
	})

	// 2. 高性能 WebSocket 接口（极简兼具高可用性）
	upgrader := &websocket.Upgrader{
		// 默认允许所有跨域请求，也可自定义 CheckOrigin
		CheckOrigin: func(r *http.Request) bool { return true },
	}

	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		// 升级 HTTP 连接到 WebSocket
		conn, err := upgrader.Upgrade(w, r)
		if err != nil {
			log.Printf("[WS] 升级失败: %v", err)
			return
		}
		defer conn.Close()

		log.Printf("[WS] 客户端建立连接: %s", conn.RemoteAddr())

		// 使用 conn.Handle 极简处理 Echo 消息（自动响应 Ping/Pong 控制帧）
		err = conn.Handle(func(op websocket.OpCode, msg []byte) error {
			// 原样回显（Echo）
			return conn.WriteMessage(op, msg)
		})

		if err != nil {
			log.Printf("[WS] 客户端断开连接: %v", err)
		}
	})

	addr := ":8081"
	fmt.Printf("==================================================\n")
	fmt.Printf(" fnet + gobwas/ws WebSocket 极简高性能示例已启动\n")
	fmt.Printf(" 访问体验: http://127.0.0.1%s\n", addr)
	fmt.Printf(" WS 接口:  ws://127.0.0.1%s/ws\n", addr)
	fmt.Printf("==================================================\n")

	if err := fnet.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
