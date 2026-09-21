# Autobahn WebSocket Testsuite for fnet

本套件用于全面验证 `fnet` WebSocket 实现对 **RFC 6455** 和 **RFC 7692** 规范的完全兼容性。

测试覆盖了 `fnet` 的两种运行模式：
1. **Event-Driven 模式（端口 9001）**：Reactor / Poller 事件循环驱动，空闲时 0 goroutine。
2. **Goroutine 模式（端口 9002）**：传统阻塞读取循环模式。

---

## 快速运行

### 方式一：直接运行 Go 原生完整测试套件（推荐，无需 Docker）

我们在 `websocket/autobahn_test.go` 中内置了对照 Autobahn 官方测试用例编写的端到端完整测试矩阵：

```bash
go test -v ./websocket -run TestAutobahn
```

包含的测试章节：
- **Case 1: Framing & Payload Sizes**：0、125、126、127、65535、65536 字节文本和二进制数据回显。
- **Case 2: Ping & Pong**：Ping 载荷回显、非预期 Pong 忽略、Ping 载荷 > 125 字节非法拒绝 (1002)、分片 Ping 非法拒绝 (1002)。
- **Case 3: Reserved Bits & Masking**：客户端未掩码数据帧拒绝 (1002)、未协商压缩时的 RSV1 拒绝 (1002)、RSV2/RSV3 拒绝 (1002)、控制帧 RSV 拒绝 (1002)。
- **Case 4: Reserved Opcodes**：保留非控制操作码 (3..7) 及控制操作码 (11..15) 拒绝 (1002)。
- **Case 5: Fragmentation**：文本/二进制多分片组装、分片间穿插 Ping/Pong 控制帧、无首分片的延续帧拒绝 (1002)、分片未结束时接收新数据帧拒绝 (1002)。
- **Case 6: UTF-8 Handling**：多字节中文及 Emoji 验证、跨分片截断多字节 UTF-8 组装还原、非法字节 (0xFF)、非法延续字节、过长编码、UTF-16 代理对、超界码位拒绝 (1007)。
- **Case 7: Close Handshake**：正常 1000 关闭、空载荷关闭、携带原因关闭、1 字节非法载荷拒绝 (1002)、保留或非法状态码 (0..999, 1005, 1006, 1014, 1015, 2000, 5000+) 拒绝 (1002)、关闭原因含非法 UTF-8 拒绝 (1007)。
- **Case 9: Limits & High Volume**：64KB、256KB、1MB 大数据流回显。
- **Case 12 & 13: Permessage-Deflate Compression**：压缩消息往返、延续帧错误设置 RSV1 拒绝 (1002)。

---

### 方式二：使用官方 Autobahn Testsuite 容器生成完整 HTML 报告

1. 启动测试脚本：
```bash
./autobahn/run.sh
```

或者使用 `docker compose`：
```bash
# 启动测试服务
go run ./autobahn/server.go &
SERVER_PID=$!

# 运行 Autobahn 模糊测试容器
docker compose -f ./autobahn/docker-compose.yml up

# 停止测试服务
kill $SERVER_PID
```

2. 测试完成后，查看生成的报告：
```bash
open ./autobahn/reports/servers/index.html
```
