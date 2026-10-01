# Autobahn WebSocket Testsuite for websocket

用 [Autobahn TestSuite](https://github.com/crossbario/autobahn-testsuite) 的 fuzzingclient 模式验证 `websocket` 对 **RFC 6455** 的兼容性。

`server.go` 是基于 `websocket` 的回显服务器，默认监听 `:9001`，单条消息上限设为 16MB（9.* 用例需要）。
`websocket` 不支持 permessage-deflate（RFC 7692），`fuzzingclient.json` 已排除对应的 12.*、13.* 用例。

容器使用 host 网络连接本机的服务器，需要在 Linux 上运行。

---

## 快速运行

### 方式一：测试脚本

```bash
./websocket/autobahn/run.sh
```

脚本会编译并启动 `server.go`，用 Docker（没有 Docker 时用本机的 `wstest`）运行 fuzzingclient，结束后停止服务器。

### 方式二：docker compose

```bash
# 终端 1：启动测试服务器，测试结束后 Ctrl+C 停止
go run ./websocket/autobahn

# 终端 2：运行 Autobahn 模糊测试容器
docker compose -f websocket/autobahn/docker-compose.yml up
```

---

## 查看报告

用浏览器打开 `websocket/autobahn/reports/servers/index.html`，点击用例可查看收发的帧与关闭过程。

- `OK`、`NON-STRICT`、`INFORMATIONAL` 视为通过。
- `websocket` 在分片消息接收完整后才校验 UTF-8，不是逐帧尽早失败，因此 6.4.* 预期为 `NON-STRICT`。

报告由容器以 root 身份写入，在宿主机上删除 `reports` 目录可能需要 `sudo`。
