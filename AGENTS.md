# AGENTS.md

fnet 是 Go 的 HTTP/HTTPS 与 WebSocket 引擎。目标是**单进程同时撑住 100 万条连接**，并且压测走的就是线上 API：`ListenAndServe`、`http.Handler`、`websocket.Upgrader`。

每次改动同时过两关：百万连接里绝大多数空闲时，内存和 goroutine 仍然撑得住；同一份二进制上，真实流量仍然可用。回显或扇出更快、但超时、TLS、保活、半包、慢连接或 RFC 行为没了，算回退。

**压测数字不是验收标准，能不能扛住真实业务才是。**

## 版本控制规则

**不允许自动提交和推送代码。**

- 禁止在未被明确要求时执行 `git commit`、`git push`、`git tag`、`git merge`、`git rebase` 等写操作。
- 完成修改后把改动留在工作区，向用户说明改了什么，由用户自己决定是否提交。
- 只有用户在当次对话中明确说"提交"/"push"/"打标签"时才执行对应命令，且仅执行本次被要求的那一步，不要顺带推送。
- 只读命令（`git status`、`git diff`、`git log`、`git show` 等）不受限制。
- 同样禁止自动创建 PR、自动合并分支或以任何方式把改动发布到远端。

## 业务优先

先回答"哪个真实业务场景因此变好"，再回答"跑分快了多少"。只能回答后一句的改动不做。

- **不写压测专用路径。** 不为固定包长、固定并发、固定 handler、单一 payload 形态做特判；不靠"benchmark 里不会出现"的假设省掉分支。benchmark 跑的必须就是业务跑的那条代码。
- **不拿正确性换吞吐。** 超时、TLS、keep-alive、分块、半包、慢连接、RFC 6455 控制帧、错误返回、优雅关闭，丢掉任意一项都算回退，哪怕 QPS 翻倍。
- **不拿易用性换吞吐。** 对外仍是 `ListenAndServe` + `http.Handler` + `websocket.Upgrader`。不要求业务方为了拿到性能去改写 handler、手动管理缓冲区生命周期、或先读懂 reactor 内部。跨调用留存的 payload 由引擎负责拷贝，不把这份心智负担推给业务。
- **默认值按"不调参也不出事"来定。** 缓冲上限、队列深度、超时、背压阈值的默认值面向普通业务，不面向压测机。无上限增长、静默丢消息、panic 逃逸出 worker，都当 bug。
- **边界场景照样正确。** 空消息、超大帧、畸形请求、客户端半关、handler 阻塞、handler panic、写阻塞、对端不读 —— 压测里几乎不出现，线上每天出现。
- **取舍写出来。** 某项优化确实让某类业务变差（延迟换吞吐、内存换 CPU、首包换稳态），在说明里点名受影响的场景和触发条件，不要只报好消息。

## 容量

空闲连接只挂在 poller 上，并占连接表里的一个槽。它不持有 goroutine，也不持有 worker。读缓冲放在 reactor 上共享。

`conn` 上多一个字段、或每条连接多一个 goroutine、map、常驻缓冲，都先按 1e6 算清字节和 goroutine，再改，并在说明里写出这笔账。

Linux `epoll` 承担百万连接。`kqueue` 与 `WSAPoll` 保持行为正确且能编译。平台差异只留在 `internal/netpoll` 的 poller、socket、writev 实现文件里。

## 热路径

事件循环（`internal/reactor` 的 `Loop`）只做就绪、解析和移交。`ServeHTTP` 与 WebSocket 业务回调进 worker pool；同一连接经 `SubmitConn` 进同一分片，保持 FIFO。

稳定态的 accept、read、write 复用 reactor 缓冲、对象池和 `writev`。

慢客户端、半包请求、阻塞的 handler 只拖住自己的连接，同一 reactor 上的其他 fd 继续收事件。用户 handler 的 panic 留在 worker 里。

要留到本次调用之外的 payload，先拷贝再返回。

## 真实流量

同一进程里这些情况同时成立，而不是各做一条压测专用路径：

- 近乎全部连接空闲，少数在推送或请求（IM、推送、扇出），旁边还有普通 HTTP。
- HTTP/1.1：keep-alive、Read/Write/Idle 超时、分块、慢速头部（上限 `maxHeaderBuffer`）、对端突然断开。
- HTTPS 走 worker 上的 TLS。事件驱动 WebSocket 目前挂不到 TLS 连接上（`Unwrap` 返回 nil，回退为每连接一个 goroutine）。明文压测代表不了这条路径。
- WebSocket 遵守 RFC 6455：掩码、Ping/Pong/Close、Origin、子协议、分片。百万连接用事件驱动，空闲时 0 goroutine。阻塞 `ReadMessage` 留给请求-响应式用法。
- 瞬时大量建连，以及成批断开。
- 业务 handler 是真实业务：会查库、会调下游、会慢、会返回大响应、偶尔会 panic。不假设 handler 是立即返回的回显。

## 改完

动到 poller、连接、池、HTTP 或 WebSocket 时：

1. 点名这次改动服务的真实业务场景；若收益只在压测形态（固定包长、固定并发、回显 handler）下成立，说明线上形态下为什么也成立，否则不做。
2. 点名保住的场景：空闲百万、慢连接、keep-alive、TLS、WebSocket 控制帧，或建连风暴。
3. 若增加每连接状态，或把工作放进 reactor，写明 1e6 下的代价，以及 reactor 为何仍然只做移交。
4. 写明这次改动对业务侧 API、默认值、行为语义的影响；有取舍就写明受影响的业务场景。
5. `go test ./...` 通过。现有测试把「慢连接堵住 reactor」和「半包卡死 reactor」当失败；碰到的场景没有测试就补上。
