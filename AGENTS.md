# AGENTS.md

这是一个GOLANG的网络库项目, 实现高性能的事件驱动网络库, Linux下使用epoll, macOS下使用kqueue, Windows下直接使用标准库简单实现

## 项目场景与性能目标

- WebSocket 业务场景主要以 Echo（回显）为主，设计、实现和验证应优先覆盖真实的 Echo 连接、收发消息、心跳和断连流程。
- 项目需要具备支撑百万级连接的能力；涉及连接管理、事件循环、内存分配、缓冲区、并发控制和资源回收的改动，都必须关注连接规模下的内存占用、CPU 开销、延迟、吞吐和稳定性。
- 在业务资源预算和百万连接目标允许的范围内，可以用合理的额外内存换取更高的吞吐、更低的延迟或更稳定的性能；但必须通过实际业务场景验证收益，并关注 RSS、每连接内存、峰值内存和资源回收，禁止无边界地堆积缓存或以内存换取脱离实际的压测结果。
- 高性能必须服务于实际业务场景。性能优化应基于真实业务需求和可复现的数据，不能为了纯粹的压测结果引入复杂设计、牺牲可维护性，或实现脱离实际使用方式的特例。
- 压测和基准测试应尽量模拟真实连接生命周期、消息大小与分布、读写比例、心跳、慢连接、异常断开等场景；单一的峰值吞吐或连接数不能作为唯一验收标准。
- 绝对不允许下降EchoRate的性能指标

## 并发与资源生命周期

- 锁的使用必须谨慎：明确保护的数据和临界区，保持临界区尽可能短，并确保多把锁的获取顺序一致；持锁期间避免执行可能阻塞的 I/O、回调或其他不可控逻辑，降低死锁风险。
- 遵循“谁获取，谁释放”：锁应在同一清晰的作用域内配对释放，通常在成功获取后及时安排释放（例如适用时使用 `defer`），不要把释放责任隐式扩散到难以追踪的调用链。确需跨作用域移交时，必须明确表达并记录责任归属。
- 池化对象也必须有清晰、可追踪的所有权和生命周期：获取方负责归还，或在所有权显式移交后由接收方负责；获取与归还应在同一清晰的生命周期路径中配对，避免在相隔很远或很深的代码路径中隐式归还，并防止遗漏归还、重复归还或归还后继续使用。

Behavioral guidelines to reduce common LLM coding mistakes. Merge with project-specific instructions as needed.

**Tradeoff:** These guidelines bias toward caution over speed. For trivial tasks, use judgment.

## 语言设定

- 所有产出物（包括 proposals、tasks、specs 等）必须使用**简体中文**撰写。
- 翻译例外：专有名词（API、REST、Token 等）、代码片段、变量名、文件路径和终端命令必须保持英文。

## 命名规范

- 结构体字段（导出与未导出）使用**全名**：由完整单词组成，脱离上下文也能读懂。例如写 `contentLength`、`transferEncodingCount`、`epollFd`、`listenerFd`、`netConnection`、`bufferedReader`，而不是 `clen`、`teCount`、`epfd`、`lnFd`、`nc`、`br`。
- 下列 Go 社区通用缩写可以直接作为字段名或字段名的一部分（如 `outPos`、`remoteAddr`、`NumLoops`）：`fd`、`mu`、`wg`、`err`、`ctx`、`conn`、`addr`、`buf`、`opts`、`srv`、`req`、`msg`、`cond`、`pos`、`num`、`max`/`min`，以及 HTTP、TCP、IO、ID、URL 等标准首字母缩略词。
- 局部变量、函数参数和方法接收者按 Go 惯例使用短名，不受本规则约束。

## Go 代码格式

- 结构体字面量较短时可以写在一行；字段较多或整体较长时应改为多行，每个字段单独一行，并对齐字段名与字段值，便于阅读和后续修改。例如：

```go
&Conn{
 handler:        h,
 maxMessageSize: opts.MaxMessageSize,
 messageTimeout: opts.MessageTimeout,
 idleTimeout:    opts.IdleTimeout,
 executor:       opts.Executor,
 busy:           true,
}
```

## 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:

- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

## 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

## 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:

- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:

- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

## 4. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:

- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:

```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

---

**These guidelines are working if:** fewer unnecessary changes in diffs, fewer rewrites due to overcomplication, and clarifying questions come before implementation rather than after mistakes.

## 5. Git 提交与推送

- 不要执行 `git commit`、`git push` 等提交或推送操作；完成代码修改和验证后，将改动留在工作区，由开发者自行审阅并提交。
