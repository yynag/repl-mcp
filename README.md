# repl-mcp

repl-mcp 是一个通过 MCP 提供交互式伪终端会话的 Go 服务。AI 可以启动 Shell、Python、SSH 或其他命令行 REPL，持续发送输入并读取输出。

## 工具

- `create_session`：启动指定可执行文件及参数，分配固定 120 列、40 行的 PTY。
- `write`：将文本输入写入会话；需要换行时由调用方在 `input` 末尾添加 `\n`。
- `read`：从字节偏移读取输出，返回下一偏移；输出缓冲最多保留 1 MiB，单次最多读取 64 KiB。
- `close_session`：终止会话并清理资源。

每个会话独立运行于 repl-mcp 进程，命令和参数直接传给操作系统执行，不经 shell 字符串解析。服务不提供命令白名单或权限隔离，请仅在可信环境中使用。

## 构建与运行

需要 Go 1.25.5 或更新版本。首次构建前安装模块依赖：

```bash
go mod download
go build -trimpath -o build/repl-mcp .
```

MCP Host 使用 stdio 启动：

```json
{
  "mcpServers": {
    "repl-mcp": {
      "command": "/绝对路径/build/repl-mcp"
    }
  }
}
```

测试与静态检查：

```bash
go test -race ./...
go vet ./...
```
