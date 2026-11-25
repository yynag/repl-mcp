# repl-mcp

![Go 1.25.5](https://img.shields.io/badge/Go-1.25.5-00ADD8?logo=go&logoColor=white)
![MIT License](https://img.shields.io/badge/License-MIT-yellow.svg)

English | [&#20013;&#25991;](README.zh-CN.md)

repl-mcp is a Go service that provides interactive pseudo-terminal sessions over MCP. An AI can start a shell, Python, SSH, or any other command-line REPL, keep sending input, and read the output.

## Tools

- `create_session`: starts the specified executable with its arguments and allocates a fixed 120-column by 40-row PTY.
- `write`: writes text input to a session; to add a newline, the caller appends `\n` to the end of `input`.
- `read`: reads output from a byte offset and returns the next offset; the output buffer retains at most 1 MiB, and a single read returns at most 64 KiB.
- `close_session`: terminates the session and releases its resources.

Each session runs independently inside the repl-mcp process. Commands and arguments are passed directly to the operating system for execution, without shell string parsing. The service provides no command allowlist or permission isolation, so use it only in trusted environments.

## Installation

### Prebuilt binaries

Prebuilt binaries are published on [GitHub Releases](https://github.com/yynag/repl-mcp/releases) for Linux and macOS. Windows is not supported yet. Release archives follow the naming convention `repl-mcp_{version}_{os}_{arch}.tar.gz`, for example `repl-mcp_1.0.0_darwin_arm64.tar.gz`.

### Build from source

Go 1.25.5 or newer is required. Install the module dependencies before the first build:

```bash
go mod download
go build -trimpath -ldflags '-s -w' -o build/repl-mcp .
```

## Running

Start it from an MCP host over stdio:

```json
{
  "mcpServers": {
    "repl-mcp": {
      "command": "/absolute/path/build/repl-mcp"
    }
  }
}
```

## Testing

Tests and static checks:

```bash
go test -race ./...
go vet ./...
```
