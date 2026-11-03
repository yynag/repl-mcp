package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/mark3labs/mcp-go/mcp"
	mcpserver "github.com/mark3labs/mcp-go/server"
)

type toolDefinition struct {
	name        string
	description string
	schema      string
}

type toolArguments struct {
	Command   string   `json:"command"`
	Args      []string `json:"args"`
	Cwd       string   `json:"cwd"`
	SessionID string   `json:"session_id"`
	Input     string   `json:"input"`
	Offset    int64    `json:"offset"`
	MaxBytes  int      `json:"max_bytes"`
}

// 工具说明使用英文并保持精简。
var toolDefinitions = []toolDefinition{
	{"create_session", "Start a command in a persistent 120x40 PTY session.", `{"type":"object","properties":{"command":{"type":"string","minLength":1},"args":{"type":"array","items":{"type":"string"}},"cwd":{"type":"string"}},"required":["command"],"additionalProperties":false}`},
	{"write", "Write input to a PTY session.", `{"type":"object","properties":{"session_id":{"type":"string","minLength":1},"input":{"type":"string"}},"required":["session_id","input"],"additionalProperties":false}`},
	{"read", "Read PTY output from a byte offset; output retains at most 1 MiB.", `{"type":"object","properties":{"session_id":{"type":"string","minLength":1},"offset":{"type":"integer","minimum":0},"max_bytes":{"type":"integer","minimum":1,"maximum":65536}},"required":["session_id"],"additionalProperties":false}`},
	{"close_session", "Terminate and close a PTY session.", `{"type":"object","properties":{"session_id":{"type":"string","minLength":1}},"required":["session_id"],"additionalProperties":false}`},
}

// NewMCPServer 注册 REPL 会话工具。
func NewMCPServer(sessions *SessionManager) *mcpserver.MCPServer {
	server := mcpserver.NewMCPServer("repl-mcp", "0.1.0", mcpserver.WithRecovery(), mcpserver.WithInputSchemaValidation())
	for _, definition := range toolDefinitions {
		tool := mcp.NewToolWithRawSchema(definition.name, definition.description, json.RawMessage(definition.schema))
		name := definition.name
		server.AddTool(tool, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			result, err := runTool(sessions, name, request.GetArguments())
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			encoded, err := json.MarshalIndent(result, "", "  ")
			if err != nil {
				return mcp.NewToolResultError(fmt.Sprintf("编码工具结果失败：%v", err)), nil
			}
			return mcp.NewToolResultText(string(encoded)), nil
		})
	}
	return server
}

func runTool(sessions *SessionManager, name string, input map[string]any) (any, error) {
	args := toolArguments{}
	encoded, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}
	if err = json.Unmarshal(encoded, &args); err != nil {
		return nil, fmt.Errorf("参数格式无效：%w", err)
	}
	switch name {
	case "create_session":
		session, err := sessions.Create(args.Command, args.Args, args.Cwd)
		if err != nil {
			return nil, err
		}
		return map[string]any{"session_id": session.id, "command": args.Command, "cols": terminalCols, "rows": terminalRows}, nil
	case "write":
		n, err := sessions.Write(args.SessionID, args.Input)
		return map[string]any{"session_id": args.SessionID, "bytes_written": n}, err
	case "read":
		return sessions.Read(args.SessionID, args.Offset, args.MaxBytes)
	case "close_session":
		if err := sessions.Close(args.SessionID); err != nil {
			return nil, err
		}
		return map[string]any{"session_id": args.SessionID, "closed": true}, nil
	default:
		return nil, fmt.Errorf("未知工具：%s", name)
	}
}
