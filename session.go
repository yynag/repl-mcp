// Package main 提供交互式伪终端会话管理。
package main

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/creack/pty"
)

const (
	terminalCols = 120
	terminalRows = 40
	maxOutput    = 1 << 20
	defaultRead  = 64 << 10
)

// Session 保存一个交互式命令及其最近输出。
type Session struct {
	mu       sync.Mutex
	id       string
	cmd      *exec.Cmd
	terminal *os.File
	output   []byte
	base     int64
	ended    bool
	exitCode int
	done     chan struct{}
}

// SessionManager 管理当前进程中的伪终端会话。
type SessionManager struct {
	mu       sync.RWMutex
	sessions map[string]*Session
}

// NewSessionManager 创建会话管理器。
func NewSessionManager() *SessionManager {
	return &SessionManager{sessions: make(map[string]*Session)}
}

// Create 启动指定程序并分配固定尺寸伪终端。
func (m *SessionManager) Create(command string, args []string, cwd string) (*Session, error) {
	if strings.TrimSpace(command) == "" {
		return nil, errors.New("command 不能为空")
	}
	cmd := exec.Command(command, args...)
	cmd.Dir = cwd
	terminal, err := pty.StartWithSize(cmd, &pty.Winsize{Cols: terminalCols, Rows: terminalRows})
	if err != nil {
		return nil, fmt.Errorf("启动伪终端命令失败：%w", err)
	}
	s := &Session{id: newSessionID(), cmd: cmd, terminal: terminal, done: make(chan struct{}), exitCode: -1}
	m.mu.Lock()
	m.sessions[s.id] = s
	m.mu.Unlock()
	go s.readOutput()
	return s, nil
}

// Write 将输入原样写入伪终端。
func (m *SessionManager) Write(id, input string) (int, error) {
	s, err := m.get(id)
	if err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ended {
		return 0, errors.New("会话已结束")
	}
	n, err := io.WriteString(s.terminal, input)
	if err != nil {
		return n, fmt.Errorf("写入会话失败：%w", err)
	}
	return n, nil
}

// Read 从绝对输出偏移读取数据，偏移落在已丢弃区间时从保留区起点读取。
func (m *SessionManager) Read(id string, offset int64, limit int) (map[string]any, error) {
	s, err := m.get(id)
	if err != nil {
		return nil, err
	}
	if offset < 0 {
		return nil, errors.New("offset 不能小于 0")
	}
	if limit <= 0 || limit > defaultRead {
		limit = defaultRead
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	requested := offset
	if offset < s.base {
		offset = s.base
	}
	end := s.base + int64(len(s.output))
	if offset > end {
		offset = end
	}
	start := int(offset - s.base)
	stop := min(start+limit, len(s.output))
	data := string(s.output[start:stop])
	return map[string]any{
		"session_id":  id,
		"output":      data,
		"next_offset": offset + int64(len(data)),
		"base_offset": s.base,
		"truncated":   requested < s.base,
		"running":     !s.ended,
		"exit_code":   s.exitCode,
	}, nil
}

// Close 终止并移除会话。
func (m *SessionManager) Close(id string) error {
	s, err := m.get(id)
	if err != nil {
		return err
	}
	s.mu.Lock()
	ended := s.ended
	s.mu.Unlock()
	if !ended {
		_ = s.cmd.Process.Kill()
	}
	select {
	case <-s.done:
	case <-time.After(3 * time.Second):
		_ = s.terminal.Close()
		return fmt.Errorf("等待会话进程退出超时")
	}
	m.mu.Lock()
	delete(m.sessions, id)
	m.mu.Unlock()
	_ = s.terminal.Close()
	return nil
}

// CloseAll 关闭并清理所有会话。
func (m *SessionManager) CloseAll() {
	m.mu.RLock()
	ids := make([]string, 0, len(m.sessions))
	for id := range m.sessions {
		ids = append(ids, id)
	}
	m.mu.RUnlock()
	for _, id := range ids {
		_ = m.Close(id)
	}
}

func (m *SessionManager) get(id string) (*Session, error) {
	m.mu.RLock()
	s := m.sessions[id]
	m.mu.RUnlock()
	if s == nil {
		return nil, fmt.Errorf("未找到会话 %q", id)
	}
	return s, nil
}

func (s *Session) readOutput() {
	buffer := make([]byte, 32*1024)
	for {
		n, err := s.terminal.Read(buffer)
		if n > 0 {
			s.mu.Lock()
			s.output = append(s.output, buffer[:n]...)
			if len(s.output) > maxOutput {
				drop := len(s.output) - maxOutput
				s.output = append(s.output[:0], s.output[drop:]...)
				s.base += int64(drop)
			}
			s.mu.Unlock()
		}
		if err != nil {
			break
		}
	}
	err := s.cmd.Wait()
	s.mu.Lock()
	s.ended = true
	if err == nil {
		s.exitCode = 0
	} else {
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			s.exitCode = exit.ExitCode()
		}
	}
	close(s.done)
	s.mu.Unlock()
}

func newSessionID() string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return fmt.Sprintf("session-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(value[:])
}
