// Package main provides interactive pseudo-terminal session management.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/creack/pty"
)

const (
	terminalCols = 120
	terminalRows = 40
	maxOutput    = 1 << 20
	defaultRead  = 64 << 10
	closeTimeout = 3 * time.Second
)

// Session holds an interactive command and its recent output.
type Session struct {
	mu       sync.Mutex
	writeMu  sync.Mutex
	killOnce sync.Once
	id       string
	cmd      *exec.Cmd
	terminal *os.File
	output   []byte
	base     int64
	endedAt  time.Time
	exitCode int
	done     chan struct{}
	killDone chan struct{}

	// Protected by Session.mu.
	lastActive time.Time

	// Protected by SessionManager.mu.
	closing bool
}

// SessionManager manages pseudo-terminal sessions within the current process.
type SessionManager struct {
	startMu     sync.Mutex
	mu          sync.Mutex
	sessions    map[string]*Session
	idleTimeout time.Duration
	retention   time.Duration
	maxSessions int
	stop        chan struct{}
}

// NewSessionManager creates a session manager.
func NewSessionManager() *SessionManager {
	m := &SessionManager{
		sessions:    make(map[string]*Session),
		idleTimeout: 30 * time.Minute,
		retention:   10 * time.Minute,
		maxSessions: 8,
		stop:        make(chan struct{}),
	}
	go m.reapLoop()
	return m
}

// Create starts the given program in a fixed-size pseudo-terminal.
func (m *SessionManager) Create(command string, args []string, cwd string) (*Session, error) {
	if strings.TrimSpace(command) == "" {
		return nil, errors.New("command must not be empty")
	}
	m.startMu.Lock()
	defer m.startMu.Unlock()
	select {
	case <-m.stop:
		return nil, errors.New("session manager is closed")
	default:
	}
	m.mu.Lock()
	count, limit := len(m.sessions), m.maxSessions
	m.mu.Unlock()
	if count >= limit {
		return nil, fmt.Errorf("session limit reached (%d)", limit)
	}
	cmd := exec.Command(command, args...)
	cmd.Dir = cwd
	terminal, err := startTerminal(cmd)
	if err != nil {
		return nil, fmt.Errorf("failed to start command in pseudo-terminal: %w", err)
	}
	s := &Session{
		id:         newSessionID(),
		cmd:        cmd,
		terminal:   terminal,
		exitCode:   -1,
		done:       make(chan struct{}),
		killDone:   make(chan struct{}),
		lastActive: time.Now(),
	}
	m.mu.Lock()
	m.sessions[s.id] = s
	m.mu.Unlock()
	outputDone := make(chan struct{})
	go s.readOutput(outputDone)
	go s.wait(outputDone)
	return s, nil
}

func startTerminal(cmd *exec.Cmd) (*os.File, error) {
	master, slave, err := pty.Open()
	if err != nil {
		return nil, err
	}
	defer master.Close()
	defer slave.Close()
	if err := pty.Setsize(master, &pty.Winsize{Cols: terminalCols, Rows: terminalRows}); err != nil {
		return nil, err
	}
	// NewFile must receive a descriptor already set to non-blocking mode so that
	// Close can interrupt pending I/O.
	syscall.ForkLock.RLock()
	fd, err := syscall.Dup(int(master.Fd()))
	if err == nil {
		syscall.CloseOnExec(fd)
	}
	syscall.ForkLock.RUnlock()
	if err != nil {
		return nil, err
	}
	if err := syscall.SetNonblock(fd, true); err != nil {
		_ = syscall.Close(fd)
		return nil, err
	}
	terminal := os.NewFile(uintptr(fd), master.Name())
	if err := terminal.SetReadDeadline(time.Time{}); err != nil {
		_ = terminal.Close()
		return nil, err
	}
	cmd.Stdin, cmd.Stdout, cmd.Stderr = slave, slave, slave
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true, Setctty: true}
	if err := cmd.Start(); err != nil {
		_ = terminal.Close()
		return nil, err
	}
	return terminal, nil
}

// Write writes the input to the pseudo-terminal as-is.
func (m *SessionManager) Write(id, input string) (int, error) {
	s, err := m.get(id)
	if err != nil {
		return 0, err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	select {
	case <-s.done:
		return 0, errors.New("session has ended")
	default:
	}
	n, err := io.WriteString(s.terminal, input)
	if n > 0 {
		s.mu.Lock()
		s.lastActive = time.Now()
		s.mu.Unlock()
	}
	if err != nil {
		return n, fmt.Errorf("failed to write to session: %w", err)
	}
	return n, nil
}

// Read reads output from an absolute offset; when the offset falls in the
// discarded range, reading starts at the beginning of the retained buffer.
func (m *SessionManager) Read(id string, offset int64, limit int) (map[string]any, error) {
	s, err := m.get(id)
	if err != nil {
		return nil, err
	}
	if offset < 0 {
		return nil, errors.New("offset must not be negative")
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
	if len(data) > 0 {
		s.lastActive = time.Now()
	}
	return map[string]any{
		"session_id":  id,
		"output":      data,
		"next_offset": offset + int64(len(data)),
		"base_offset": s.base,
		"truncated":   requested < s.base,
		"running":     s.endedAt.IsZero(),
		"exit_code":   s.exitCode,
	}, nil
}

// Close terminates and removes the session.
func (m *SessionManager) Close(id string) error {
	m.mu.Lock()
	s := m.sessions[id]
	if s != nil {
		s.closing = true
	}
	m.mu.Unlock()
	if s == nil {
		return fmt.Errorf("session %q not found", id)
	}
	return m.closeSession(s)
}

func (m *SessionManager) closeSession(s *Session) error {
	defer func() {
		m.mu.Lock()
		if m.sessions[s.id] == s {
			delete(m.sessions, s.id)
		}
		m.mu.Unlock()
	}()
	select {
	case <-s.done:
		return nil
	default:
	}
	s.kill()
	_ = s.terminal.Close()
	timer := time.NewTimer(closeTimeout)
	defer timer.Stop()
	select {
	case <-s.done:
		return nil
	case <-timer.C:
		return errors.New("timed out waiting for session process to exit")
	}
}

// CloseAll closes and cleans up all sessions.
func (m *SessionManager) CloseAll() {
	m.startMu.Lock()
	m.mu.Lock()
	select {
	case <-m.stop:
	default:
		close(m.stop)
	}
	sessions := make([]*Session, 0, len(m.sessions))
	for _, s := range m.sessions {
		s.closing = true
		sessions = append(sessions, s)
	}
	m.mu.Unlock()
	m.startMu.Unlock()
	m.closeSessions(sessions)
}

func (m *SessionManager) reapLoop() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-m.stop:
			return
		case now := <-ticker.C:
			m.reap(now)
		}
	}
}

func (m *SessionManager) reap(now time.Time) {
	m.mu.Lock()
	var idle []*Session
	for id, s := range m.sessions {
		s.mu.Lock()
		if !s.endedAt.IsZero() {
			if now.Sub(s.endedAt) >= m.retention {
				delete(m.sessions, id)
			}
		} else if !s.closing && now.Sub(s.lastActive) >= m.idleTimeout {
			s.closing = true
			idle = append(idle, s)
		}
		s.mu.Unlock()
	}
	m.mu.Unlock()
	m.closeSessions(idle)
}

func (m *SessionManager) closeSessions(sessions []*Session) {
	var wg sync.WaitGroup
	for _, s := range sessions {
		wg.Go(func() { _ = m.closeSession(s) })
	}
	wg.Wait()
}

func (m *SessionManager) get(id string) (*Session, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.sessions[id]
	if s == nil {
		return nil, fmt.Errorf("session %q not found", id)
	}
	return s, nil
}

func (s *Session) readOutput(done chan<- struct{}) {
	defer close(done)
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
			return
		}
	}
}

func (s *Session) wait(outputDone <-chan struct{}) {
	err := s.cmd.Wait()
	<-s.kill()
	// Drain the output produced before exit, but do not wait for processes that
	// still hold the slave side open.
	_ = s.terminal.SetReadDeadline(time.Now().Add(250 * time.Millisecond))
	<-outputDone
	_ = s.terminal.Close()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.endedAt = time.Now()
	if err == nil {
		s.exitCode = 0
	} else {
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			s.exitCode = exit.ExitCode()
		}
	}
	close(s.done)
}

func (s *Session) kill() <-chan struct{} {
	s.killOnce.Do(func() {
		sid := s.cmd.Process.Pid
		_ = syscall.Kill(-sid, syscall.SIGKILL)
		go func() {
			defer close(s.killDone)
			killSessionMembers(sid)
		}()
	})
	return s.killDone
}

func killSessionMembers(sid int) {
	switch runtime.GOOS {
	case "linux":
		entries, err := os.ReadDir("/proc")
		if err != nil {
			return
		}
		for _, entry := range entries {
			pid, err := strconv.Atoi(entry.Name())
			if err != nil || pid <= 0 || pid == sid {
				continue
			}
			stat, err := os.ReadFile("/proc/" + entry.Name() + "/stat")
			if err == nil && procStatSID(string(stat)) == sid {
				_ = syscall.Kill(pid, syscall.SIGKILL)
			}
		}
	case "darwin":
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		ps := exec.CommandContext(ctx, "ps", "-axo", "pid=")
		ps.WaitDelay = time.Second
		output, err := ps.Output()
		if err != nil {
			return
		}
		// The syscall package on Linux does not export Getsid, so use the Darwin
		// syscall number in this shared source file.
		const darwinSysGetsid = 310
		for _, field := range strings.Fields(string(output)) {
			pid, err := strconv.Atoi(field)
			if err != nil || pid <= 0 || pid == sid {
				continue
			}
			processSID, _, errno := syscall.Syscall(darwinSysGetsid, uintptr(pid), 0, 0)
			if errno == 0 && processSID == uintptr(sid) {
				_ = syscall.Kill(pid, syscall.SIGKILL)
			}
		}
	}
}

func procStatSID(stat string) int {
	// comm may contain right parentheses; the fields that follow are state,
	// ppid, pgrp and session.
	end := strings.LastIndexByte(stat, ')')
	if end < 0 {
		return 0
	}
	fields := strings.Fields(stat[end+1:])
	if len(fields) < 4 {
		return 0
	}
	sid, err := strconv.Atoi(fields[3])
	if err != nil {
		return 0
	}
	return sid
}

func newSessionID() string {
	var value [8]byte
	if _, err := rand.Read(value[:]); err != nil {
		return fmt.Sprintf("session-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(value[:])
}
