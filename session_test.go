package main

import (
	"errors"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestSessionRoundTrip verifies interactive input, output reading and session close.
func TestSessionRoundTrip(t *testing.T) {
	m := testSessionManager(t)
	s, err := m.Create("/bin/sh", []string{"-c", `read line; printf 'got:%s\n' "$line"`}, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = m.Write(s.id, "hello\n"); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		result, readErr := m.Read(s.id, 0, 4096)
		if readErr != nil {
			t.Fatal(readErr)
		}
		if strings.Contains(result["output"].(string), "got:hello") {
			if err = m.Close(s.id); err != nil {
				t.Fatal(err)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("command output was not read before the deadline")
}

// TestSessionOutputLimit verifies the output buffer is bounded and truncated offsets are reported correctly.
func TestSessionOutputLimit(t *testing.T) {
	s := &Session{id: "test", output: []byte("56789"), base: 5}
	m := &SessionManager{sessions: map[string]*Session{s.id: s}}
	result, err := m.Read(s.id, 0, 3)
	if err != nil {
		t.Fatal(err)
	}
	if result["output"] != "567" || result["next_offset"] != int64(8) || result["truncated"] != true {
		t.Fatalf("unexpected read result: %v", result)
	}
}

// TestSessionErrors verifies that an empty command, an invalid offset and a missing session return errors.
func TestSessionErrors(t *testing.T) {
	m := testSessionManager(t)
	if _, err := m.Create("", nil, ""); err == nil {
		t.Fatal("empty command should return an error")
	}
	if _, err := m.Read("missing", 0, 1); err == nil {
		t.Fatal("missing session should return an error")
	}
	s := createTestSession(t, m, "/bin/cat")
	if _, err := m.Read(s.id, -1, 1); err == nil {
		t.Fatal("negative offset should return an error")
	}
}

func TestCloseKillsBackgroundJob(t *testing.T) {
	m := testSessionManager(t)
	s, pid := startBackgroundJob(t, m)
	if err := m.Close(s.id); err != nil {
		t.Fatal(err)
	}
	waitForProcessGone(t, pid)
}

func TestNaturalExitKillsBackgroundJob(t *testing.T) {
	m := testSessionManager(t)
	s, pid := startBackgroundJob(t, m)
	if _, err := m.Write(s.id, "disown\nexit 7\n"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-s.done:
	case <-time.After(3 * time.Second):
		t.Fatal("session with a leftover process did not end")
	}
	waitForProcessGone(t, pid)
	result, err := m.Read(s.id, 0, 4096)
	if err != nil || result["running"] != false || result["exit_code"] != 7 {
		t.Fatalf("unexpected exit status: %v, %v", result, err)
	}

	m.mu.Lock()
	m.retention = 20 * time.Millisecond
	retention := m.retention
	m.mu.Unlock()
	expires := s.endedAt.Add(retention)
	m.reap(expires.Add(-time.Nanosecond))
	if _, err := m.Read(s.id, 0, 4096); err != nil {
		t.Fatalf("read failed within the retention period: %v", err)
	}
	m.reap(expires)
	if _, err := m.Read(s.id, 0, 4096); err == nil {
		t.Fatal("session was not removed after the retention period ended")
	}
}

func TestBlockedWriteDoesNotBlockReadOrClose(t *testing.T) {
	m := testSessionManager(t)
	s := createTestSession(t, m, "/bin/sh", "-c", `stty raw -echo || exit 1; while :; do printf 'tick\n'; sleep 0.02; done`)
	t.Cleanup(func() { _ = s.cmd.Process.Kill() })
	var offset int64
	waitForCondition(t, "initial output was not read", func() bool {
		result, err := m.Read(s.id, 0, 4096)
		if err != nil {
			t.Fatal(err)
		}
		offset = result["next_offset"].(int64)
		return strings.Contains(result["output"].(string), "tick\n")
	})
	writeResult := make(chan error, 1)
	go func() {
		_, err := m.Write(s.id, strings.Repeat("x", 2*maxOutput))
		writeResult <- err
	}()
	waitForCondition(t, "write did not block due to terminal backpressure", func() bool {
		if s.writeMu.TryLock() {
			s.writeMu.Unlock()
			return false
		}
		return true
	})
	readResult := make(chan error, 1)
	go func() {
		for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
			result, err := m.Read(s.id, offset, 4096)
			if err != nil || strings.Contains(result["output"].(string), "tick\n") {
				readResult <- err
				return
			}
		}
		readResult <- errors.New("no new output was read while the write was blocked")
	}()
	select {
	case err := <-readResult:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("read was stuck behind the blocked write")
	}
	select {
	case err := <-writeResult:
		t.Fatalf("write returned before close: %v", err)
	default:
	}
	closeResult := make(chan error, 1)
	go func() { closeResult <- m.Close(s.id) }()
	select {
	case err := <-closeResult:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("close was stuck behind the blocked write")
	}
	select {
	case err := <-writeResult:
		if err == nil {
			t.Fatal("close should interrupt the write")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("close did not wake the write")
	}
}

func TestEmptyReadPollsDoNotPreventIdleReap(t *testing.T) {
	m := testSessionManager(t)
	const idleTimeout = 50 * time.Millisecond
	m.mu.Lock()
	m.idleTimeout = idleTimeout
	m.mu.Unlock()
	s := createTestSession(t, m, "/bin/cat")
	deadline := time.Now().Add(idleTimeout)
	for {
		now := time.Now()
		result, err := m.Read(s.id, 0, 4096)
		if err != nil || result["output"] != "" || result["running"] != true {
			t.Fatalf("expected no output from a running session, got %v, %v", result, err)
		}
		if !now.Before(deadline) {
			// Trigger reaping directly instead of waiting for the one-minute
			// tick used in production.
			m.reap(now)
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, err := m.get(s.id); err == nil {
		t.Fatal("repeated empty reads should not prevent idle session reaping")
	}
	select {
	case <-s.done:
	default:
		t.Fatal("process is still running after the idle session was removed")
	}
}

func TestSessionIORefreshesIdleTimeout(t *testing.T) {
	const idleTimeout = 50 * time.Millisecond
	for _, name := range []string{"read", "write", "empty_write"} {
		t.Run(name, func(t *testing.T) {
			m := testSessionManager(t)
			m.mu.Lock()
			m.idleTimeout = idleTimeout
			m.mu.Unlock()
			s := createTestSession(t, m, "/bin/sh", "-c", `printf ready; exec /bin/cat`)
			if name == "read" {
				waitForCondition(t, "test output was not received", func() bool {
					s.mu.Lock()
					defer s.mu.Unlock()
					return string(s.output) == "ready"
				})
			}
			now := time.Now()
			s.mu.Lock()
			s.lastActive = now.Add(-idleTimeout)
			s.mu.Unlock()
			switch name {
			case "read":
				result, err := m.Read(s.id, 0, 4096)
				if err != nil || result["output"] != "ready" {
					t.Fatalf("expected output, got %v, %v", result, err)
				}
			case "write", "empty_write":
				input := ""
				if name == "write" {
					input = "hello\n"
				}
				if n, err := m.Write(s.id, input); err != nil || n != len(input) {
					t.Fatalf("expected to write %d bytes, wrote %d: %v", len(input), n, err)
				}
			}
			m.reap(now.Add(idleTimeout / 2))
			_, err := m.get(s.id)
			if name == "empty_write" {
				if err == nil {
					t.Fatal("zero-byte writes should not prevent idle session reaping")
				}
			} else if err != nil {
				t.Fatalf("session with real reads or writes was reaped early: %v", err)
			}
		})
	}
}

func TestCreateSessionLimit(t *testing.T) {
	m := testSessionManager(t)
	first := createTestSession(t, m, "/bin/cat")
	for i := 1; i < 8; i++ {
		createTestSession(t, m, "/bin/cat")
	}
	if _, err := m.Create("/bin/cat", nil, ""); err == nil || !strings.Contains(err.Error(), "session limit reached") {
		t.Fatalf("ninth session should exceed the limit: %v", err)
	}
	if err := m.Close(first.id); err != nil {
		t.Fatal(err)
	}
	createTestSession(t, m, "/bin/cat")
}

func TestCloseTimeoutRemovesSession(t *testing.T) {
	m := testSessionManager(t)
	cmd := exec.Command("/bin/cat")
	terminal, err := startTerminal(cmd)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = terminal.Close()
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	s := &Session{id: "timeout", cmd: cmd, terminal: terminal, done: make(chan struct{}), killDone: make(chan struct{}), lastActive: time.Now()}
	m.mu.Lock()
	m.sessions[s.id] = s
	m.mu.Unlock()
	if err := m.Close(s.id); err == nil || !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("did not get a close timeout error: %v", err)
	}
	if _, err := terminal.WriteString("test"); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("terminal is still open after the close timeout: %v", err)
	}
	if _, err := m.get(s.id); err == nil {
		t.Fatal("session was not removed after the close timeout")
	}
}

func TestProcStatSID(t *testing.T) {
	if got := procStatSID("123 (name with ) and spaces)) S 7 88 543 0"); got != 543 {
		t.Fatalf("SID = %d, want 543", got)
	}
}

func testSessionManager(t *testing.T) *SessionManager {
	t.Helper()
	m := NewSessionManager()
	t.Cleanup(m.CloseAll)
	return m
}

func createTestSession(t *testing.T, m *SessionManager, command string, args ...string) *Session {
	t.Helper()
	s, err := m.Create(command, args, "")
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func startBackgroundJob(t *testing.T, m *SessionManager) (*Session, int) {
	t.Helper()
	s := createTestSession(t, m, "/bin/bash", "--noprofile", "--norc", "-i")
	if _, err := m.Write(s.id, "set +H\nsleep 300 & printf 'child:%s\\n' \"$!\"\n"); err != nil {
		t.Fatal(err)
	}
	var pid int
	waitForCondition(t, "background process PID was not read", func() bool {
		result, err := m.Read(s.id, 0, 4096)
		if err != nil {
			t.Fatal(err)
		}
		lines := strings.Split(result["output"].(string), "\n")
		for _, line := range lines[:len(lines)-1] {
			if value, ok := strings.CutPrefix(strings.TrimSpace(line), "child:"); ok {
				pid, err = strconv.Atoi(value)
				return err == nil && pid > 0
			}
		}
		return false
	})
	t.Cleanup(func() {
		if t.Failed() && syscall.Kill(pid, 0) == nil {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
	})
	if pgid, err := syscall.Getpgid(pid); err != nil || pgid != pid || pgid == s.cmd.Process.Pid {
		t.Fatalf("background process %d did not enter its own process group: pgid=%d, %v", pid, pgid, err)
	}
	return s, pid
}

func waitForProcessGone(t *testing.T, pid int) {
	t.Helper()
	for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		err := syscall.Kill(pid, 0)
		if errors.Is(err, syscall.ESRCH) {
			return
		}
		if err != nil {
			t.Fatalf("cannot inspect process %d: %v", pid, err)
		}
	}
	t.Fatalf("process %d still exists, want syscall.Kill(pid, 0) to return ESRCH", pid)
}

func waitForCondition(t *testing.T, failure string, condition func() bool) {
	t.Helper()
	for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		if condition() {
			return
		}
	}
	t.Fatal(failure)
}
