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

// TestSessionRoundTrip 验证交互输入、输出读取及会话关闭。
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
	t.Fatal("未在期限内读取到命令输出")
}

// TestSessionOutputLimit 验证输出缓存有界并正确报告被截断的偏移。
func TestSessionOutputLimit(t *testing.T) {
	s := &Session{id: "test", output: []byte("56789"), base: 5}
	m := &SessionManager{sessions: map[string]*Session{s.id: s}}
	result, err := m.Read(s.id, 0, 3)
	if err != nil {
		t.Fatal(err)
	}
	if result["output"] != "567" || result["next_offset"] != int64(8) || result["truncated"] != true {
		t.Fatalf("意外读取结果：%v", result)
	}
}

// TestSessionErrors 验证空命令、无效偏移和不存在的会话会返回错误。
func TestSessionErrors(t *testing.T) {
	m := testSessionManager(t)
	if _, err := m.Create("", nil, ""); err == nil {
		t.Fatal("空命令应报错")
	}
	if _, err := m.Read("missing", 0, 1); err == nil {
		t.Fatal("不存在的会话应报错")
	}
	s := createTestSession(t, m, "/bin/cat")
	if _, err := m.Read(s.id, -1, 1); err == nil {
		t.Fatal("负偏移应报错")
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
		t.Fatal("带有残留进程的会话未结束")
	}
	waitForProcessGone(t, pid)
	result, err := m.Read(s.id, 0, 4096)
	if err != nil || result["running"] != false || result["exit_code"] != 7 {
		t.Fatalf("意外退出状态：%v，%v", result, err)
	}

	m.mu.Lock()
	m.retention = 20 * time.Millisecond
	retention := m.retention
	m.mu.Unlock()
	expires := s.endedAt.Add(retention)
	m.reap(expires.Add(-time.Nanosecond))
	if _, err := m.Read(s.id, 0, 4096); err != nil {
		t.Fatalf("保留期限内无法读取：%v", err)
	}
	m.reap(expires)
	if _, err := m.Read(s.id, 0, 4096); err == nil {
		t.Fatal("保留期限结束后会话未移除")
	}
}

func TestBlockedWriteDoesNotBlockReadOrClose(t *testing.T) {
	m := testSessionManager(t)
	s := createTestSession(t, m, "/bin/sh", "-c", `stty raw -echo || exit 1; while :; do printf 'tick\n'; sleep 0.02; done`)
	t.Cleanup(func() { _ = s.cmd.Process.Kill() })
	var offset int64
	waitForCondition(t, "未读取到初始输出", func() bool {
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
	waitForCondition(t, "写入未因终端背压而阻塞", func() bool {
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
		readResult <- errors.New("写入阻塞期间未读取到新输出")
	}()
	select {
	case err := <-readResult:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("读取被阻塞的写入卡住")
	}
	select {
	case err := <-writeResult:
		t.Fatalf("关闭前写入已返回：%v", err)
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
		t.Fatal("关闭被阻塞的写入卡住")
	}
	select {
	case err := <-writeResult:
		if err == nil {
			t.Fatal("关闭应中断写入")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("关闭未唤醒写入")
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
			t.Fatalf("预期运行中的会话无输出，得到 %v，%v", result, err)
		}
		if !now.Before(deadline) {
			// 直接触发巡检，以免等待生产环境的一分钟周期。
			m.reap(now)
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, err := m.get(s.id); err == nil {
		t.Fatal("连续空读不应阻止空闲会话回收")
	}
	select {
	case <-s.done:
	default:
		t.Fatal("空闲会话移除后进程仍未结束")
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
				waitForCondition(t, "未收到测试输出", func() bool {
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
					t.Fatalf("预期读取到输出，得到 %v，%v", result, err)
				}
			case "write", "empty_write":
				input := ""
				if name == "write" {
					input = "hello\n"
				}
				if n, err := m.Write(s.id, input); err != nil || n != len(input) {
					t.Fatalf("预期写入 %d 字节，得到 %d，%v", len(input), n, err)
				}
			}
			m.reap(now.Add(idleTimeout / 2))
			_, err := m.get(s.id)
			if name == "empty_write" {
				if err == nil {
					t.Fatal("零字节写入不应阻止空闲会话回收")
				}
			} else if err != nil {
				t.Fatalf("有实际读写的会话被提前回收：%v", err)
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
	if _, err := m.Create("/bin/cat", nil, ""); err == nil || !strings.Contains(err.Error(), "上限") {
		t.Fatalf("第九个会话应超出上限：%v", err)
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
	if err := m.Close(s.id); err == nil || !strings.Contains(err.Error(), "退出超时") {
		t.Fatalf("未收到关闭超时错误：%v", err)
	}
	if _, err := terminal.WriteString("test"); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("关闭超时后终端仍未关闭：%v", err)
	}
	if _, err := m.get(s.id); err == nil {
		t.Fatal("关闭超时后会话未移除")
	}
}

func TestProcStatSID(t *testing.T) {
	if got := procStatSID("123 (name with ) and spaces)) S 7 88 543 0"); got != 543 {
		t.Fatalf("SID = %d，预期为 543", got)
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
	waitForCondition(t, "未读取到后台进程 PID", func() bool {
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
		t.Fatalf("后台进程 %d 未进入独立进程组：pgid=%d，%v", pid, pgid, err)
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
			t.Fatalf("无法检查进程 %d：%v", pid, err)
		}
	}
	t.Fatalf("进程 %d 仍存在，预期 syscall.Kill(pid, 0) 返回 ESRCH", pid)
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
