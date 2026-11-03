package main

import (
	"strings"
	"testing"
	"time"
)

// TestSessionRoundTrip 验证交互输入、输出读取及会话关闭。
func TestSessionRoundTrip(t *testing.T) {
	m := NewSessionManager()
	s, err := m.Create("/bin/sh", []string{"-c", `read line; printf 'got:%s\n' "$line"`}, "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.CloseAll)
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
	m := NewSessionManager()
	m.sessions[s.id] = s
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
	m := NewSessionManager()
	if _, err := m.Create("", nil, ""); err == nil {
		t.Fatal("空命令应报错")
	}
	if _, err := m.Read("missing", 0, 1); err == nil {
		t.Fatal("不存在的会话应报错")
	}
	s := &Session{id: "test"}
	m.sessions[s.id] = s
	if _, err := m.Read(s.id, -1, 1); err == nil {
		t.Fatal("负偏移应报错")
	}
}
