package fnet

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestVirtualConnReadWrite(t *testing.T) {
	vc := NewVirtualConn(&net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1},
		&net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 2})

	var got []byte
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		buf := make([]byte, 64)
		n, err := vc.Read(buf)
		if err != nil {
			t.Errorf("Read: %v", err)
			return
		}
		got = append([]byte(nil), buf[:n]...)
	}()

	time.Sleep(20 * time.Millisecond)
	vc.FeedInput([]byte("hello"))
	wg.Wait()
	if string(got) != "hello" {
		t.Fatalf("got %q want hello", got)
	}

	n, err := vc.Write([]byte("world"))
	if err != nil || n != 5 {
		t.Fatalf("Write: n=%d err=%v", n, err)
	}
	out := make([]byte, 16)
	n, rem := vc.DrainWrite(out)
	if n != 5 || rem || string(out[:n]) != "world" {
		t.Fatalf("DrainWrite: n=%d rem=%v data=%q", n, rem, out[:n])
	}
}

func TestVirtualConnEOF(t *testing.T) {
	vc := NewVirtualConn(nil, nil)
	vc.FeedInput([]byte("ab"))
	vc.FeedEOF()

	buf := make([]byte, 8)
	n, err := vc.Read(buf)
	if err != nil || string(buf[:n]) != "ab" {
		t.Fatalf("first read: n=%d err=%v data=%q", n, err, buf[:n])
	}
	n, err = vc.Read(buf)
	if err != io.EOF || n != 0 {
		t.Fatalf("second read: n=%d err=%v", n, err)
	}
}

func TestVirtualConnCloseUnblocksRead(t *testing.T) {
	vc := NewVirtualConn(nil, nil)
	done := make(chan error, 1)
	go func() {
		buf := make([]byte, 8)
		_, err := vc.Read(buf)
		done <- err
	}()
	time.Sleep(20 * time.Millisecond)
	_ = vc.Close()
	select {
	case err := <-done:
		if err != io.EOF {
			t.Fatalf("want EOF, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Read did not unblock")
	}
}

func TestHTTPParseOverVirtualConn(t *testing.T) {
	vc := NewVirtualConn(
		&net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 80},
		&net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 12345},
	)

	raw := "GET /hello?x=1 HTTP/1.1\r\nHost: example.com\r\nUser-Agent: test\r\nContent-Length: 0\r\n\r\n"
	go func() {
		time.Sleep(10 * time.Millisecond)
		vc.FeedInput([]byte(raw))
	}()

	req, err := http.ReadRequest(bufio.NewReader(vc))
	if err != nil {
		t.Fatalf("ReadRequest: %v", err)
	}
	if req.Method != http.MethodGet || req.URL.Path != "/hello" {
		t.Fatalf("unexpected request: %s %s", req.Method, req.URL)
	}
	if req.Host != "example.com" {
		t.Fatalf("host=%q", req.Host)
	}

	w := newResponseWriter(vc)
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, "ok")
	_ = w.finish()

	out := make([]byte, 512)
	n, _ := vc.DrainWrite(out)
	resp := string(out[:n])
	if !strings.Contains(resp, "HTTP/1.1 200") || !strings.Contains(resp, "ok") {
		t.Fatalf("bad response:\n%s", resp)
	}
}

func TestServerHTTP(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = io.WriteString(w, "pong")
	})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	srv := &Server{Addr: addr, Handler: mux}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()
	defer func() { _ = srv.Close() }()

	deadline := time.Now().Add(3 * time.Second)
	var resp *http.Response
	for {
		resp, err = http.Get("http://" + addr + "/ping")
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("GET failed: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 || string(body) != "pong" {
		t.Fatalf("status=%d body=%q", resp.StatusCode, body)
	}
}

func TestServerHTTPS(t *testing.T) {
	cert, err := generateTestCertificate()
	if err != nil {
		t.Fatal(err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/secure", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "secure-ok")
	})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	srv := &Server{
		Addr:    addr,
		Handler: mux,
		TLSConfig: &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		},
	}
	go func() { _ = srv.ListenAndServe() }()
	defer func() { _ = srv.Close() }()

	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
		Timeout: 3 * time.Second,
	}

	deadline := time.Now().Add(3 * time.Second)
	var resp *http.Response
	for {
		resp, err = client.Get("https://" + addr + "/secure")
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("HTTPS GET failed: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 || string(body) != "secure-ok" {
		t.Fatalf("status=%d body=%q", resp.StatusCode, body)
	}
}

func TestPollerSmoke(t *testing.T) {
	p, err := NewPoller()
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	events, err := p.Wait(10 * time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if events == nil {
		events = []Event{}
	}
	_ = fmt.Sprintf("%d", len(events))
}

func TestResponseWriterChunked(t *testing.T) {
	var buf bytes.Buffer
	rw := &bufferConn{buf: &buf}
	w := newResponseWriter(rw)
	w.Header().Set("Transfer-Encoding", "chunked")
	w.Header().Set("Content-Type", "text/plain")
	_, _ = w.Write([]byte("hi"))
	_ = w.finish()
	s := buf.String()
	if !strings.Contains(s, "2\r\nhi\r\n") || !strings.Contains(s, "0\r\n\r\n") {
		t.Fatalf("chunked response:\n%s", s)
	}
}

type bufferConn struct {
	buf *bytes.Buffer
}

func (c *bufferConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (c *bufferConn) Write(b []byte) (int, error)      { return c.buf.Write(b) }
func (c *bufferConn) Close() error                     { return nil }
func (c *bufferConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (c *bufferConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (c *bufferConn) SetDeadline(time.Time) error      { return nil }
func (c *bufferConn) SetReadDeadline(time.Time) error  { return nil }
func (c *bufferConn) SetWriteDeadline(time.Time) error { return nil }
