package fhttp

import (
	"bufio"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/linfeip/fnet"
)

// echoProtocol writes the data it receives back unchanged; it is used to test Upgrade.
type echoProtocol struct {
	connection fnet.Conn
	closed     chan error
}

func (p *echoProtocol) OnData(data []byte) int {
	p.connection.Write(data)
	return len(data)
}

func (p *echoProtocol) OnClose(err error) { p.closed <- err }

// wrappedWriter simulates a middleware wrapping the ResponseWriter.
type wrappedWriter struct{ http.ResponseWriter }

func (w wrappedWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func upgradeServer(t *testing.T) (s *Server, closed chan error, helloCalls *atomic.Int32) {
	closed = make(chan error, 1)
	helloCalls = new(atomic.Int32)
	upgrade := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Upgrade", "echo")
		Upgrade(w, func(c fnet.Conn) Protocol { return &echoProtocol{connection: c, closed: closed} })
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/upgrade", upgrade)
	mux.HandleFunc("/wrapped", func(w http.ResponseWriter, r *http.Request) { upgrade(wrappedWriter{w}, r) })
	mux.HandleFunc("/hello", func(w http.ResponseWriter, r *http.Request) {
		helloCalls.Add(1)
		io.WriteString(w, "hello world")
	})
	return serve(t, mux, Options{}), closed, helloCalls
}

// TestUpgradeReleasesHTTPState verifies that HTTP requests are no longer handled after an upgrade: the
// request queue and the cached peer address are released right away, so a large number of upgraded
// connections no longer keep that memory resident.
func TestUpgradeReleasesHTTPState(t *testing.T) {
	type state struct {
		queue      []request
		remoteAddr string
	}
	got := make(chan state, 1)
	mux := http.NewServeMux()
	mux.HandleFunc("/upgrade", func(w http.ResponseWriter, r *http.Request) {
		Upgrade(w, func(c fnet.Conn) Protocol { return &echoProtocol{connection: c, closed: make(chan error, 1)} })
		c := w.(*response).conn
		c.mu.Lock()
		got <- state{c.queue, c.remoteAddr} // remoteAddr is only accessed by the worker, which this is
		c.mu.Unlock()
	})
	s := serve(t, mux, Options{})
	c, err := net.Dial("tcp", s.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	io.WriteString(c, "GET /upgrade HTTP/1.1\r\nHost: a\r\n\r\n")
	select {
	case st := <-got:
		if st.queue != nil || st.remoteAddr != "" {
			t.Fatalf("升级后仍保留 queue(cap=%d) 与 remoteAddr=%q", cap(st.queue), st.remoteAddr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("未执行升级")
	}
}

func TestUpgrade(t *testing.T) {
	s, closed, _ := upgradeServer(t)
	for _, path := range []string{"/upgrade", "/wrapped"} {
		// Requests before the upgrade are handled as usual; data after the upgrade goes to the new
		// protocol.
		c, err := net.Dial("tcp", s.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		c.SetDeadline(time.Now().Add(5 * time.Second))
		io.WriteString(c, "GET /hello HTTP/1.1\r\nHost: a\r\n\r\nGET "+path+" HTTP/1.1\r\nHost: a\r\n\r\n")
		br := bufio.NewReader(c)
		if _, body := readResp(t, br); body != "hello world" {
			t.Fatalf("%s: 升级前的响应 %q", path, body)
		}
		resp, _ := readResp(t, br)
		if resp.StatusCode != http.StatusSwitchingProtocols || resp.Header.Get("Upgrade") != "echo" {
			t.Fatalf("%s: 升级响应 %d %v", path, resp.StatusCode, resp.Header)
		}
		io.WriteString(c, "GET /hello HTTP/1.1\r\n\r\n")
		buf := make([]byte, 23)
		if _, err := io.ReadFull(br, buf); err != nil || string(buf) != "GET /hello HTTP/1.1\r\n\r\n" {
			t.Fatalf("%s: 回显 %q %v", path, buf, err)
		}
		c.Close()
		select {
		case err := <-closed:
			if err != io.EOF {
				t.Fatalf("%s: OnClose(%v)", path, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("%s: 未回调 OnClose", path)
		}
	}
}

func TestUpgradeEarlyRequest(t *testing.T) {
	s, _, helloCalls := upgradeServer(t)
	// The client sent a follow-up request without waiting for the upgrade response: that request must
	// not be executed as HTTP, and the connection is closed. Depending on the timing, the server either
	// replies 400, or closes the connection after switching protocols and finding this data.
	br := dialRaw(t, s, "GET /upgrade HTTP/1.1\r\nHost: a\r\n\r\nGET /hello HTTP/1.1\r\nHost: a\r\n\r\n")
	out, err := io.ReadAll(br)
	if err != nil {
		t.Fatal(err)
	}
	if n := helloCalls.Load(); n != 0 || strings.Contains(string(out), "hello world") {
		t.Fatalf("升级请求之后的请求被执行: calls=%d, out=%q", n, out)
	}
}
