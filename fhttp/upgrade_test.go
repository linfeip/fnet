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

// pausedProtocol pauses reading as it takes over the connection, and reports the data it receives.
type pausedProtocol struct {
	connection fnet.Conn
	received   chan string
}

func (p *pausedProtocol) OnData(data []byte) int {
	p.received <- string(data)
	return len(data)
}

func (p *pausedProtocol) OnClose(error) {}

// TestUpgradeAfterFullQueue upgrades a connection whose reading fhttp paused for a full queue (see maxQueuedBytes): that
// pause ends with HTTP, and the one the new protocol makes as it takes over holds until the protocol resumes reading.
func TestUpgradeAfterFullQueue(t *testing.T) {
	protocols := make(chan *pausedProtocol, 1)
	s := serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Upgrade", "paused")
		Upgrade(w, func(c fnet.Conn) Protocol {
			c.PauseRead()
			p := &pausedProtocol{connection: c, received: make(chan string, 1)}
			protocols <- p
			return p
		})
	}), Options{})
	c, err := net.Dial("tcp", s.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(5 * time.Second))
	io.WriteString(c, "POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 65536\r\n\r\n"+strings.Repeat("x", maxQueuedBytes))
	if resp, err := http.ReadResponse(bufio.NewReader(c), nil); err != nil || resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("upgrade: %v %v", resp, err)
	}
	p := <-protocols
	io.WriteString(c, "data")
	select {
	case got := <-p.received:
		t.Fatalf("the protocol paused reading, yet received %q", got)
	case <-time.After(300 * time.Millisecond):
	}
	p.connection.ResumeRead()
	select {
	case got := <-p.received:
		if got != "data" {
			t.Fatalf("received %q after ResumeRead, want %q", got, "data")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("nothing received after ResumeRead")
	}
}

// contextConn is a recordConn that keeps the context in which the engine's callbacks find the connection's state.
type contextConn struct {
	recordConn
	ctx any
}

func (c *contextConn) Context() any       { return c.ctx }
func (c *contextConn) SetContext(ctx any) { c.ctx = ctx }

// writableProtocol is an echoProtocol that counts its OnWritable calls.
type writableProtocol struct {
	echoProtocol
	writable int
}

func (p *writableProtocol) OnWritable() { p.writable++ }

// TestProtocolTakesContext checks that the protocol that took a connection over gets the connection's context with the
// first event after the switch, OnResume (websocket resumes reading once its OnOpen has returned) or OnWritable, before
// any data has come, so that the HTTP state can go, and that it gets OnWritable as a WritableProtocol.
func TestProtocolTakesContext(t *testing.T) {
	var h engineHandler
	for name, event := range map[string]func(fnet.Conn){"OnResume": h.OnResume, "OnWritable": h.OnWritable} {
		cc := &contextConn{}
		hc := queueConn(&cc.recordConn)
		cc.ctx = hc
		event(cc)
		if cc.ctx != any(hc) {
			t.Fatalf("%s took the context from HTTP, which is in charge", name)
		}
		p := &writableProtocol{}
		var protocol Protocol = p
		hc.protocol.Store(&protocol)
		event(cc)
		if cc.ctx != any(p) {
			t.Fatalf("%s did not hand the context to the protocol that took the connection over", name)
		}
		writable := p.writable
		h.OnWritable(cc)
		if p.writable != writable+1 {
			t.Fatalf("after %s, the protocol did not get OnWritable", name)
		}
	}
}

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

// TestUpgradeEndsContext checks that the context of the upgrade request ends with its Handler, as on net/http, also once
// the new protocol has taken the connection over: fhttp's onClose, which would cancel it otherwise, is the protocol's
// from then on.
func TestUpgradeEndsContext(t *testing.T) {
	done := make(chan (<-chan struct{}), 1)
	s := serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		done <- r.Context().Done()
		w.Header().Set("Upgrade", "echo")
		Upgrade(w, func(c fnet.Conn) Protocol { return &echoProtocol{connection: c, closed: make(chan error, 1)} })
	}), Options{})
	br := dialRaw(t, s, "GET / HTTP/1.1\r\nHost: a\r\n\r\n")
	if resp, _ := readResp(t, br); resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("upgrade response %d", resp.StatusCode)
	}
	select {
	case <-<-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the upgrade request's context did not end with its Handler")
	}
}

// TestUpgradeHeaderInjection checks that a Sec-WebSocket-Accept value with a CR or LF in it cannot add fields to the 101.
func TestUpgradeHeaderInjection(t *testing.T) {
	s := serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Upgrade", "websocket")
		w.Header().Set("Connection", "Upgrade")
		w.Header().Set("Sec-WebSocket-Accept", "x\r\nX-Injected: 1")
		Upgrade(w, func(c fnet.Conn) Protocol { return &echoProtocol{connection: c, closed: make(chan error, 1)} })
	}), Options{})
	br := dialRaw(t, s, "GET / HTTP/1.1\r\nHost: a\r\n\r\n")
	if resp, _ := readResp(t, br); resp.StatusCode != http.StatusSwitchingProtocols || resp.Header.Get("X-Injected") != "" {
		t.Fatalf("upgrade response %d %v", resp.StatusCode, resp.Header)
	}
}

// TestUpgradeReleasesHTTPState verifies that HTTP requests are no longer handled after an upgrade: the
// request queue and the shared request state are released once the upgrade Handler finishes, so a large
// number of upgraded connections no longer keep that memory resident.
func TestUpgradeReleasesHTTPState(t *testing.T) {
	upgraded := make(chan *conn, 1)
	mux := http.NewServeMux()
	mux.HandleFunc("/upgrade", func(w http.ResponseWriter, r *http.Request) {
		Upgrade(w, func(c fnet.Conn) Protocol { return &echoProtocol{connection: c, closed: make(chan error, 1)} })
		upgraded <- w.(*response).conn
	})
	s := serve(t, mux, Options{})
	c, err := net.Dial("tcp", s.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(5 * time.Second))
	io.WriteString(c, "GET /upgrade HTTP/1.1\r\nHost: a\r\n\r\n")
	if resp, _ := readResp(t, bufio.NewReader(c)); resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("upgrade response %d", resp.StatusCode)
	}
	select {
	case hc := <-upgraded:
		hc.mu.Lock()
		queue, state := hc.queue, hc.state
		hc.mu.Unlock()
		if queue != nil || state != nil {
			t.Fatalf("queue(cap=%d) and state=%v are still retained after the upgrade", cap(queue), state)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("upgrade was not performed")
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
			t.Fatalf("%s: response before the upgrade %q", path, body)
		}
		resp, _ := readResp(t, br)
		if resp.StatusCode != http.StatusSwitchingProtocols || resp.Header.Get("Upgrade") != "echo" {
			t.Fatalf("%s: upgrade response %d %v", path, resp.StatusCode, resp.Header)
		}
		io.WriteString(c, "GET /hello HTTP/1.1\r\n\r\n")
		buf := make([]byte, 23)
		if _, err := io.ReadFull(br, buf); err != nil || string(buf) != "GET /hello HTTP/1.1\r\n\r\n" {
			t.Fatalf("%s: echo %q %v", path, buf, err)
		}
		c.Close()
		select {
		case err := <-closed:
			if err != io.EOF {
				t.Fatalf("%s: OnClose(%v)", path, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("%s: OnClose was not called", path)
		}
	}
}

// TestUpgradeHTTP10 checks that an HTTP/1.0 request is not upgraded (RFC 9110 7.8, 15.2): Upgrade returns
// http.ErrNotSupported and leaves w to the Handler.
func TestUpgradeHTTP10(t *testing.T) {
	s := serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		err := Upgrade(w, func(c fnet.Conn) Protocol { return &echoProtocol{connection: c} })
		io.WriteString(w, err.Error())
	}), Options{})
	br := dialRaw(t, s, "GET / HTTP/1.0\r\nConnection: Upgrade\r\nUpgrade: echo\r\n\r\n")
	if resp, body := readResp(t, br); resp.StatusCode != http.StatusOK || body != http.ErrNotSupported.Error() {
		t.Fatalf("got %d %q, want 200 %q", resp.StatusCode, body, http.ErrNotSupported.Error())
	}
}

// TestUpgradeDropsWrittenResponse checks that an Upgrade that fails after the Handler wrote some of its own response
// sends none of it: the client that did not wait for the 101 gets the 400 alone, not after a header cut short.
func TestUpgradeDropsWrittenResponse(t *testing.T) {
	rc := &recordConn{}
	c := queueConn(rc)
	c.srv.handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/upgrade" {
			return
		}
		w.Header().Set("X-Before", "1")
		io.WriteString(w, "partial") // commits the header
		Upgrade(w, func(fnet.Conn) Protocol { return &echoProtocol{} })
	})
	c.onData([]byte("GET /upgrade HTTP/1.1\r\nHost: a\r\n\r\nGET /next HTTP/1.1\r\nHost: a\r\n\r\n"))
	c.serve()
	if wire := strings.Join(rc.writes, ""); !strings.HasPrefix(wire, "HTTP/1.1 400 ") || strings.Contains(wire, "X-Before") {
		t.Fatalf("got %q, want the 400 alone", wire)
	}
}

// TestUpgradeThenPanic checks a Handler that panics once Upgrade has switched the protocol: the responses that went out
// before the 101 do not go out again, onto the new protocol's stream, and the request's context ends with the Handler.
func TestUpgradeThenPanic(t *testing.T) {
	rc := &recordConn{}
	c := queueConn(rc)
	var done <-chan struct{}
	c.srv.handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/upgrade" {
			io.WriteString(w, "hello")
			return
		}
		done = r.Context().Done()
		Upgrade(w, func(fnet.Conn) Protocol { return &echoProtocol{} })
		panic(http.ErrAbortHandler)
	})
	c.onData([]byte("GET /a HTTP/1.1\r\nHost: a\r\n\r\nGET /upgrade HTTP/1.1\r\nHost: a\r\n\r\n"))
	c.serve()
	if wire := strings.Join(rc.writes, ""); strings.Count(wire, "hello") != 1 || !strings.HasSuffix(wire, "Switching Protocols\r\n\r\n") {
		t.Fatalf("got %q, want the first response once, then the 101", wire)
	}
	select {
	case <-done:
	default:
		t.Fatal("the request's context did not end with its Handler")
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
		t.Fatalf("the request after the upgrade request was executed: calls=%d, out=%q", n, out)
	}
}
