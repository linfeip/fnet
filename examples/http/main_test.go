package main

import (
	"bufio"
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/linfeip/fnet/fhttp"

	"github.com/valyala/fasthttp"
)

// conns is the number of client connections, each with one request in flight at a time. Far more connections than
// cores keep every server busy, as real traffic does; with about one per core the benchmark mostly measures how fast
// the Go scheduler wakes goroutines up.
var conns = flag.Int("conns", 1000, "BenchmarkGET 的客户端连接数")

// BenchmarkGET compares fhttp, net/http and fasthttp serving the same GET /hello over -conns loopback keep-alive
// connections:
//
//	cd examples
//	go test ./http -run XXX -bench GET -benchtime 3s
//	go test ./http -run XXX -bench GET -conns 100 -cpu 4,8 -count 5   # other connection and core counts, repeated
//
// The servers and the client share the process and its CPUs, so the numbers compare the servers with each other rather
// than measure any of them alone. The client allocates nothing, so allocs/op is the server's.
func BenchmarkGET(b *testing.B) {
	for _, s := range []struct {
		name  string
		start func(b *testing.B) (addr string, stop func())
	}{
		{"fhttp", startFhttp},
		{"nethttp", startNetHTTP},
		{"fasthttp", startFasthttp},
	} {
		b.Run(s.name, func(b *testing.B) {
			addr, stop := s.start(b)
			defer stop()
			clients := make([]*client, *conns)
			for i := range clients {
				c, err := dialClient(addr)
				if err != nil {
					b.Fatal(err)
				}
				defer c.close()
				clients[i] = c
			}
			b.ReportAllocs()
			b.ResetTimer()
			// Every connection sends requests until b.N have been issued in total.
			var issued atomic.Int64
			var wg sync.WaitGroup
			for _, c := range clients {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for issued.Add(1) <= int64(b.N) {
						if err := c.get(); err != nil {
							b.Error(err)
							return
						}
					}
				}()
			}
			wg.Wait()
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "req/s")
		})
	}
}

const helloBody = "hello world"

// hello is the handler of fhttp and net/http. The Content-Type is set as fasthttp does by default, so that no server
// sniffs it.
func hello(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	io.WriteString(w, helloBody)
}

func startFhttp(b *testing.B) (string, func()) {
	srv, err := fhttp.NewServer("127.0.0.1:0", http.HandlerFunc(hello), fhttp.Options{})
	if err != nil {
		b.Fatal(err)
	}
	go srv.Serve()
	return srv.Addr().String(), func() { srv.Close() }
}

func startNetHTTP(b *testing.B) (string, func()) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(hello)}
	go srv.Serve(ln)
	return ln.Addr().String(), func() { srv.Close() }
}

func startFasthttp(b *testing.B) (string, func()) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	srv := &fasthttp.Server{Handler: func(ctx *fasthttp.RequestCtx) {
		ctx.SetContentType("text/plain; charset=utf-8")
		ctx.WriteString(helloBody)
	}}
	go srv.Serve(ln)
	return ln.Addr().String(), func() { srv.Shutdown() }
}

// client is one keep-alive connection sending GET /hello and reading the response without allocating, so that it
// costs the same against every server.
type client struct {
	conn net.Conn
	br   *bufio.Reader
}

var getRequest = []byte("GET /hello HTTP/1.1\r\nHost: localhost\r\n\r\n")

func dialClient(addr string) (*client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &client{conn: conn, br: bufio.NewReader(conn)}, nil
}

// close resets the connection instead of leaving it in TIME_WAIT: every run opens -conns new ones, and repeated runs
// would otherwise use up the ephemeral ports (about 16k on macOS).
func (c *client) close() {
	c.conn.(*net.TCPConn).SetLinger(0)
	c.conn.Close()
}

// get sends one request and reads the whole response, checking the status and the body length.
func (c *client) get() error {
	if _, err := c.conn.Write(getRequest); err != nil {
		return err
	}
	line, err := c.br.ReadSlice('\n')
	if err != nil {
		return err
	}
	if !bytes.HasPrefix(line, []byte("HTTP/1.1 200 ")) {
		return fmt.Errorf("unexpected status line %q", line)
	}
	length := -1
	for {
		if line, err = c.br.ReadSlice('\n'); err != nil {
			return err
		}
		if len(line) <= 2 { // the empty line ending the headers
			break
		}
		const name = "content-length:"
		if len(line) > len(name) && bytes.EqualFold(line[:len(name)], []byte(name)) {
			if length, err = strconv.Atoi(string(bytes.TrimSpace(line[len(name):]))); err != nil {
				return err
			}
		}
	}
	if length != len(helloBody) {
		return errors.New("unexpected Content-Length")
	}
	_, err = c.br.Discard(length)
	return err
}
