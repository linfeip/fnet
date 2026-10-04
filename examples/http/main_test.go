package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"testing"

	"github.com/linfeip/fnet/fhttp"

	"github.com/valyala/fasthttp"
)

// BenchmarkGET compares fhttp, net/http and fasthttp serving the same GET /hello over loopback keep-alive connections,
// one connection per parallel client goroutine:
//
//	cd examples
//	go test ./http -run XXX -bench GET -benchtime 3s
//	go test ./http -run XXX -bench GET -cpu 4,8 -count 5   # vary the number of connections, repeat
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
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				c, err := dialClient(addr)
				if err != nil {
					b.Error(err)
					return
				}
				defer c.conn.Close()
				for pb.Next() {
					if err := c.get(); err != nil {
						b.Error(err)
						return
					}
				}
			})
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
