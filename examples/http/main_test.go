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
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/linfeip/fnet"
	"github.com/linfeip/fnet/fhttp"

	"github.com/gofiber/fiber/v2"
)

// conns is the number of client connections, each with one request in flight at a time. Far more connections than
// cores keep every server busy, as real traffic does; with about one per core the benchmark mostly measures how fast
// the Go scheduler wakes goroutines up.
var conns = flag.Int("conns", 1000, "number of client connections for BenchmarkGET")

// reusePort makes fhttp listen with one SO_REUSEPORT socket per loop, so that the loops accept in parallel.
var reusePort = flag.Bool("reuseport", false, "give each fhttp loop its own SO_REUSEPORT listening socket")

// BenchmarkGET compares fhttp, net/http and fiber serving the same GET /hello over -conns loopback keep-alive
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
		{"fiber", startFiber},
	} {
		b.Run(s.name, func(b *testing.B) {
			idleGoroutines := settleGoroutines()
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
			var peakGoroutines atomic.Int64
			peakGoroutines.Store(int64(runtime.NumGoroutine()))
			samplingDone := make(chan struct{})
			go func() {
				ticker := time.NewTicker(10 * time.Millisecond)
				defer ticker.Stop()
				for {
					select {
					case <-samplingDone:
						return
					case <-ticker.C:
						updatePeak(&peakGoroutines, int64(runtime.NumGoroutine()))
					}
				}
			}()
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
			close(samplingDone)
			b.ReportMetric(float64(idleGoroutines), "idle-goroutines")
			b.ReportMetric(float64(peakGoroutines.Load()), "peak-goroutines")
			b.ReportMetric(float64(runtime.NumGoroutine()), "end-goroutines")
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "req/s")
		})
	}
}

// settleGoroutines waits until the goroutine count stops dropping, so that goroutines left over from the calibration
// runs of the testing framework do not skew the baseline, and returns the settled count.
func settleGoroutines() int64 {
	previous := runtime.NumGoroutine()
	for i := 0; i < 100; i++ {
		time.Sleep(50 * time.Millisecond)
		runtime.GC()
		current := runtime.NumGoroutine()
		if current >= previous {
			return int64(current)
		}
		previous = current
	}
	return int64(previous)
}

// updatePeak stores value in target when it is larger than the current maximum.
func updatePeak(target *atomic.Int64, value int64) {
	for {
		current := target.Load()
		if value <= current || target.CompareAndSwap(current, value) {
			return
		}
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
	srv, err := fhttp.NewServer("127.0.0.1:0", http.HandlerFunc(hello), fhttp.Options{
		Engine: fnet.Options{ReusePort: *reusePort},
	})
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

func startFiber(b *testing.B) (string, func()) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	// The performance options that fit this route: case-sensitive and strict routing (the path has no trailing
	// slash) and no request header-name normalization.
	app := fiber.New(fiber.Config{
		DisableStartupMessage:    true,
		CaseSensitive:            true,
		StrictRouting:            true,
		DisableHeaderNormalizing: true,
	})
	app.Get("/hello", func(ctx *fiber.Ctx) error {
		return ctx.SendString(helloBody)
	})
	go app.Listener(ln)
	return ln.Addr().String(), func() { app.Shutdown() }
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
