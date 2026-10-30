package fhttp

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"mime/multipart"
	"net"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestBufferedBody covers request bodies up to MaxBufferedBodyBytes, which stay on fnet: the connection is kept alive
// and the body comes from the standard library's reader over the buffered message.
func TestBufferedBody(t *testing.T) {
	mux := testMux()
	mux.HandleFunc("/form", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "%s %s", r.FormValue("a"), r.PostFormValue("b"))
	})
	onDisk := make(chan string, 1)
	mux.HandleFunc("/upload", func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseMultipartForm(0); err != nil { // 0: every file part goes to a temporary file
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		f, _, err := r.FormFile("file")
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		defer f.Close()
		if osf, ok := f.(*os.File); ok {
			onDisk <- osf.Name()
		}
		io.Copy(w, f)
	})
	s := serve(t, mux, Options{})

	t.Run("one byte at a time", func(t *testing.T) {
		c, err := net.Dial("tcp", s.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		c.SetDeadline(time.Now().Add(5 * time.Second))
		for _, b := range []byte("POST /echo HTTP/1.1\r\nHost: a\r\nContent-Length: 11\r\n\r\nhello world") {
			c.Write([]byte{b})
		}
		if resp, body := readResp(t, bufio.NewReader(c)); resp.StatusCode != 200 || body != "hello world" || resp.Close {
			t.Fatalf("got %d %q close=%v", resp.StatusCode, body, resp.Close)
		}
	})

	t.Run("pipelining", func(t *testing.T) {
		// /hello does not read its body: the next request still starts where the body ends.
		br := dialRaw(t, s, "POST /echo HTTP/1.1\r\nHost: a\r\nContent-Length: 3\r\n\r\nabc"+
			"POST /hello HTTP/1.1\r\nHost: a\r\nContent-Length: 5\r\n\r\nGET /"+
			"GET /hello HTTP/1.1\r\nHost: a\r\n\r\n"+
			"POST /echo HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nch\r\n4\r\nunks\r\n0\r\n\r\n"+
			"PUT /echo HTTP/1.1\r\nHost: a\r\nContent-Length: 3\r\n\r\nxyz")
		for _, want := range []string{"abc", "hello world", "hello world", "chunks", "xyz"} {
			if resp, body := readResp(t, br); resp.StatusCode != 200 || body != want || resp.Close {
				t.Fatalf("got %d %q close=%v, want %q", resp.StatusCode, body, resp.Close, want)
			}
		}
	})

	client := &http.Client{Transport: &http.Transport{}}
	defer client.CloseIdleConnections()
	base := "http://" + s.Addr().String()

	t.Run("form", func(t *testing.T) {
		resp, err := client.PostForm(base+"/form?a=1", url.Values{"b": {"2"}})
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if string(body) != "1 2" || resp.Close {
			t.Fatalf("got %q close=%v", body, resp.Close)
		}
	})

	t.Run("temporary file removed after multipart upload", func(t *testing.T) {
		var form bytes.Buffer
		mw := multipart.NewWriter(&form)
		fw, _ := mw.CreateFormFile("file", "a.txt")
		io.WriteString(fw, "file content")
		mw.Close()
		resp, err := client.Post(base+"/upload", mw.FormDataContentType(), &form)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 || string(body) != "file content" || resp.Close {
			t.Fatalf("got %d %q close=%v", resp.StatusCode, body, resp.Close)
		}
		name := <-onDisk
		if _, err := os.Stat(name); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("temporary file %s was not removed: %v", name, err)
		}
	})
}

// TestExpectContinue checks the 100 Continue that a client waits for before sending the body: it follows the response
// to an earlier request, and net/http sends it for a body that is not buffered.
func TestExpectContinue(t *testing.T) {
	s := newTestServer(t, Options{MaxBufferedBodyBytes: 5})
	for _, tt := range []struct {
		name  string
		body  string
		close bool
	}{
		{"buffered body", "hello", false},
		{"body handed to net/http", "hello!", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c, err := net.Dial("tcp", s.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			c.SetDeadline(time.Now().Add(5 * time.Second))
			io.WriteString(c, "GET /hello HTTP/1.1\r\nHost: a\r\n\r\n"+
				"POST /echo HTTP/1.1\r\nHost: a\r\nExpect: 100-continue\r\nContent-Length: "+strconv.Itoa(len(tt.body))+"\r\n\r\n")
			br := bufio.NewReader(c)
			if resp, body := readResp(t, br); resp.StatusCode != 200 || body != "hello world" {
				t.Fatalf("first response: %d %q", resp.StatusCode, body)
			}
			if resp, _ := readResp(t, br); resp.StatusCode != http.StatusContinue {
				t.Fatalf("want 100 Continue, got %d", resp.StatusCode)
			}
			io.WriteString(c, tt.body)
			if resp, got := readResp(t, br); resp.StatusCode != 200 || got != tt.body || resp.Close != tt.close {
				t.Fatalf("got %d %q close=%v", resp.StatusCode, got, resp.Close)
			}
		})
	}

	t.Run("http.Client", func(t *testing.T) {
		client := &http.Client{Transport: &http.Transport{ExpectContinueTimeout: 5 * time.Second}}
		defer client.CloseIdleConnections()
		req, _ := http.NewRequest("POST", "http://"+s.Addr().String()+"/echo", strings.NewReader("hi"))
		req.Header.Set("Expect", "100-continue")
		start := time.Now()
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if string(body) != "hi" || time.Since(start) > 2*time.Second {
			t.Fatalf("got %q after %v", body, time.Since(start))
		}
	})
}

// TestHandoff covers request bodies that are not buffered: net/http serves them, streaming the body to the Handler,
// and closes the connection afterwards, so the client's next request lands on fnet again.
func TestHandoff(t *testing.T) {
	mux := testMux()
	started := make(chan struct{}, 1)
	mux.HandleFunc("/stream", func(w http.ResponseWriter, r *http.Request) {
		if _, err := io.ReadFull(r.Body, make([]byte, 1<<10)); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		started <- struct{}{}
		n, err := io.Copy(io.Discard, r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		fmt.Fprint(w, (1<<10)+n)
	})
	mux.HandleFunc("/sha256", func(w http.ResponseWriter, r *http.Request) {
		mr, err := r.MultipartReader()
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		part, err := mr.NextPart()
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		h := sha256.New()
		io.Copy(h, part)
		fmt.Fprintf(w, "%s %x", part.FileName(), h.Sum(nil))
	})
	s := serve(t, mux, Options{MaxBufferedBodyBytes: 1 << 10})
	base := "http://" + s.Addr().String()
	client := &http.Client{Transport: &http.Transport{}}
	defer client.CloseIdleConnections()
	do := func(t *testing.T, req *http.Request) (*http.Response, string) {
		t.Helper()
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		return resp, string(body)
	}

	t.Run("Handler starts before the request body arrives", func(t *testing.T) {
		const size = 8 << 20
		pr, pw := io.Pipe()
		go func() {
			pw.Write(make([]byte, 64<<10))
			select { // a server buffering the body would never start the Handler here
			case <-started:
			case <-time.After(5 * time.Second):
				pw.CloseWithError(errors.New("Handler did not start before the request body arrived"))
				return
			}
			pw.Write(make([]byte, size-64<<10))
			pw.Close()
		}()
		req, _ := http.NewRequest("POST", base+"/stream", pr)
		req.ContentLength = size
		if resp, body := do(t, req); resp.StatusCode != 200 || body != strconv.Itoa(size) || !resp.Close {
			t.Fatalf("got %d %q close=%v", resp.StatusCode, body, resp.Close)
		}
		// The connection was closed after that request: the next one is served by fnet again.
		if resp, _ := get(t, client, base+"/hello"); resp.StatusCode != 200 || resp.Close {
			t.Fatalf("following request: %d close=%v", resp.StatusCode, resp.Close)
		}
	})

	t.Run("streaming multipart upload of a large file", func(t *testing.T) {
		data := make([]byte, 32<<20)
		for i := range data {
			data[i] = byte(rand.Uint32())
		}
		pr, pw := io.Pipe()
		mw := multipart.NewWriter(pw)
		go func() {
			fw, _ := mw.CreateFormFile("file", "big.bin")
			fw.Write(data)
			pw.CloseWithError(mw.Close())
		}()
		req, _ := http.NewRequest("POST", base+"/sha256", pr) // unknown length: chunked
		req.Header.Set("Content-Type", mw.FormDataContentType())
		if resp, body := do(t, req); resp.StatusCode != 200 || body != fmt.Sprintf("big.bin %x", sha256.Sum256(data)) {
			t.Fatalf("got %d %q", resp.StatusCode, body)
		}
	})

	t.Run("reply to the earlier request first", func(t *testing.T) {
		body := strings.Repeat("y", 2<<10)
		br := dialRaw(t, s, "GET /hello HTTP/1.1\r\nHost: a\r\n\r\n"+
			"POST /echo HTTP/1.1\r\nHost: a\r\nContent-Length: 2048\r\n\r\n"+body)
		if resp, got := readResp(t, br); resp.StatusCode != 200 || got != "hello world" || resp.Close {
			t.Fatalf("first response: %d %q close=%v", resp.StatusCode, got, resp.Close)
		}
		if resp, got := readResp(t, br); resp.StatusCode != 200 || got != body || !resp.Close {
			t.Fatalf("second response: %d, %d bytes, close=%v", resp.StatusCode, len(got), resp.Close)
		}
		expectClosed(t, br)
	})
}

// TestHandoffClose checks that closing the server also closes the connections handed to net/http.
func TestHandoffClose(t *testing.T) {
	reading := make(chan struct{})
	done := make(chan error, 1)
	s, err := NewServer("127.0.0.1:0", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(reading)
		_, err := io.Copy(io.Discard, r.Body)
		done <- err
	}), Options{MaxBufferedBodyBytes: 1 << 10})
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- s.Serve() }()
	br := dialRaw(t, s, "POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 1048576\r\n\r\nabc")
	<-reading
	s.Close()
	if err := <-served; !errors.Is(err, http.ErrServerClosed) {
		t.Fatalf("Serve returned %v", err)
	}
	if err := <-done; err == nil {
		t.Fatal("Handler should read an error after the server closes")
	}
	if _, err := br.ReadByte(); err == nil {
		t.Fatal("want the connection to be closed")
	}
}

// TestEventStream checks that a client asking for an event stream (Server-Sent Events) is served by net/http: each event
// arrives as soon as the Handler flushes it, and the Handler learns through the request's context that the client left.
func TestEventStream(t *testing.T) {
	left := make(chan struct{})
	s := serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		rc := http.NewResponseController(w)
		for i := 0; ; i++ {
			fmt.Fprintf(w, "data: %d\n\n", i)
			if err := rc.Flush(); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			select {
			case <-r.Context().Done():
				close(left)
				return
			case <-time.After(10 * time.Millisecond):
			}
		}
	}), Options{})
	c, err := net.Dial("tcp", s.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	c.SetDeadline(time.Now().Add(5 * time.Second))
	fmt.Fprint(c, "GET /events HTTP/1.1\r\nHost: a\r\nAccept: text/event-stream\r\n\r\n")
	br := bufio.NewReader(c)
	resp, err := http.ReadResponse(br, nil)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("got %v, %v", resp, err)
	}
	events := bufio.NewReader(resp.Body)
	for i := range 3 { // the Handler never returns, so these come only if each is sent as it is flushed
		if line, err := events.ReadString('\n'); err != nil || line != fmt.Sprintf("data: %d\n", i) {
			t.Fatalf("event %d: %q %v", i, line, err)
		}
		events.ReadString('\n')
	}
	c.Close()
	select {
	case <-left:
	case <-time.After(5 * time.Second):
		t.Fatal("the Handler was not told that the client left")
	}
}

// TestBufferedRequestContext checks that the fnet path gives a Handler a connection context without allocating a
// separate context per request, and that it is canceled when the client closes the connection.
func TestBufferedRequestContext(t *testing.T) {
	started := make(chan struct{})
	canceled := make(chan struct{})
	s := serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
		close(canceled)
	}), Options{})
	c, err := net.Dial("tcp", s.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(c, "GET / HTTP/1.1\r\nHost: a\r\n\r\n"); err != nil {
		c.Close()
		t.Fatal(err)
	}
	<-started
	c.Close()
	select {
	case <-canceled:
	case <-time.After(5 * time.Second):
		t.Fatal("buffered request context was not canceled")
	}
}

// TestRequestContextValues checks that a request's context holds the values the net/http server puts in it, which
// middleware may type-assert without checking.
func TestRequestContextValues(t *testing.T) {
	s := serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, isServer := r.Context().Value(http.ServerContextKey).(*http.Server)
		fmt.Fprint(w, isServer, r.Context().Value(http.LocalAddrContextKey).(net.Addr))
	}), Options{})
	want := fmt.Sprint(true, s.Addr())
	if _, body := readResp(t, dialRaw(t, s, "GET / HTTP/1.1\r\nHost: a\r\n\r\n")); body != want {
		t.Fatalf("got %q, want %q", body, want)
	}
}

// TestCloseDuringFirstRequest closes the client while the Handler of the connection's first request runs, the Handler
// leaving the request's context alone: onClose then reads what the worker set up for the connection (run with -race).
func TestCloseDuringFirstRequest(t *testing.T) {
	done := make(chan struct{})
	s := serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond) // the client closes meanwhile
		close(done)
	}), Options{})
	c, err := net.Dial("tcp", s.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	io.WriteString(c, "GET / HTTP/1.1\r\nHost: a\r\n\r\n")
	time.Sleep(100 * time.Millisecond) // not synchronized with the Handler, which would hide a race from the detector
	c.Close()
	<-done
}

// TestDerivedRequestContext checks that the contexts a Handler derives from a request's register with the connection's
// context, which cancels them when the connection closes, without starting a goroutine per derived context.
func TestDerivedRequestContext(t *testing.T) {
	c := queueConn(&recordConn{})
	c.state = &connState{connection: c}
	const count = 100
	before := runtime.NumGoroutine()
	var derived []context.Context
	for range count {
		ctx, cancel := context.WithTimeout(c.state, time.Hour)
		defer cancel()
		derived = append(derived, ctx)
	}
	if started := runtime.NumGoroutine() - before; started >= count/2 {
		t.Fatalf("%d goroutines started for %d derived contexts", started, count)
	}
	c.onClose(nil)
	for i, ctx := range derived {
		if ctx.Err() == nil {
			t.Fatalf("derived context %d not canceled when the connection closed", i)
		}
	}
}

func TestRequestCleanup(t *testing.T) {
	for _, method := range []string{http.MethodPost, "CUSTOM"} {
		for _, exit := range []string{"return", "panic", "goexit"} {
			t.Run(method+"/"+exit, func(t *testing.T) {
				var form bytes.Buffer
				writer := multipart.NewWriter(&form)
				part, err := writer.CreateFormFile("file", "a.txt")
				if err != nil {
					t.Fatal(err)
				}
				if _, err := io.WriteString(part, "file content"); err != nil {
					t.Fatal(err)
				}
				if err := writer.Close(); err != nil {
					t.Fatal(err)
				}

				var requestBody io.ReadCloser
				var filename string
				var handlerErr error
				c := queueConn(&recordConn{})
				c.srv.handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					requestBody = r.Body
					if handlerErr = r.ParseMultipartForm(0); handlerErr != nil {
						return
					}
					file, _, err := r.FormFile("file")
					if handlerErr = err; err != nil {
						return
					}
					filename = file.(*os.File).Name()
					file.Close()
					switch exit {
					case "panic":
						panic(http.ErrAbortHandler)
					case "goexit":
						runtime.Goexit()
					}
				})
				data := fmt.Sprintf("%s /upload HTTP/1.1\r\nHost: a\r\nContent-Type: %s\r\nContent-Length: %d\r\n\r\n%s",
					method, writer.FormDataContentType(), form.Len(), form.String())
				if n := c.onData([]byte(data)); n != len(data) {
					t.Fatalf("consumed %d of %d bytes", n, len(data))
				}
				done := make(chan struct{})
				go func() {
					defer close(done)
					c.serve()
				}()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Fatal("request worker did not exit")
				}
				if handlerErr != nil {
					t.Fatal(handlerErr)
				}
				if filename == "" {
					t.Fatal("multipart upload did not create a temporary file")
				}
				defer os.Remove(filename)
				if _, err := os.Stat(filename); !errors.Is(err, os.ErrNotExist) {
					t.Errorf("temporary upload file was not removed: %v", err)
				}
				if _, err := requestBody.Read(make([]byte, 1)); !errors.Is(err, http.ErrBodyReadAfterClose) {
					t.Errorf("request body after handler exit: %v", err)
				}
			})
		}
	}
}
