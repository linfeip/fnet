package fhttp

import (
	"bufio"
	"bytes"
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
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/linfeip/fnet/internal/units"
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

	t.Run("逐字节到达", func(t *testing.T) {
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
			"PUT /echo HTTP/1.1\r\nHost: a\r\nContent-Length: 3\r\n\r\nxyz")
		for _, want := range []string{"abc", "hello world", "hello world", "xyz"} {
			if resp, body := readResp(t, br); resp.StatusCode != 200 || body != want || resp.Close {
				t.Fatalf("got %d %q close=%v, want %q", resp.StatusCode, body, resp.Close, want)
			}
		}
	})

	client := &http.Client{Transport: &http.Transport{}}
	defer client.CloseIdleConnections()
	base := "http://" + s.Addr().String()

	t.Run("表单", func(t *testing.T) {
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

	t.Run("multipart 上传后删除临时文件", func(t *testing.T) {
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
			t.Fatalf("临时文件 %s 未被删除: %v", name, err)
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
		{"缓冲的请求体", "hello", false},
		{"交给 net/http 的请求体", "hello!", true},
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
				t.Fatalf("第一个响应: %d %q", resp.StatusCode, body)
			}
			if resp, _ := readResp(t, br); resp.StatusCode != http.StatusContinue {
				t.Fatalf("期望 100 Continue, got %d", resp.StatusCode)
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
		if _, err := io.ReadFull(r.Body, make([]byte, units.KB)); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		started <- struct{}{}
		n, err := io.Copy(io.Discard, r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		fmt.Fprint(w, units.KB+n)
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
	s := serve(t, mux, Options{MaxBufferedBodyBytes: units.KB})
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

	t.Run("Handler 在请求体到齐前开始执行", func(t *testing.T) {
		const size = 8 * units.MB
		pr, pw := io.Pipe()
		go func() {
			pw.Write(make([]byte, 64*units.KB))
			select { // a server buffering the body would never start the Handler here
			case <-started:
			case <-time.After(5 * time.Second):
				pw.CloseWithError(errors.New("Handler 没有在请求体到齐前开始执行"))
				return
			}
			pw.Write(make([]byte, size-64*units.KB))
			pw.Close()
		}()
		req, _ := http.NewRequest("POST", base+"/stream", pr)
		req.ContentLength = size
		if resp, body := do(t, req); resp.StatusCode != 200 || body != strconv.Itoa(size) || !resp.Close {
			t.Fatalf("got %d %q close=%v", resp.StatusCode, body, resp.Close)
		}
		// The connection was closed after that request: the next one is served by fnet again.
		if resp, _ := get(t, client, base+"/hello"); resp.StatusCode != 200 || resp.Close {
			t.Fatalf("之后的请求: %d close=%v", resp.StatusCode, resp.Close)
		}
	})

	t.Run("流式 multipart 上传大文件", func(t *testing.T) {
		data := make([]byte, 32*units.MB)
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

	t.Run("先回复之前的请求", func(t *testing.T) {
		body := strings.Repeat("y", 2*units.KB)
		br := dialRaw(t, s, "GET /hello HTTP/1.1\r\nHost: a\r\n\r\n"+
			"POST /echo HTTP/1.1\r\nHost: a\r\nContent-Length: 2048\r\n\r\n"+body)
		if resp, got := readResp(t, br); resp.StatusCode != 200 || got != "hello world" || resp.Close {
			t.Fatalf("第一个响应: %d %q close=%v", resp.StatusCode, got, resp.Close)
		}
		if resp, got := readResp(t, br); resp.StatusCode != 200 || got != body || !resp.Close {
			t.Fatalf("第二个响应: %d %d 字节 close=%v", resp.StatusCode, len(got), resp.Close)
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
	}), Options{MaxBufferedBodyBytes: units.KB})
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- s.Serve() }()
	br := dialRaw(t, s, "POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 1048576\r\n\r\nabc")
	<-reading
	s.Close()
	if err := <-served; !errors.Is(err, http.ErrServerClosed) {
		t.Fatalf("Serve 返回 %v", err)
	}
	if err := <-done; err == nil {
		t.Fatal("服务端关闭后 Handler 应读到错误")
	}
	if _, err := br.ReadByte(); err == nil {
		t.Fatal("期望连接被关闭")
	}
}
