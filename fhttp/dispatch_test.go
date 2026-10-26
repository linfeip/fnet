package fhttp

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"runtime"
	"testing"
	"time"
)

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
				var requestContext *pooledContext
				var filename string
				var handlerErr error
				c := queueConn(&recordConn{})
				c.srv.handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					requestBody = r.Body
					requestContext = r.Context().(*pooledContext)
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
				if buffered, ok := requestBody.(*body); ok {
					if buffered.data != nil {
						t.Error("pooled body still references the request message")
					}
				} else if _, err := requestBody.Read(make([]byte, 1)); !errors.Is(err, http.ErrBodyReadAfterClose) {
					t.Errorf("request body after handler exit: %v", err)
				}
				if requestContext.connection.Load() != nil {
					t.Error("parsed request was not released after handler exit")
				}
			})
		}
	}
}
