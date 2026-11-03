// http-gin is an example HTTP server running a Gin router on fhttp.Server. Because *gin.Engine implements
// http.Handler, existing Gin routes, middleware, parameter binding and rendering work unchanged: a request body of up
// to fhttp.Options.MaxBufferedBodyBytes (1MB by default), with Content-Length or chunked, is served on fnet, and a
// larger one (such as a big file upload) is streamed by net/http through the same Gin router. With -tls it serves
// HTTPS; with -std it serves the same router on net/http for comparison.
//
//	cd examples && go run ./http-gin -addr :8080
//
//	curl localhost:8080/hello
//	curl -I localhost:8080/hello                                      # HEAD
//	curl 'localhost:8080/query?name=fnet'                             # query parameters
//	curl -d 'name=fnet&lang=go' localhost:8080/form                   # urlencoded form
//	curl -H 'Content-Type: application/json' -d '{"name":"fnet"}' localhost:8080/json
//	curl -X PUT -d 'v1' localhost:8080/items/1                        # create or replace
//	curl -X PATCH -d ',v2' localhost:8080/items/1                     # append
//	curl localhost:8080/items/1
//	curl -X DELETE localhost:8080/items/1
//	curl -i -X OPTIONS localhost:8080/items/1                         # the allowed methods
//	curl -T - localhost:8080/echo < go.mod                            # chunked: the length is unknown
//	curl -F note=hi -F file=@go.mod localhost:8080/upload             # multipart, parsed as a whole
//	curl -F file=@big.bin localhost:8080/upload/stream                # multipart, streamed part by part
//	curl -O localhost:8080/files/go.mod                               # download an uploaded file
//
// HTTPS takes the same requests at https://. The certificate is a self-signed one generated at startup, which curl
// accepts with -k:
//
//	go run ./http-gin -addr :8443 -tls
//
//	curl -k https://localhost:8443/tls                                # TLS version, cipher suite, ALPN protocol
//	curl -k -T - https://localhost:8443/echo < go.mod                 # streamed by net/http, in the same TLS session
package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/linfeip/fnet"
	"github.com/linfeip/fnet/fhttp"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	dir := flag.String("dir", filepath.Join(os.TempDir(), "fhttp-gin-uploads"), "directory for uploaded files")
	useTLS := flag.Bool("tls", false, "serve HTTPS, with a self-signed certificate generated at startup")
	useStd := flag.Bool("std", false, "serve with standard library net/http instead of fhttp")
	flag.Parse()
	if err := os.MkdirAll(*dir, 0o755); err != nil {
		panic(err)
	}

	router := newRouter(*dir)
	if *useStd {
		srv := &http.Server{
			Addr:              *addr,
			Handler:           router,
			MaxHeaderBytes:    16 << 10,
			ReadHeaderTimeout: 10 * time.Second,
			IdleTimeout:       120 * time.Second,
		}
		if *useTLS {
			srv.TLSConfig = &tls.Config{Certificates: []tls.Certificate{selfSignedCertificate()}}
			slog.Info("http-gin (net/http) server listening", "addr", *addr, "https", true, "uploads", *dir)
			panic(srv.ListenAndServeTLS("", ""))
		}
		slog.Info("http-gin (net/http) server listening", "addr", *addr, "https", false, "uploads", *dir)
		panic(srv.ListenAndServe())
	}

	opts := fhttp.Options{Engine: fnet.Options{ReusePort: true}}
	if *useTLS {
		opts.TLSConfig = &tls.Config{Certificates: []tls.Certificate{selfSignedCertificate()}}
	}
	srv, err := fhttp.NewServer(*addr, router, opts)
	if err != nil {
		panic(err)
	}
	slog.Info("http-gin server listening", "addr", srv.Addr().String(), "https", opts.TLSConfig != nil, "uploads", *dir)
	panic(srv.Serve())
}

func newRouter(uploadDir string) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())

	hello := func(c *gin.Context) {
		c.String(http.StatusOK, "hello world")
	}
	r.GET("/", hello)
	r.GET("/hello", hello)
	r.HEAD("/hello", hello)

	r.GET("/query", func(c *gin.Context) {
		c.String(http.StatusOK, "hello %s\n", c.Query("name"))
	})
	r.POST("/form", func(c *gin.Context) {
		c.String(http.StatusOK, "name=%s lang=%s\n", c.PostForm("name"), c.PostForm("lang"))
	})
	r.POST("/json", echoJSON)
	r.GET("/tls", func(c *gin.Context) {
		if c.Request.TLS == nil {
			c.String(http.StatusOK, "plain HTTP\n")
			return
		}
		c.String(http.StatusOK, "%s %s alpn=%s\n",
			tls.VersionName(c.Request.TLS.Version),
			tls.CipherSuiteName(c.Request.TLS.CipherSuite),
			c.Request.TLS.NegotiatedProtocol)
	})

	store := &items{data: make(map[string][]byte)}
	r.GET("/items/:id", store.get)
	r.PUT("/items/:id", store.put)
	r.PATCH("/items/:id", store.patch)
	r.DELETE("/items/:id", store.delete)
	r.OPTIONS("/items/:id", func(c *gin.Context) {
		c.Header("Allow", "GET, HEAD, PUT, PATCH, DELETE, OPTIONS")
		c.Status(http.StatusNoContent)
	})

	r.Any("/echo", func(c *gin.Context) {
		io.Copy(c.Writer, c.Request.Body)
	})
	r.POST("/upload", upload(uploadDir))
	r.POST("/upload/stream", streamUpload(uploadDir))
	r.Static("/files", uploadDir)
	return r
}

// selfSignedCertificate generates a certificate for localhost signed with its own key, good for trying HTTPS only:
// clients do not trust it (curl needs -k). A real server loads its certificate with tls.LoadX509KeyPair instead.
func selfSignedCertificate() tls.Certificate {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		panic(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(365 * 24 * time.Hour),
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		panic(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

// echoJSON decodes a JSON object from the body and sends it back.
func echoJSON(c *gin.Context) {
	var v map[string]any
	if err := c.ShouldBindJSON(&v); err != nil {
		c.String(http.StatusBadRequest, "%s\n", err.Error())
		return
	}
	c.JSON(http.StatusOK, gin.H{"received": v})
}

// items is a small in-memory store behind the REST-style routes.
type items struct {
	mu   sync.Mutex
	data map[string][]byte
}

func (s *items) get(c *gin.Context) {
	id := c.Param("id")
	s.mu.Lock()
	value, ok := s.data[id]
	s.mu.Unlock()
	if !ok {
		c.String(http.StatusNotFound, "404 page not found")
		return
	}
	c.Data(http.StatusOK, http.DetectContentType(value), value)
}

// put creates or replaces the item: 201 when it is new, 204 otherwise.
func (s *items) put(c *gin.Context) {
	value, ok := readValue(c)
	if !ok {
		return
	}
	id := c.Param("id")
	s.mu.Lock()
	_, existed := s.data[id]
	s.data[id] = value
	s.mu.Unlock()
	if existed {
		c.Status(http.StatusNoContent)
		return
	}
	c.Status(http.StatusCreated)
}

// patch appends the body to an existing item.
func (s *items) patch(c *gin.Context) {
	value, ok := readValue(c)
	if !ok {
		return
	}
	id := c.Param("id")
	s.mu.Lock()
	old, existed := s.data[id]
	if existed {
		s.data[id] = append(old, value...)
	}
	s.mu.Unlock()
	if !existed {
		c.String(http.StatusNotFound, "404 page not found")
		return
	}
	c.Status(http.StatusNoContent)
}

func (s *items) delete(c *gin.Context) {
	id := c.Param("id")
	s.mu.Lock()
	_, existed := s.data[id]
	delete(s.data, id)
	s.mu.Unlock()
	if !existed {
		c.String(http.StatusNotFound, "404 page not found")
		return
	}
	c.Status(http.StatusNoContent)
}

// readValue reads the whole body, at most 64KB: http.MaxBytesReader replies 413 beyond that.
func readValue(c *gin.Context) ([]byte, bool) {
	value, err := io.ReadAll(http.MaxBytesReader(c.Writer, c.Request.Body, 64<<10))
	if err != nil {
		status := http.StatusBadRequest
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			status = http.StatusRequestEntityTooLarge
		}
		c.String(status, "%s\n", err.Error())
		return nil, false
	}
	return value, true
}

// upload parses the whole form with MultipartForm: file parts beyond 32MB in total go to temporary files, which are
// removed after the request. It is the simplest way, suited to small and medium files.
func upload(dir string) gin.HandlerFunc {
	return func(c *gin.Context) {
		if err := c.Request.ParseMultipartForm(32 << 20); err != nil {
			c.String(http.StatusBadRequest, "%s\n", err.Error())
			return
		}
		for name, values := range c.Request.MultipartForm.Value {
			fmt.Fprintf(c.Writer, "%s=%q\n", name, values)
		}
		for _, headers := range c.Request.MultipartForm.File {
			for _, header := range headers {
				f, err := header.Open()
				if err != nil {
					c.String(http.StatusInternalServerError, "%s\n", err.Error())
					return
				}
				n, err := saveFile(dir, header.Filename, f)
				f.Close()
				if err != nil {
					c.String(http.StatusInternalServerError, "%s\n", err.Error())
					return
				}
				fmt.Fprintf(c.Writer, "saved %s (%d bytes)\n", header.Filename, n)
			}
		}
	}
}

// streamUpload reads the form part by part with MultipartReader and copies every file straight to disk as it arrives,
// so a file of any size costs constant memory.
func streamUpload(dir string) gin.HandlerFunc {
	return func(c *gin.Context) {
		mr, err := c.Request.MultipartReader()
		if err != nil {
			c.String(http.StatusBadRequest, "%s\n", err.Error())
			return
		}
		for {
			part, err := mr.NextPart()
			if err == io.EOF {
				return
			}
			if err != nil {
				c.String(http.StatusBadRequest, "%s\n", err.Error())
				return
			}
			if part.FileName() == "" {
				value, _ := io.ReadAll(io.LimitReader(part, 4<<10))
				fmt.Fprintf(c.Writer, "%s=%q\n", part.FormName(), value)
				continue
			}
			n, err := saveFile(dir, part.FileName(), part)
			if err != nil {
				c.String(http.StatusInternalServerError, "%s\n", err.Error())
				return
			}
			fmt.Fprintf(c.Writer, "saved %s (%d bytes)\n", part.FileName(), n)
		}
	}
}

// saveFile writes r to dir under the base name of filename, so that a client cannot write outside dir.
func saveFile(dir, filename string, r io.Reader) (int64, error) {
	name := filepath.Base(filename)
	if name == "." || name == ".." || name == string(filepath.Separator) {
		return 0, fmt.Errorf("invalid file name %q", filename)
	}
	f, err := os.Create(filepath.Join(dir, name))
	if err != nil {
		return 0, err
	}
	n, err := io.Copy(f, r)
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	return n, err
}
