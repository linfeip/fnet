// http is an example HTTP server based on fhttp, whose Handler is fully compatible with the standard library. It shows
// the common request types: a request body of up to fhttp.Options.MaxBufferedBodyBytes (1MB by default) is served on
// fnet, a larger or chunked one (such as a big file upload) is streamed by net/http, with the same Handler. With -tls
// it serves HTTPS.
//
//	cd examples && go run ./http -addr :8080
//
//	curl localhost:8080/hello
//	curl -I localhost:8080/hello                                      # HEAD, served by the GET route
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
//	go run ./http -addr :8443 -tls
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
	"encoding/json"
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

	"github.com/linfeip/fnet/fhttp"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	dir := flag.String("dir", filepath.Join(os.TempDir(), "fhttp-uploads"), "directory for uploaded files")
	useTLS := flag.Bool("tls", false, "serve HTTPS, with a self-signed certificate generated at startup")
	flag.Parse()
	if err := os.MkdirAll(*dir, 0o755); err != nil {
		panic(err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /hello", func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "hello world")
	})
	mux.HandleFunc("GET /query", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "hello %s\n", r.URL.Query().Get("name"))
	})
	mux.HandleFunc("POST /form", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "name=%s lang=%s\n", r.FormValue("name"), r.FormValue("lang"))
	})
	mux.HandleFunc("POST /json", echoJSON)
	mux.HandleFunc("GET /tls", func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil {
			io.WriteString(w, "plain HTTP\n")
			return
		}
		fmt.Fprintf(w, "%s %s alpn=%s\n", tls.VersionName(r.TLS.Version), tls.CipherSuiteName(r.TLS.CipherSuite),
			r.TLS.NegotiatedProtocol)
	})

	store := &items{data: make(map[string][]byte)}
	mux.HandleFunc("GET /items/{id}", store.get)
	mux.HandleFunc("PUT /items/{id}", store.put)
	mux.HandleFunc("PATCH /items/{id}", store.patch)
	mux.HandleFunc("DELETE /items/{id}", store.delete)
	mux.HandleFunc("OPTIONS /items/{id}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Allow", "GET, HEAD, PUT, PATCH, DELETE, OPTIONS")
		w.WriteHeader(http.StatusNoContent)
	})

	mux.HandleFunc("/echo", func(w http.ResponseWriter, r *http.Request) { // any method: curl -T sends a PUT
		io.Copy(w, r.Body)
	})
	mux.HandleFunc("POST /upload", upload(*dir))
	mux.HandleFunc("POST /upload/stream", streamUpload(*dir))
	mux.Handle("GET /files/", http.StripPrefix("/files/", http.FileServer(http.Dir(*dir))))

	var opts fhttp.Options
	if *useTLS {
		opts.TLSConfig = &tls.Config{Certificates: []tls.Certificate{selfSignedCertificate()}}
	}
	srv, err := fhttp.NewServer(*addr, mux, opts)
	if err != nil {
		panic(err)
	}
	slog.Info("http server listening", "addr", srv.Addr().String(), "https", opts.TLSConfig != nil, "uploads", *dir)
	panic(srv.Serve())
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
func echoJSON(w http.ResponseWriter, r *http.Request) {
	var v map[string]any
	if err := json.NewDecoder(r.Body).Decode(&v); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"received": v})
}

// items is a small in-memory store behind the REST-style routes.
type items struct {
	mu   sync.Mutex
	data map[string][]byte
}

func (s *items) get(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	value, ok := s.data[r.PathValue("id")]
	s.mu.Unlock()
	if !ok {
		http.NotFound(w, r)
		return
	}
	w.Write(value)
}

// put creates or replaces the item: 201 when it is new, 204 otherwise.
func (s *items) put(w http.ResponseWriter, r *http.Request) {
	value, ok := readValue(w, r)
	if !ok {
		return
	}
	s.mu.Lock()
	_, existed := s.data[r.PathValue("id")]
	s.data[r.PathValue("id")] = value
	s.mu.Unlock()
	if existed {
		w.WriteHeader(http.StatusNoContent)
	} else {
		w.WriteHeader(http.StatusCreated)
	}
}

// patch appends the body to an existing item.
func (s *items) patch(w http.ResponseWriter, r *http.Request) {
	value, ok := readValue(w, r)
	if !ok {
		return
	}
	s.mu.Lock()
	old, existed := s.data[r.PathValue("id")]
	if existed {
		s.data[r.PathValue("id")] = append(old, value...)
	}
	s.mu.Unlock()
	if !existed {
		http.NotFound(w, r)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *items) delete(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	_, existed := s.data[r.PathValue("id")]
	delete(s.data, r.PathValue("id"))
	s.mu.Unlock()
	if !existed {
		http.NotFound(w, r)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// readValue reads the whole body, at most 64KB: http.MaxBytesReader replies 413 beyond that.
func readValue(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	value, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 64<<10))
	if err != nil {
		status := http.StatusBadRequest
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			status = http.StatusRequestEntityTooLarge
		}
		http.Error(w, err.Error(), status)
		return nil, false
	}
	return value, true
}

// upload parses the whole form with ParseMultipartForm: file parts beyond 32MB in total go to temporary files, which
// are removed after the request. It is the simplest way, suited to small and medium files.
func upload(dir string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseMultipartForm(32 << 20); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		for name, values := range r.MultipartForm.Value {
			fmt.Fprintf(w, "%s=%q\n", name, values)
		}
		for _, headers := range r.MultipartForm.File {
			for _, header := range headers {
				f, err := header.Open()
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				n, err := saveFile(dir, header.Filename, f)
				f.Close()
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				fmt.Fprintf(w, "saved %s (%d bytes)\n", header.Filename, n)
			}
		}
	}
}

// streamUpload reads the form part by part with MultipartReader and copies every file straight to disk as it arrives,
// so a file of any size costs constant memory.
func streamUpload(dir string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		mr, err := r.MultipartReader()
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		for {
			part, err := mr.NextPart()
			if err == io.EOF {
				return
			}
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if part.FileName() == "" { // a plain field
				value, _ := io.ReadAll(io.LimitReader(part, 4<<10))
				fmt.Fprintf(w, "%s=%q\n", part.FormName(), value)
				continue
			}
			n, err := saveFile(dir, part.FileName(), part)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			fmt.Fprintf(w, "saved %s (%d bytes)\n", part.FileName(), n)
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
