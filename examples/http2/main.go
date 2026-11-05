// http2 is an example HTTP/2 server based on fnet's http2 package, whose Handler is compatible with net/http. By
// default it serves HTTPS (negotiating "h2" via ALPN) with a self-signed certificate generated at startup; with
// -tls=false it serves cleartext HTTP/2 with prior knowledge (h2c).
//
//	cd examples && go run ./http2 -addr :8443
//
//	curl -k https://localhost:8443/hello
//	curl -k -I https://localhost:8443/hello                           # HEAD, served by the GET route
//	curl -k https://localhost:8443/tls                                # TLS version, cipher suite, ALPN protocol
//	curl -k 'https://localhost:8443/query?name=fnet'                  # query parameters
//	curl -k -d 'name=fnet&lang=go' https://localhost:8443/form        # urlencoded form
//	curl -k -H 'Content-Type: application/json' -d '{"name":"fnet"}' https://localhost:8443/json
//	curl -k -T - https://localhost:8443/echo < go.mod                 # streamed request & response body
//	curl -k -N https://localhost:8443/events                          # Server-Sent Events via http.Flusher
//	curl -k -i --data-binary @go.mod https://localhost:8443/trailers  # HTTP/2 response trailers
//
// Cleartext HTTP/2 (h2c with prior knowledge):
//
//	go run ./http2 -addr :8080 -tls=false
//
//	curl --http2-prior-knowledge http://localhost:8080/hello
//	curl --http2-prior-knowledge http://localhost:8080/tls
package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"time"

	"github.com/linfeip/fnet/http2"
)

func main() {
	addr := flag.String("addr", ":8443", "listen address")
	useTLS := flag.Bool("tls", true, "serve HTTPS (ALPN h2) with a self-signed certificate; set false for h2c")
	flag.Parse()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "hello world (proto=%s)\n", r.Proto)
	})
	mux.HandleFunc("GET /hello", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "hello world (proto=%s)\n", r.Proto)
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
			io.WriteString(w, "cleartext HTTP/2 (h2c)\n")
			return
		}
		fmt.Fprintf(w, "%s %s alpn=%s\n",
			tls.VersionName(r.TLS.Version),
			tls.CipherSuiteName(r.TLS.CipherSuite),
			r.TLS.NegotiatedProtocol)
	})
	mux.HandleFunc("/echo", func(w http.ResponseWriter, r *http.Request) {
		io.Copy(w, r.Body)
	})
	mux.HandleFunc("GET /events", streamEvents)
	mux.HandleFunc("POST /trailers", echoWithTrailer)

	var opts http2.Options
	if *useTLS {
		opts.TLSConfig = &tls.Config{Certificates: []tls.Certificate{selfSignedCertificate()}}
	}
	srv, err := http2.NewServer(*addr, mux, opts)
	if err != nil {
		panic(err)
	}
	slog.Info("http2 server listening", "addr", srv.Addr().String(), "https", opts.TLSConfig != nil)
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

// streamEvents sends 5 Server-Sent Events over the HTTP/2 stream, flushing each event immediately and stopping early
// if the client resets the stream or disconnects.
func streamEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for i := 1; i <= 5; i++ {
		if _, err := fmt.Fprintf(w, "id: %d\ndata: tick %d\n\n", i, i); err != nil {
			return
		}
		flusher.Flush()
		if i == 5 {
			return
		}
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
		}
	}
}

// echoWithTrailer streams the request body back while computing its SHA-256 digest, sent in a trailing HEADERS frame
// once the body finishes.
func echoWithTrailer(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Trailer", "X-Content-Sha256")
	hash := sha256.New()
	if _, err := io.Copy(w, io.TeeReader(r.Body, hash)); err != nil {
		return
	}
	w.Header().Set("X-Content-Sha256", hex.EncodeToString(hash.Sum(nil)))
}
