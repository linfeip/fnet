package main

import (
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"fnet"
)

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "welcome to fnet\n")
	})
	mux.HandleFunc("/hello", func(w http.ResponseWriter, r *http.Request) {
		name := r.URL.Query().Get("name")
		if name == "" {
			name = "world"
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		fmt.Fprintf(w, "hello, %s\n", name)
	})
	mux.HandleFunc("/echo", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write(body)
	})

	httpAddr := envOr("FNET_HTTP_ADDR", "127.0.0.1:8080")
	httpsAddr := envOr("FNET_HTTPS_ADDR", "127.0.0.1:8443")

	go func() {
		log.Printf("HTTP listening on http://%s", httpAddr)
		srv := &fnet.Server{Addr: httpAddr, Handler: mux}
		if err := srv.ListenAndServe(); err != nil {
			log.Fatalf("http server: %v", err)
		}
	}()

	certFile, keyFile, err := ensureExampleCerts()
	if err != nil {
		log.Fatalf("certs: %v", err)
	}

	log.Printf("HTTPS listening on https://%s", httpsAddr)
	httpsSrv := &fnet.Server{
		Addr:    httpsAddr,
		Handler: mux,
		TLSConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
		},
	}
	if err := httpsSrv.ListenAndServeTLS(certFile, keyFile); err != nil {
		log.Fatalf("https server: %v", err)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func ensureExampleCerts() (certFile, keyFile string, err error) {
	dir := filepath.Join(os.TempDir(), "fnet-example")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", "", err
	}
	certFile = filepath.Join(dir, "cert.pem")
	keyFile = filepath.Join(dir, "key.pem")
	if _, err1 := os.Stat(certFile); err1 == nil {
		if _, err2 := os.Stat(keyFile); err2 == nil {
			return certFile, keyFile, nil
		}
	}
	certPEM, keyPEM, err := fnet.GenerateSelfSignedCertPEM()
	if err != nil {
		return "", "", err
	}
	if err := os.WriteFile(certFile, certPEM, 0o644); err != nil {
		return "", "", err
	}
	if err := os.WriteFile(keyFile, keyPEM, 0o600); err != nil {
		return "", "", err
	}
	// Touch mtime so repeated runs reuse fresh-enough certs.
	_ = os.Chtimes(certFile, time.Now(), time.Now())
	return certFile, keyFile, nil
}
