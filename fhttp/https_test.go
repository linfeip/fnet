package fhttp

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/linfeip/fnet/internal/testcert"
)

// ---------------------------------------------------------------------------
// Certificate helpers
// ---------------------------------------------------------------------------

// testCA is a throwaway certificate authority used to mint server and client
// certificates so the TLS tests can verify chains properly instead of leaning
// on InsecureSkipVerify everywhere.
type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pool *x509.CertPool
}

func newTestCA(t *testing.T) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{Organization: []string{"fnet-test-ca"}, CommonName: "fnet test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create CA cert: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse CA cert: %v", err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return &testCA{cert: cert, key: key, pool: pool}
}

// issue mints a leaf certificate signed by the CA. Client certificates get
// ExtKeyUsageClientAuth, server certificates ExtKeyUsageServerAuth.
func (ca *testCA) issue(t *testing.T, cn string, dnsNames []string, ips []net.IP, client bool) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate leaf key: %v", err)
	}
	serial, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		t.Fatalf("serial: %v", err)
	}
	usage := x509.ExtKeyUsageServerAuth
	if client {
		usage = x509.ExtKeyUsageClientAuth
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{Organization: []string{"fnet-test"}, CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{usage},
		DNSNames:     dnsNames,
		IPAddresses:  ips,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("create leaf cert: %v", err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse leaf cert: %v", err)
	}
	return tls.Certificate{
		Certificate: [][]byte{der},
		PrivateKey:  key,
		Leaf:        leaf,
	}
}

// serverCert returns a certificate valid for 127.0.0.1 and localhost.
func (ca *testCA) serverCert(t *testing.T) tls.Certificate {
	t.Helper()
	return ca.issue(t, "localhost", []string{"localhost"}, []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback}, false)
}

// startTLS runs an HTTPS server. idle enables keep-alive: without IdleTimeout
// the server serves one request per TLS connection by design.
func startTLS(t *testing.T, h http.Handler, cfg *tls.Config, idle time.Duration) string {
	t.Helper()
	return startServer(t, &Server{Handler: h, TLSConfig: cfg, IdleTimeout: idle})
}

func clientTLSConfig(ca *testCA, serverName string) *tls.Config {
	return &tls.Config{RootCAs: ca.pool, ServerName: serverName, MinVersion: tls.VersionTLS12}
}

// ---------------------------------------------------------------------------
// Basic HTTPS
// ---------------------------------------------------------------------------

func TestHTTPSBasicRequestResponse(t *testing.T) {
	ca := newTestCA(t)
	addr := startTLS(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
		}
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintf(w, "%s %s body=%s", r.Method, r.URL.Path, body)
	}), &tls.Config{Certificates: []tls.Certificate{ca.serverCert(t)}}, 0)

	client := newClient(t, clientTLSConfig(ca, "localhost"))
	base := "https://" + addr

	t.Run("GET", func(t *testing.T) {
		resp, err := client.Get(base + "/get")
		if err != nil {
			t.Fatalf("GET: %v", err)
		}
		if resp.StatusCode != 200 {
			t.Fatalf("status = %d", resp.StatusCode)
		}
		if got := readBody(t, resp); got != "GET /get body=" {
			t.Fatalf("body = %q", got)
		}
	})

	t.Run("POST", func(t *testing.T) {
		resp, err := client.Post(base+"/post", "text/plain", strings.NewReader("payload"))
		if err != nil {
			t.Fatalf("POST: %v", err)
		}
		if got := readBody(t, resp); got != "POST /post body=payload" {
			t.Fatalf("body = %q", got)
		}
	})
}

// TestHTTPSTLSVersions pins each supported protocol version end to end.
func TestHTTPSTLSVersions(t *testing.T) {
	ca := newTestCA(t)
	versions := map[string]uint16{
		"TLS1.2": tls.VersionTLS12,
		"TLS1.3": tls.VersionTLS13,
	}
	for name, version := range versions {
		t.Run(name, func(t *testing.T) {
			addr := startTLS(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.TLS == nil {
					t.Error("r.TLS is nil on a TLS connection")
					fmt.Fprint(w, "no-tls-state")
					return
				}
				fmt.Fprintf(w, "version=%#04x cipher=%#04x", r.TLS.Version, r.TLS.CipherSuite)
			}), &tls.Config{
				Certificates: []tls.Certificate{ca.serverCert(t)},
				MinVersion:   version,
				MaxVersion:   version,
			}, 0)

			cfg := clientTLSConfig(ca, "localhost")
			cfg.MinVersion, cfg.MaxVersion = version, version
			resp, err := newClient(t, cfg).Get("https://" + addr + "/v")
			if err != nil {
				t.Fatalf("GET: %v", err)
			}
			got := readBody(t, resp)
			if !strings.Contains(got, fmt.Sprintf("version=%#04x", version)) {
				t.Fatalf("handler saw %q, want version %#04x", got, version)
			}
			if strings.Contains(got, "cipher=0x0000") {
				t.Fatalf("cipher suite not reported: %q", got)
			}
		})
	}
}

// TestHTTPSRequestTLSState checks that handlers can inspect the connection:
// r.TLS must carry the negotiated version, SNI name, ALPN protocol and peer
// certificates, exactly as net/http provides.
func TestHTTPSRequestTLSState(t *testing.T) {
	ca := newTestCA(t)
	addr := startTLS(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil {
			t.Error("r.TLS is nil")
			return
		}
		fmt.Fprintf(w, "sni=%s alpn=%s complete=%v resumed=%v",
			r.TLS.ServerName, r.TLS.NegotiatedProtocol, r.TLS.HandshakeComplete, r.TLS.DidResume)
	}), &tls.Config{
		Certificates: []tls.Certificate{ca.serverCert(t)},
		NextProtos:   []string{"http/1.1"},
	}, 0)

	cfg := clientTLSConfig(ca, "localhost")
	cfg.NextProtos = []string{"http/1.1"}
	c := dialRawTLS(t, addr, cfg)
	defer c.close()
	c.write("GET /state HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")

	got := readBody(t, c.readResponse("GET"))
	want := "sni=localhost alpn=http/1.1 complete=true resumed=false"
	if got != want {
		t.Fatalf("TLS state = %q, want %q", got, want)
	}
}

// TestHTTPSSNISelectsCertificate drives GetCertificate with two virtual hosts.
func TestHTTPSSNISelectsCertificate(t *testing.T) {
	ca := newTestCA(t)
	alpha := ca.issue(t, "alpha.test", []string{"alpha.test"}, nil, false)
	beta := ca.issue(t, "beta.test", []string{"beta.test"}, nil, false)

	var mu sync.Mutex
	var seen []string

	addr := startTLS(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "sni=%s", r.TLS.ServerName)
	}), &tls.Config{
		GetCertificate: func(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
			mu.Lock()
			seen = append(seen, hello.ServerName)
			mu.Unlock()
			switch hello.ServerName {
			case "alpha.test":
				return &alpha, nil
			case "beta.test":
				return &beta, nil
			}
			return nil, fmt.Errorf("unknown server name %q", hello.ServerName)
		},
	}, 0)

	for _, host := range []string{"alpha.test", "beta.test"} {
		t.Run(host, func(t *testing.T) {
			c := dialRawTLS(t, addr, clientTLSConfig(ca, host))
			defer c.close()
			// Verify the served leaf really is the one for this name.
			state := c.c.(*tls.Conn).ConnectionState()
			if got := state.PeerCertificates[0].Subject.CommonName; got != host {
				t.Fatalf("served certificate CN = %q, want %q", got, host)
			}
			c.write("GET / HTTP/1.1\r\nHost: " + host + "\r\nConnection: close\r\n\r\n")
			if got := readBody(t, c.readResponse("GET")); got != "sni="+host {
				t.Fatalf("handler saw %q", got)
			}
		})
	}

	t.Run("unknown-sni-rejected", func(t *testing.T) {
		d := &net.Dialer{Timeout: testDialTimeout}
		conn, err := tls.DialWithDialer(d, "tcp", addr, clientTLSConfig(ca, "gamma.test"))
		if err == nil {
			_ = conn.Close()
			t.Fatal("handshake succeeded for an unknown SNI name")
		}
	})

	mu.Lock()
	defer mu.Unlock()
	if len(seen) < 3 {
		t.Fatalf("GetCertificate saw %v, want at least 3 hellos", seen)
	}
}

// TestHTTPSMutualTLS covers client-certificate authentication in both
// directions: a trusted client gets through and its certificate reaches the
// handler; an unauthenticated client is rejected during the handshake.
func TestHTTPSMutualTLS(t *testing.T) {
	ca := newTestCA(t)
	clientCert := ca.issue(t, "test-client", nil, nil, true)

	addr := startTLS(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
			t.Error("handler saw no client certificate")
			fmt.Fprint(w, "anonymous")
			return
		}
		fmt.Fprintf(w, "client=%s chains=%d",
			r.TLS.PeerCertificates[0].Subject.CommonName, len(r.TLS.VerifiedChains))
	}), &tls.Config{
		Certificates: []tls.Certificate{ca.serverCert(t)},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    ca.pool,
	}, 0)

	t.Run("authenticated", func(t *testing.T) {
		cfg := clientTLSConfig(ca, "localhost")
		cfg.Certificates = []tls.Certificate{clientCert}
		c := dialRawTLS(t, addr, cfg)
		defer c.close()
		c.write("GET /mtls HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
		if got := readBody(t, c.readResponse("GET")); got != "client=test-client chains=1" {
			t.Fatalf("body = %q", got)
		}
	})

	t.Run("no-client-cert-rejected", func(t *testing.T) {
		d := &net.Dialer{Timeout: testDialTimeout}
		conn, err := tls.DialWithDialer(d, "tcp", addr, clientTLSConfig(ca, "localhost"))
		if err == nil {
			// TLS 1.3 defers the client-auth alert to the first read.
			_ = conn.SetDeadline(time.Now().Add(testIOTimeout))
			_, _ = io.WriteString(conn, "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
			_, err = io.ReadAll(conn)
			_ = conn.Close()
		}
		if err == nil {
			t.Fatal("server accepted a connection without a client certificate")
		}
	})

	t.Run("untrusted-client-cert-rejected", func(t *testing.T) {
		other := newTestCA(t)
		cfg := clientTLSConfig(ca, "localhost")
		cfg.Certificates = []tls.Certificate{other.issue(t, "rogue", nil, nil, true)}
		d := &net.Dialer{Timeout: testDialTimeout}
		conn, err := tls.DialWithDialer(d, "tcp", addr, cfg)
		if err == nil {
			_ = conn.SetDeadline(time.Now().Add(testIOTimeout))
			_, _ = io.WriteString(conn, "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
			_, err = io.ReadAll(conn)
			_ = conn.Close()
		}
		if err == nil {
			t.Fatal("server accepted a client certificate from an untrusted CA")
		}
	})
}

func TestHTTPSALPNNegotiation(t *testing.T) {
	ca := newTestCA(t)
	addr := startTLS(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "alpn=%q", r.TLS.NegotiatedProtocol)
	}), &tls.Config{
		Certificates: []tls.Certificate{ca.serverCert(t)},
		NextProtos:   []string{"http/1.1"},
	}, 0)

	cases := []struct {
		name         string
		clientProtos []string
		want         string
	}{
		{"http1.1", []string{"http/1.1"}, `alpn="http/1.1"`},
		{"preference-order", []string{"h2", "http/1.1"}, `alpn="http/1.1"`},
		{"no-alpn", nil, `alpn=""`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := clientTLSConfig(ca, "localhost")
			cfg.NextProtos = tc.clientProtos
			c := dialRawTLS(t, addr, cfg)
			defer c.close()
			c.write("GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
			if got := readBody(t, c.readResponse("GET")); got != tc.want {
				t.Fatalf("body = %q, want %q", got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// HTTPS message handling
// ---------------------------------------------------------------------------

// TestHTTPSKeepAlive documents the contract: TLS connections are persistent
// only when IdleTimeout is configured, because the idle deadline is what lets a
// TLS worker goroutine park between requests.
func TestHTTPSKeepAlive(t *testing.T) {
	ca := newTestCA(t)
	cert := ca.serverCert(t)

	t.Run("with-idle-timeout", func(t *testing.T) {
		addr := startTLS(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintf(w, "p=%s", r.URL.Path)
		}), &tls.Config{Certificates: []tls.Certificate{cert}}, 5*time.Second)

		c := dialRawTLS(t, addr, clientTLSConfig(ca, "localhost"))
		defer c.close()
		for i := 0; i < 10; i++ {
			c.write(fmt.Sprintf("GET /r%d HTTP/1.1\r\nHost: localhost\r\n\r\n", i))
			if got, want := readBody(t, c.readResponse("GET")), fmt.Sprintf("p=/r%d", i); got != want {
				t.Fatalf("request %d: body = %q, want %q", i, got, want)
			}
		}
	})

	t.Run("without-idle-timeout-closes", func(t *testing.T) {
		addr := startTLS(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.WriteString(w, "once")
		}), &tls.Config{Certificates: []tls.Certificate{cert}}, 0)

		c := dialRawTLS(t, addr, clientTLSConfig(ca, "localhost"))
		defer c.close()
		c.write("GET /1 HTTP/1.1\r\nHost: localhost\r\n\r\n")
		if got := readBody(t, c.readResponse("GET")); got != "once" {
			t.Fatalf("first response = %q", got)
		}
		c.write("GET /2 HTTP/1.1\r\nHost: localhost\r\n\r\n")
		if _, err := c.readResponseErr("GET"); err == nil {
			t.Fatal("connection stayed open without IdleTimeout")
		}
	})
}

// TestHTTPSPipelined sends two requests inside one TLS record. The read-ahead
// plaintext must stay inside the HTTP reader: pushing it back into the raw
// socket stream would desynchronise the TLS record layer.
func TestHTTPSPipelined(t *testing.T) {
	ca := newTestCA(t)
	addr := startTLS(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "p=%s", r.URL.Path)
	}), &tls.Config{Certificates: []tls.Certificate{ca.serverCert(t)}}, 5*time.Second)

	c := dialRawTLS(t, addr, clientTLSConfig(ca, "localhost"))
	defer c.close()
	c.write("GET /a HTTP/1.1\r\nHost: localhost\r\n\r\n" +
		"GET /b HTTP/1.1\r\nHost: localhost\r\n\r\n" +
		"GET /c HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")

	for _, want := range []string{"p=/a", "p=/b", "p=/c"} {
		if got := readBody(t, c.readResponse("GET")); got != want {
			t.Fatalf("body = %q, want %q", got, want)
		}
	}
}

func TestHTTPSChunkedRequestAndResponse(t *testing.T) {
	ca := newTestCA(t)
	addr := startTLS(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
		}
		w.Header().Set("Transfer-Encoding", "chunked")
		fmt.Fprintf(w, "te=%v body=%s trailer=%s", r.TransferEncoding, body, r.Trailer.Get("X-Sum"))
	}), &tls.Config{Certificates: []tls.Certificate{ca.serverCert(t)}}, 0)

	c := dialRawTLS(t, addr, clientTLSConfig(ca, "localhost"))
	defer c.close()
	c.write("POST /chunked HTTP/1.1\r\nHost: localhost\r\n" +
		"Transfer-Encoding: chunked\r\nTrailer: X-Sum\r\nConnection: close\r\n\r\n" +
		"5;ext\r\nhello\r\n6\r\n world\r\n0\r\nX-Sum: 99\r\n\r\n")

	resp := c.readResponse("POST")
	if len(resp.TransferEncoding) == 0 || resp.TransferEncoding[0] != "chunked" {
		t.Errorf("response TransferEncoding = %v, want chunked", resp.TransferEncoding)
	}
	if got := readBody(t, resp); got != "te=[chunked] body=hello world trailer=99" {
		t.Fatalf("body = %q", got)
	}
}

func TestHTTPSHeadHasNoBody(t *testing.T) {
	ca := newTestCA(t)
	addr := startTLS(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "secret-payload")
	}), &tls.Config{Certificates: []tls.Certificate{ca.serverCert(t)}}, 5*time.Second)

	c := dialRawTLS(t, addr, clientTLSConfig(ca, "localhost"))
	defer c.close()
	c.write("HEAD /res HTTP/1.1\r\nHost: localhost\r\n\r\n")
	resp := c.readResponse("HEAD")
	if got := resp.Header.Get("Content-Length"); got != "14" {
		t.Errorf("Content-Length = %q, want 14", got)
	}
	if body := readBody(t, resp); body != "" {
		t.Errorf("HEAD over TLS carried a body: %q", body)
	}
	// The TLS stream must remain synchronised for the next request.
	c.write("GET /res HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
	if got := readBody(t, c.readResponse("GET")); got != "secret-payload" {
		t.Fatalf("follow-up GET = %q", got)
	}
}

func TestHTTPSLargeBodyRoundTrip(t *testing.T) {
	const size = 4 << 20 // spans many TLS records and the 64 KiB response buffer
	ca := newTestCA(t)
	addr := startTLS(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
			return
		}
		sum := sha256.Sum256(body)
		w.Header().Set("X-Body-Sha256", hex.EncodeToString(sum[:]))
		w.Header().Set("X-Body-Len", fmt.Sprint(len(body)))
		_, _ = w.Write(body) // echo it straight back
	}), &tls.Config{Certificates: []tls.Certificate{ca.serverCert(t)}}, 0)

	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i%253 + 1)
	}
	want := sha256.Sum256(payload)

	client := newClient(t, clientTLSConfig(ca, "localhost"))
	resp, err := client.Post("https://"+addr+"/echo", "application/octet-stream", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if got := resp.Header.Get("X-Body-Sha256"); got != hex.EncodeToString(want[:]) {
		t.Fatalf("server received a corrupted body: sha=%s len=%s", got, resp.Header.Get("X-Body-Len"))
	}
	sum := sha256.New()
	n, err := io.Copy(sum, resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if n != size {
		t.Fatalf("echoed %d bytes, want %d", n, size)
	}
	if !bytes.Equal(sum.Sum(nil), want[:]) {
		t.Fatal("echoed payload does not match the request body")
	}
}

func TestHTTPSStatusAndHeaders(t *testing.T) {
	ca := newTestCA(t)
	addr := startTLS(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Custom", "value")
		w.Header().Add("X-Multi", "a")
		w.Header().Add("X-Multi", "b")
		w.WriteHeader(http.StatusTeapot)
		_, _ = io.WriteString(w, "teapot")
	}), &tls.Config{Certificates: []tls.Certificate{ca.serverCert(t)}}, 0)

	resp, err := newClient(t, clientTLSConfig(ca, "localhost")).Get("https://" + addr + "/s")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	if resp.StatusCode != http.StatusTeapot {
		t.Errorf("status = %d, want 418", resp.StatusCode)
	}
	if got := resp.Header.Get("X-Custom"); got != "value" {
		t.Errorf("X-Custom = %q", got)
	}
	if got := resp.Header.Values("X-Multi"); len(got) != 2 {
		t.Errorf("X-Multi = %v", got)
	}
	if got := readBody(t, resp); got != "teapot" {
		t.Errorf("body = %q", got)
	}
}

func TestHTTPSConcurrentRequests(t *testing.T) {
	ca := newTestCA(t)
	addr := startTLS(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
			return
		}
		w.Header().Set("X-Echo-Path", r.URL.Path)
		_, _ = w.Write(body)
	}), &tls.Config{Certificates: []tls.Certificate{ca.serverCert(t)}}, 5*time.Second)

	client := newClient(t, clientTLSConfig(ca, "localhost"))
	const (
		workers   = 8
		perWorker = 10
	)
	var wg sync.WaitGroup
	errs := make(chan error, workers*perWorker)
	for wk := 0; wk < workers; wk++ {
		wg.Add(1)
		go func(wk int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				path := fmt.Sprintf("/w%d/r%d", wk, i)
				want := fmt.Sprintf("tls-payload-%d-%d", wk, i)
				resp, err := client.Post("https://"+addr+path, "text/plain", strings.NewReader(want))
				if err != nil {
					errs <- fmt.Errorf("%s: %w", path, err)
					return
				}
				got, err := io.ReadAll(resp.Body)
				resp.Body.Close()
				if err != nil {
					errs <- fmt.Errorf("%s: read: %w", path, err)
					return
				}
				if string(got) != want {
					errs <- fmt.Errorf("%s: body = %q, want %q", path, got, want)
					return
				}
				if hp := resp.Header.Get("X-Echo-Path"); hp != path {
					errs <- fmt.Errorf("%s: response crossed with %q", path, hp)
					return
				}
			}
		}(wk)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// ---------------------------------------------------------------------------
// Handshake failures
// ---------------------------------------------------------------------------

func TestHTTPSPlaintextRequestRejected(t *testing.T) {
	ca := newTestCA(t)
	var reached bool
	var mu sync.Mutex
	addr := startTLS(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		reached = true
		mu.Unlock()
	}), &tls.Config{Certificates: []tls.Certificate{ca.serverCert(t)}}, 0)

	// A cleartext HTTP request is not a valid ClientHello: the handshake must
	// fail and the connection must be dropped without an HTTP response.
	out := rawExchange(t, addr, "GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
	if strings.Contains(out, "HTTP/1.1") {
		t.Fatalf("TLS port answered a cleartext request: %q", truncate(out, 200))
	}
	mu.Lock()
	defer mu.Unlock()
	if reached {
		t.Fatal("handler ran for a cleartext request on a TLS port")
	}
}

func TestHTTPSHandshakeFailures(t *testing.T) {
	ca := newTestCA(t)
	addr := startTLS(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("handler must not run for a failed handshake")
	}), &tls.Config{
		Certificates: []tls.Certificate{ca.serverCert(t)},
		MinVersion:   tls.VersionTLS13,
	}, 0)

	cases := []struct {
		name string
		cfg  *tls.Config
	}{
		{
			// Client capped below the server's MinVersion.
			name: "version-too-old",
			cfg: &tls.Config{
				RootCAs:    ca.pool,
				ServerName: "localhost",
				MinVersion: tls.VersionTLS10,
				MaxVersion: tls.VersionTLS12,
			},
		},
		{
			// Server certificate is not signed by a CA the client trusts.
			name: "untrusted-server-cert",
			cfg: &tls.Config{
				RootCAs:    newTestCA(t).pool,
				ServerName: "localhost",
				MinVersion: tls.VersionTLS12,
			},
		},
		{
			// Certificate is valid but not for this name.
			name: "hostname-mismatch",
			cfg: &tls.Config{
				RootCAs:    ca.pool,
				ServerName: "wrong.example",
				MinVersion: tls.VersionTLS12,
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := &net.Dialer{Timeout: testDialTimeout}
			conn, err := tls.DialWithDialer(d, "tcp", addr, tc.cfg)
			if err == nil {
				_ = conn.Close()
				t.Fatal("handshake succeeded, want failure")
			}
		})
	}

	// After all those failures the server must still complete a valid handshake.
	// dialRawTLS fails the test if it does not.
	cfg := clientTLSConfig(ca, "localhost")
	cfg.MinVersion = tls.VersionTLS13
	dialRawTLS(t, addr, cfg).close()
}

func TestHTTPSTruncatedClientHello(t *testing.T) {
	ca := newTestCA(t)
	var handled atomic.Int32
	addr := startTLS(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handled.Add(1)
		_, _ = io.WriteString(w, "ok")
	}), &tls.Config{Certificates: []tls.Certificate{ca.serverCert(t)}}, 0)

	// A TLS record header promising 512 bytes of ClientHello, followed by
	// nothing. The server should sit waiting for the rest, never reach the
	// handler, and never emit HTTP.
	stalled := dialRaw(t, addr)
	stalled.write("\x16\x03\x01\x02\x00")
	if out := stalled.readAll(time.Second); out != "" {
		t.Fatalf("server replied to a truncated ClientHello: %q", truncate(out, 120))
	}
	stalled.close()

	// A healthy client must still be served afterwards.
	c := dialRawTLS(t, addr, clientTLSConfig(ca, "localhost"))
	defer c.close()
	c.write("GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
	resp := c.readResponse("GET")
	if got := readBody(t, resp); got != "ok" {
		t.Fatalf("server unhealthy after a truncated handshake: body = %q", got)
	}
	if n := handled.Load(); n != 1 {
		t.Fatalf("handler ran %d times, want exactly 1 (the truncated hello must not reach it)", n)
	}
}

// ---------------------------------------------------------------------------
// ListenAndServeTLS
// ---------------------------------------------------------------------------

func TestHTTPSListenAndServeTLSFromFiles(t *testing.T) {
	certPEM, keyPEM, err := testcert.Generate()
	if err != nil {
		t.Fatalf("generate cert: %v", err)
	}
	dir := t.TempDir()
	certFile := filepath.Join(dir, "cert.pem")
	keyFile := filepath.Join(dir, "key.pem")
	if err := os.WriteFile(certFile, certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, keyPEM, 0o600); err != nil {
		t.Fatal(err)
	}

	srv := &Server{
		Addr: freeAddr(t),
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintf(w, "tls=%v path=%s", r.TLS != nil, r.URL.Path)
		}),
	}
	go func() { _ = srv.ListenAndServeTLS(certFile, keyFile) }()
	t.Cleanup(func() { _ = srv.Close() })
	waitReady(t, srv.Addr)

	pool, err := certPoolFromPEM(certPEM)
	if err != nil {
		t.Fatalf("build cert pool: %v", err)
	}
	client := newClient(t, &tls.Config{RootCAs: pool, ServerName: "localhost", MinVersion: tls.VersionTLS12})
	resp, err := client.Get("https://" + srv.Addr + "/files")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	if got := readBody(t, resp); got != "tls=true path=/files" {
		t.Fatalf("body = %q", got)
	}
}

func TestHTTPSListenAndServeTLSBadCertFiles(t *testing.T) {
	srv := &Server{Addr: freeAddr(t), Handler: http.NotFoundHandler()}
	err := srv.ListenAndServeTLS(filepath.Join(t.TempDir(), "missing.pem"), filepath.Join(t.TempDir(), "missing.key"))
	if err == nil {
		_ = srv.Close()
		t.Fatal("ListenAndServeTLS accepted missing certificate files")
	}
}

// TestHTTPSTLSConfigPreservedByListenAndServeTLS checks that the certificate
// from the files is merged into an existing TLSConfig rather than replacing it.
func TestHTTPSTLSConfigPreservedByListenAndServeTLS(t *testing.T) {
	certPEM, keyPEM, err := testcert.Generate()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certFile := filepath.Join(dir, "cert.pem")
	keyFile := filepath.Join(dir, "key.pem")
	if err := os.WriteFile(certFile, certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, keyPEM, 0o600); err != nil {
		t.Fatal(err)
	}

	srv := &Server{
		Addr: freeAddr(t),
		TLSConfig: &tls.Config{
			MinVersion: tls.VersionTLS13,
			NextProtos: []string{"http/1.1"},
		},
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintf(w, "version=%#04x alpn=%s", r.TLS.Version, r.TLS.NegotiatedProtocol)
		}),
	}
	go func() { _ = srv.ListenAndServeTLS(certFile, keyFile) }()
	t.Cleanup(func() { _ = srv.Close() })
	waitReady(t, srv.Addr)

	pool, err := certPoolFromPEM(certPEM)
	if err != nil {
		t.Fatal(err)
	}
	cfg := &tls.Config{
		RootCAs:    pool,
		ServerName: "localhost",
		MinVersion: tls.VersionTLS12,
		NextProtos: []string{"http/1.1"},
	}
	c := dialRawTLS(t, srv.Addr, cfg)
	defer c.close()
	c.write("GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
	want := fmt.Sprintf("version=%#04x alpn=http/1.1", tls.VersionTLS13)
	if got := readBody(t, c.readResponse("GET")); got != want {
		t.Fatalf("body = %q, want %q (configured MinVersion/NextProtos were dropped)", got, want)
	}
}

func certPoolFromPEM(certPEM []byte) (*x509.CertPool, error) {
	block, _ := pem.Decode(certPEM)
	if block == nil {
		return nil, fmt.Errorf("no PEM block in certificate")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return pool, nil
}

// ---------------------------------------------------------------------------
// Advanced HTTPS / TLS Edge Cases
// ---------------------------------------------------------------------------

func TestHTTPSTLSSessionResumption(t *testing.T) {
	ca := newTestCA(t)
	addr := startTLS(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil {
			t.Error("r.TLS is nil")
			return
		}
		fmt.Fprintf(w, "resumed=%v", r.TLS.DidResume)
	}), &tls.Config{
		Certificates: []tls.Certificate{ca.serverCert(t)},
	}, 0)

	sessionCache := tls.NewLRUClientSessionCache(16)
	cfg := clientTLSConfig(ca, "localhost")
	cfg.ClientSessionCache = sessionCache

	// First request creates a new session
	client := newClient(t, cfg)
	resp1, err := client.Get("https://" + addr + "/1")
	if err != nil {
		t.Fatalf("first GET: %v", err)
	}
	if got := readBody(t, resp1); got != "resumed=false" {
		t.Fatalf("first request got %q, want resumed=false", got)
	}

	// Second request on a fresh connection should resume the cached session
	resp2, err := client.Get("https://" + addr + "/2")
	if err != nil {
		t.Fatalf("second GET: %v", err)
	}
	if got := readBody(t, resp2); got != "resumed=true" {
		t.Fatalf("second request got %q, want resumed=true (TLS session resumption failed)", got)
	}
}

func TestHTTPSTLSReadTimeoutPostHandshake(t *testing.T) {
	ca := newTestCA(t)
	srv := &Server{
		Addr:        freeAddr(t),
		TLSConfig:   &tls.Config{Certificates: []tls.Certificate{ca.serverCert(t)}},
		ReadTimeout: 120 * time.Millisecond,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.WriteString(w, "should-not-reach")
		}),
	}
	addr := startServer(t, srv)

	c := dialRawTLS(t, addr, clientTLSConfig(ca, "localhost"))
	defer c.close()

	// TLS handshake completed, but client does not send HTTP request bytes.
	// Server must close the connection within ReadTimeout.
	out := c.readAll(1500 * time.Millisecond)
	if strings.Contains(out, "should-not-reach") {
		t.Fatalf("handler executed unexpectedly: %q", out)
	}
	// Subsequent read should hit EOF / closed connection
	if _, err := c.readResponseErr("GET"); err == nil {
		t.Fatal("connection remained open after ReadTimeout expired")
	}
}

func TestHTTPSTLSReadTimeoutHandshake(t *testing.T) {
	ca := newTestCA(t)
	srv := &Server{
		Addr:        freeAddr(t),
		TLSConfig:   &tls.Config{Certificates: []tls.Certificate{ca.serverCert(t)}},
		ReadTimeout: 150 * time.Millisecond,
		Handler:     http.NotFoundHandler(),
	}
	addr := startServer(t, srv)

	c := dialRaw(t, addr)
	defer c.close()

	// Send only 3 bytes of TLS record header, then stop
	c.write("\x16\x03\x01")

	// ReadTimeout should fire and close the connection without hanging
	start := time.Now()
	out := c.readAll(2 * time.Second)
	if out != "" {
		t.Fatalf("unexpected output: %q", out)
	}
	if dur := time.Since(start); dur < 100*time.Millisecond {
		t.Logf("handshake timeout closed connection promptly in %v", dur)
	}
}

func TestHTTPSTLSIdleTimeout(t *testing.T) {
	ca := newTestCA(t)
	srv := &Server{
		Addr:        freeAddr(t),
		TLSConfig:   &tls.Config{Certificates: []tls.Certificate{ca.serverCert(t)}},
		IdleTimeout: 100 * time.Millisecond,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.WriteString(w, "ok:"+r.URL.Path)
		}),
	}
	addr := startServer(t, srv)

	c := dialRawTLS(t, addr, clientTLSConfig(ca, "localhost"))
	defer c.close()

	// First request succeeds
	c.write("GET /first HTTP/1.1\r\nHost: localhost\r\n\r\n")
	resp := c.readResponse("GET")
	if got := readBody(t, resp); got != "ok:/first" {
		t.Fatalf("first response = %q, want ok:/first", got)
	}

	// Sleep longer than IdleTimeout (100ms)
	time.Sleep(200 * time.Millisecond)

	// Second request on the same connection should fail because server closed it
	c.write("GET /second HTTP/1.1\r\nHost: localhost\r\n\r\n")
	if _, err := c.readResponseErr("GET"); err == nil {
		t.Fatal("connection stayed open after IdleTimeout exceeded")
	}
}

func TestHTTPSWildcardSNI(t *testing.T) {
	ca := newTestCA(t)
	wildCert := ca.issue(t, "*.example.test", []string{"*.example.test"}, nil, false)

	addr := startTLS(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "sni=%s", r.TLS.ServerName)
	}), &tls.Config{
		Certificates: []tls.Certificate{wildCert},
	}, 0)

	for _, sub := range []string{"api.example.test", "auth.example.test", "cdn.example.test"} {
		t.Run(sub, func(t *testing.T) {
			c := dialRawTLS(t, addr, clientTLSConfig(ca, sub))
			defer c.close()
			c.write("GET / HTTP/1.1\r\nHost: " + sub + "\r\nConnection: close\r\n\r\n")
			resp := c.readResponse("GET")
			if got := readBody(t, resp); got != "sni="+sub {
				t.Fatalf("got body %q, want sni=%s", got, sub)
			}
		})
	}
}

func TestHTTPSCustomCipherSuites(t *testing.T) {
	ca := newTestCA(t)
	cert := ca.serverCert(t)

	// TLS 1.2 with ECDHE-ECDSA-AES128-GCM-SHA256
	srvCipher := uint16(tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256)
	addr := startTLS(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "cipher=%#04x", r.TLS.CipherSuite)
	}), &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
		MaxVersion:   tls.VersionTLS12,
		CipherSuites: []uint16{srvCipher},
	}, 0)

	t.Run("matching-cipher-succeeds", func(t *testing.T) {
		cfg := clientTLSConfig(ca, "localhost")
		cfg.MinVersion = tls.VersionTLS12
		cfg.MaxVersion = tls.VersionTLS12
		cfg.CipherSuites = []uint16{srvCipher}

		resp, err := newClient(t, cfg).Get("https://" + addr + "/cipher")
		if err != nil {
			t.Fatalf("GET: %v", err)
		}
		if got := readBody(t, resp); got != fmt.Sprintf("cipher=%#04x", srvCipher) {
			t.Fatalf("got %q, want cipher=%#04x", got, srvCipher)
		}
	})

	t.Run("mismatch-cipher-fails", func(t *testing.T) {
		cfg := clientTLSConfig(ca, "localhost")
		cfg.MinVersion = tls.VersionTLS12
		cfg.MaxVersion = tls.VersionTLS12
		// Client only offers an AES-256 cipher which the server does not support
		cfg.CipherSuites = []uint16{tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384}

		d := &net.Dialer{Timeout: testDialTimeout}
		conn, err := tls.DialWithDialer(d, "tcp", addr, cfg)
		if err == nil {
			_ = conn.Close()
			t.Fatal("handshake succeeded despite mismatched cipher suites")
		}
	})
}

func TestHTTPSConcurrentSmallAndLargeMix(t *testing.T) {
	ca := newTestCA(t)
	addr := startTLS(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
			return
		}
		sum := sha256.Sum256(body)
		w.Header().Set("X-Sha256", hex.EncodeToString(sum[:]))
		w.Header().Set("X-Path", r.URL.Path)
		_, _ = w.Write(body)
	}), &tls.Config{Certificates: []tls.Certificate{ca.serverCert(t)}}, 5*time.Second)

	client := newClient(t, clientTLSConfig(ca, "localhost"))
	const workers = 12
	var wg sync.WaitGroup
	errs := make(chan error, workers)

	for wk := 0; wk < workers; wk++ {
		wg.Add(1)
		go func(wk int) {
			defer wg.Done()
			var size int
			if wk%3 == 0 {
				size = 128 * 1024 // Large 128KB payload
			} else {
				size = (wk + 1) * 64 // Small payload
			}
			payload := make([]byte, size)
			for i := range payload {
				payload[i] = byte((i + wk) % 256)
			}
			wantSha := sha256.Sum256(payload)
			path := fmt.Sprintf("/w%d", wk)

			resp, err := client.Post("https://"+addr+path, "application/octet-stream", bytes.NewReader(payload))
			if err != nil {
				errs <- fmt.Errorf("worker %d POST: %w", wk, err)
				return
			}
			defer resp.Body.Close()

			if gotSha := resp.Header.Get("X-Sha256"); gotSha != hex.EncodeToString(wantSha[:]) {
				errs <- fmt.Errorf("worker %d: sha mismatch: got %s, want %s", wk, gotSha, hex.EncodeToString(wantSha[:]))
				return
			}
			echoed, err := io.ReadAll(resp.Body)
			if err != nil {
				errs <- fmt.Errorf("worker %d read: %w", wk, err)
				return
			}
			if !bytes.Equal(echoed, payload) {
				errs <- fmt.Errorf("worker %d: echoed payload mismatch (size %d vs %d)", wk, len(echoed), len(payload))
				return
			}
		}(wk)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestHTTPSClientAbruptDisconnect(t *testing.T) {
	ca := newTestCA(t)
	addr := startTLS(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "healthy")
	}), &tls.Config{Certificates: []tls.Certificate{ca.serverCert(t)}}, 0)

	// 10 clients connect, perform TLS handshake, then abruptly close connection without HTTP request
	for i := 0; i < 10; i++ {
		c := dialRawTLS(t, addr, clientTLSConfig(ca, "localhost"))
		c.close()
	}

	// Server must remain healthy and answer subsequent regular HTTPS clients
	client := newClient(t, clientTLSConfig(ca, "localhost"))
	resp, err := client.Get("https://" + addr + "/health")
	if err != nil {
		t.Fatalf("health check failed after abrupt disconnects: %v", err)
	}
	if got := readBody(t, resp); got != "healthy" {
		t.Fatalf("got body %q, want healthy", got)
	}
}

// A TLS client that stalls mid-handshake is bounded by ReadHeaderTimeout even
// without a ReadTimeout.
func TestHTTPSHeaderTimeoutBoundsStalledHandshake(t *testing.T) {
	ca := newTestCA(t)
	addr := startServer(t, &Server{
		TLSConfig:         &tls.Config{Certificates: []tls.Certificate{ca.serverCert(t)}},
		ReadHeaderTimeout: 300 * time.Millisecond,
		Handler:           http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}),
	})
	c, err := net.DialTimeout("tcp", addr, testDialTimeout)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_, _ = c.Write([]byte{0x16, 0x03, 0x01}) // the start of a ClientHello, then nothing
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := c.Read(make([]byte, 1)); err == nil || isTimeout(err) {
		t.Fatalf("stalled handshake still open (err=%v)", err)
	}
}
