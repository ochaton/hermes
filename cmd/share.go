package cmd

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/big"
	"mime"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/quic-go/quic-go/http3"
)

type shareEntry struct {
	path     string
	filename string
	size     int64
}

func Share(args []string) {
	fs := flag.NewFlagSet("share", flag.ExitOnError)
	addr := fs.String("addr", "0.0.0.0:4443", "listen address ip:port")
	_ = fs.Parse(args)

	if fs.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "usage: hermes share --addr <ip:port> <file>")
		os.Exit(1)
	}

	if strings.HasPrefix(*addr, "0.0.0.0") {
		ip, err := outboundIP()
		if err != nil {
			fmt.Fprintln(os.Stderr, "warning: --addr is 0.0.0.0 and could not detect outbound IP:", err)
		} else {
			_, port, _ := strings.Cut(*addr, ":")
			*addr = ip + ":" + port
		}
	}

	path := fs.Arg(0)

	f, err := os.Open(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	info, err := f.Stat()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	h := xxhash.New()
	if _, err := io.Copy(h, f); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	_ = f.Close()
	token := fmt.Sprintf("%016x", h.Sum64())

	entry := shareEntry{
		path:     path,
		filename: info.Name(),
		size:     info.Size(),
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/"+token+"/meta", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"filename": entry.filename,
			"size":     entry.size,
		})
	})

	contentType := mime.TypeByExtension(filepath.Ext(entry.filename))
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	mux.HandleFunc("/"+token, func(w http.ResponseWriter, r *http.Request) {
		f, err := os.Open(entry.path)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer func() { _ = f.Close() }()

		w.Header().Set("Content-Type", contentType)
		w.Header().Set("Content-Disposition", fmt.Sprintf(`inline; filename="%s"`, entry.filename))
		w.Header().Set("Accept-Ranges", "bytes")

		rangeHdr := r.Header.Get("Range")
		if rangeHdr == "" {
			// Full file — browser opening the URL directly.
			fmt.Fprintf(os.Stderr, "serving full file to %s\n", r.RemoteAddr)
			w.Header().Set("Content-Length", fmt.Sprintf("%d", entry.size))
			_, _ = io.Copy(w, f)
			return
		}

		var start, end int64
		end = entry.size - 1
		_, _ = fmt.Sscanf(strings.TrimPrefix(rangeHdr, "bytes="), "%d-%d", &start, &end)
		length := end - start + 1

		// First pass: compute xxHash64 over the range (for hermes download verification).
		if _, err := f.Seek(start, io.SeekStart); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		h := xxhash.New()
		if _, err := io.Copy(h, io.LimitReader(f, length)); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		etag := fmt.Sprintf("%016x", h.Sum64())

		// Second pass: stream bytes to response.
		if _, err := f.Seek(start, io.SeekStart); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		fmt.Fprintf(os.Stderr, "serving range %d-%d to %s\n", start, end, r.RemoteAddr)

		w.Header().Set("ETag", etag)
		w.Header().Set("Content-Length", fmt.Sprintf("%d", length))
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, entry.size))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = io.Copy(w, io.LimitReader(f, length))
	})

	tlsCfg, err := selfSignedTLS()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	_, port, _ := net.SplitHostPort(*addr)

	// TCP server: HTTP/1.1 + HTTP/2, advertises HTTP/3 via Alt-Svc.
	tcpTLSCfg := tlsCfg.Clone()
	tcpTLSCfg.NextProtos = []string{"h2", "http/1.1"}
	tcpHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Alt-Svc", fmt.Sprintf(`h3=":%s"`, port))
		mux.ServeHTTP(w, r)
	})
	tcpSrv := &http.Server{Addr: *addr, Handler: tcpHandler}
	ln, err := tls.Listen("tcp", *addr, tcpTLSCfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	go func() {
		if err := tcpSrv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintln(os.Stderr, "tcp:", err)
		}
	}()

	// UDP server: HTTP/3 (QUIC).
	h3Srv := &http3.Server{
		Addr:      *addr,
		TLSConfig: tlsCfg,
		Handler:   mux,
	}

	fmt.Printf("%s/%s\n", *addr, token)

	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
		<-ch
		_ = h3Srv.Close()
		_ = tcpSrv.Close()
	}()

	if err := h3Srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// outboundIP returns the preferred local IP used for outbound connections
// by dialing a UDP "connection" (no packets sent) and reading the local address.
func outboundIP() (string, error) {
	conn, err := net.Dial("udp", "8.8.8.8:53")
	if err != nil {
		return "", err
	}
	_ = conn.Close()
	host, _, err := net.SplitHostPort(conn.LocalAddr().String())
	return host, err
}

func selfSignedTLS() (*tls.Config, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "hermes"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(24 * time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, err
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, err
	}
	return &tls.Config{Certificates: []tls.Certificate{cert}}, nil
}
