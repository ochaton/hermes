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
	"net/http"
	"os"
	"os/signal"
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
	fs.Parse(args)

	if fs.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "usage: hermes share --addr <ip:port> <file>")
		os.Exit(1)
	}

	if strings.HasPrefix(*addr, "0.0.0.0") {
		fmt.Fprintln(os.Stderr, "warning: --addr is 0.0.0.0 — receiver will not be able to connect; use your public IP")
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
	f.Close()
	token := fmt.Sprintf("%016x", h.Sum64())

	entry := shareEntry{
		path:     path,
		filename: info.Name(),
		size:     info.Size(),
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/"+token+"/meta", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"filename": entry.filename,
			"size":     entry.size,
		})
	})

	mux.HandleFunc("/"+token, func(w http.ResponseWriter, r *http.Request) {
		rangeHdr := r.Header.Get("Range")
		if rangeHdr == "" {
			http.Error(w, "Range header required", http.StatusRequestedRangeNotSatisfiable)
			return
		}

		var start, end int64
		end = entry.size - 1
		fmt.Sscanf(strings.TrimPrefix(rangeHdr, "bytes="), "%d-%d", &start, &end)

		f, err := os.Open(entry.path)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer f.Close()

		if _, err := f.Seek(start, io.SeekStart); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		length := end - start + 1
		buf := make([]byte, length)
		if _, err := io.ReadFull(f, buf); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		h := xxhash.New()
		h.Write(buf)
		etag := fmt.Sprintf("%016x", h.Sum64())

		fmt.Fprintf(os.Stderr, "serving range %d-%d to %s\n", start, end, r.RemoteAddr)

		w.Header().Set("ETag", etag)
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, entry.size))
		w.WriteHeader(http.StatusPartialContent)
		w.Write(buf)
	})

	tlsCfg, err := selfSignedTLS()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	srv := &http3.Server{
		Addr:      *addr,
		TLSConfig: tlsCfg,
		Handler:   mux,
	}

	fmt.Printf("%s/%s\n", *addr, token)

	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
		<-ch
		srv.Close()
	}()

	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
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
