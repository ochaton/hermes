package cmd

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/quic-go/quic-go/http3"
)

type entry struct {
	Path string `json:"path"`
	Size int64  `json:"size"`
	Dir  bool   `json:"dir,omitempty"`
	src  string
}

type meta struct {
	Name  string  `json:"name"`
	Bytes int64   `json:"bytes"`
	Files []entry `json:"files"`
}

func Send(args []string) {
	flags := newFlags("send")
	port := flags.Int("port", 4443, "listen port; if taken, the next free one up to 4499")
	serve := flags.Bool("serve", false, "keep serving after the first receive until Ctrl+C")
	paths := parseArgs(flags, args)
	if len(paths) == 0 {
		flags.Usage()
		os.Exit(1)
	}

	m, err := walk(paths)
	if err != nil {
		fail(err)
	}
	tlsCfg, err := selfSignedTLS()
	if err != nil {
		fail(err)
	}
	var tok [8]byte
	_, _ = rand.Read(tok[:])
	token := hex.EncodeToString(tok[:])

	var mu sync.Mutex
	var cur *progress
	current := func() *progress {
		mu.Lock()
		defer mu.Unlock()
		if cur == nil {
			cur = newProgress(m.Name, m.Bytes)
		}
		return cur
	}
	doneCh := make(chan struct{}, 1)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /"+token+"/meta", func(w http.ResponseWriter, r *http.Request) {
		current()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(m)
	})
	mux.HandleFunc("GET /"+token+"/f/{i}", func(w http.ResponseWriter, r *http.Request) {
		i, err := strconv.Atoi(r.PathValue("i"))
		if err != nil || i < 0 || i >= len(m.Files) || m.Files[i].Dir {
			http.NotFound(w, r)
			return
		}
		e := m.Files[i]
		start, end := int64(0), e.Size-1
		if h := r.Header.Get("Range"); h != "" {
			if _, err := fmt.Sscanf(strings.TrimPrefix(h, "bytes="), "%d-%d", &start, &end); err != nil {
				start = -1
			}
		}
		if start < 0 || end < start || end >= e.Size {
			http.Error(w, "bad range", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		f, err := os.Open(e.src)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer func() { _ = f.Close() }()
		length := end - start + 1

		h := xxhash.New()
		if _, err := io.Copy(h, io.NewSectionReader(f, start, length)); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		hd := w.Header()
		hd.Set("Accept-Ranges", "bytes")
		hd.Set("ETag", fmt.Sprintf("%016x", h.Sum64()))
		hd.Set("Content-Length", strconv.FormatInt(length, 10))
		hd.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, e.Size))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = io.Copy(&countingWriter{w: w, counter: &current().n}, io.NewSectionReader(f, start, length))
	})
	mux.HandleFunc("POST /"+token+"/done", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		p := cur
		cur = nil
		mu.Unlock()
		if p != nil {
			p.finish()
		}
		if jsonOut {
			emit("event", "done", "path", m.Name, "bytes", m.Bytes)
		}
		w.WriteHeader(http.StatusNoContent)
		w.(http.Flusher).Flush()
		if !*serve {
			select {
			case doneCh <- struct{}{}:
			default:
			}
		}
	})

	var tcpLn net.Listener
	var udpConn *net.UDPConn
	bound := *port
	for {
		l, err := net.Listen("tcp", ":"+strconv.Itoa(bound))
		if err == nil {
			u, err := net.ListenUDP("udp", &net.UDPAddr{Port: bound})
			if err == nil {
				tcpLn, udpConn = l, u
				break
			}
			_ = l.Close()
		}
		if bound++; bound > max(*port, 4499) {
			fail(fmt.Errorf("ports %d-%d are busy", *port, bound-1))
		}
	}

	t := ticket(bound, tok[:])
	if t == "" {
		fail(errors.New("no address"))
	}
	if jsonOut {
		emit("event", "ready", "ticket", t)
	} else {
		fmt.Println("hermes get " + t)
	}

	tcpTLS := tlsCfg.Clone()
	tcpTLS.NextProtos = []string{"h2", "http/1.1"}
	tcpSrv := &http.Server{Handler: mux, ErrorLog: log.New(io.Discard, "", 0)}
	h3Srv := &http3.Server{TLSConfig: tlsCfg, Handler: mux}
	go func() { _ = tcpSrv.Serve(tls.NewListener(tcpLn, tcpTLS)) }()
	go func() { _ = h3Srv.Serve(udpConn) }()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, stopSignals...)
	select {
	case <-doneCh:
		_ = h3Srv.Close()
		_ = tcpSrv.Close()
		exit(0, "")
	case <-sig:
		_ = h3Srv.Close()
		_ = tcpSrv.Close()
		if *serve {
			exit(0, "")
		}
		fail(errors.New("cancelled"))
	}
}

func walk(args []string) (meta, error) {
	var m meta
	seen := map[string]bool{}
	for _, a := range args {
		fi, err := os.Stat(a)
		if errors.Is(err, fs.ErrNotExist) {
			return m, fmt.Errorf("not found: %s", a)
		}
		if err != nil {
			return m, err
		}
		if !fi.IsDir() && !fi.Mode().IsRegular() {
			return m, fmt.Errorf("not a file or folder: %s", a)
		}
		abs, err := filepath.Abs(a)
		if err != nil {
			return m, err
		}
		top := filepath.Base(abs)
		if !safePath(top) || seen[top] {
			return m, fmt.Errorf("cannot send %s: pick another name", a)
		}
		seen[top] = true
		root, err := filepath.EvalSymlinks(abs)
		if err != nil {
			return m, err
		}
		err = filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !d.IsDir() && !d.Type().IsRegular() {
				return nil
			}
			rel, err := filepath.Rel(root, p)
			if err != nil {
				return err
			}
			e := entry{Path: path.Join(top, filepath.ToSlash(rel)), Dir: d.IsDir(), src: p}
			if !e.Dir {
				info, err := d.Info()
				if err != nil {
					return err
				}
				e.Size = info.Size()
			}
			m.Files = append(m.Files, e)
			m.Bytes += e.Size
			return nil
		})
		if err != nil {
			return m, err
		}
	}
	m.Name = m.Files[0].Path
	if len(args) > 1 {
		m.Name = "files"
	}
	return m, nil
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
	return &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}}, nil
}
