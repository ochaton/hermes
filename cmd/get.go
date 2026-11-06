package cmd

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/quic-go/quic-go/http3"
)

const chunkSize = 4 * 1024 * 1024
const maxRetries = 3

type chunk struct {
	file       int
	start, end int64
	retry      int
}

func Get(args []string) {
	flags := newFlags("get")
	out := flags.String("o", ".", "output directory")
	streams := flags.Int("streams", 8, "parallel streams")
	conns := flags.Int("conns", 2, "QUIC connections; streams are spread across them")
	pos := parseArgs(flags, args)
	if len(pos) != 1 {
		flags.Usage()
		os.Exit(1)
	}
	target := pos[0]
	hosts, token := parseTicket(target)
	if addr, tok, ok := strings.Cut(target, "/"); ok {
		hosts, token = []string{addr}, tok
	}
	if hosts == nil {
		fail(fmt.Errorf("not a ticket: %s", target))
	}
	ctx, stop := signal.NotifyContext(context.Background(), stopSignals...)
	defer stop()

	c, base, m, err := race(ctx, hosts, token, *streams)
	if err != nil {
		if ctx.Err() != nil {
			err = errors.New("cancelled")
		}
		fail(err)
	}
	clients := []*http.Client{c}
	if _, ok := c.Transport.(*http3.Transport); ok {
		for range *conns - 1 {
			clients = append(clients, newClient(true, 0))
		}
	}
	if len(m.Files) == 0 {
		fail(errors.New("nothing to receive"))
	}
	for _, e := range m.Files {
		if !safePath(e.Path) || e.Size < 0 {
			fail(fmt.Errorf("unsafe path: %q", e.Path))
		}
	}
	if jsonOut {
		emit("event", "ready", "name", m.Name, "bytes", m.Bytes)
	}

	tops := map[string]string{}
	dests := make([]string, len(m.Files))
	tmps := make([]string, len(m.Files))
	cleanup := func() {
		for _, t := range tmps {
			if t != "" {
				_ = os.Remove(t)
			}
		}
	}
	var chunks []chunk
	for i, e := range m.Files {
		top, rest, _ := strings.Cut(e.Path, "/")
		if tops[top] == "" {
			tops[top] = finalName(filepath.Join(*out, top))
		}
		dests[i] = filepath.Join(tops[top], filepath.FromSlash(rest))
		if e.Dir {
			err = os.MkdirAll(dests[i], 0755)
		} else {
			tmps[i], err = createTemp(dests[i], e.Size)
		}
		if err != nil {
			cleanup()
			fail(err)
		}
		for s := int64(0); s < e.Size; s += chunkSize {
			chunks = append(chunks, chunk{file: i, start: s, end: min(s+chunkSize, e.Size) - 1})
		}
	}

	p := newProgress(m.Name, m.Bytes)
	if err := fetchAll(ctx, clients, base, chunks, tmps, *streams, &p.n); err != nil {
		cleanup()
		if ctx.Err() != nil {
			err = errors.New("cancelled")
		}
		fail(err)
	}
	p.finish()
	for i, t := range tmps {
		if t != "" {
			if err := os.Rename(t, dests[i]); err != nil {
				cleanup()
				fail(err)
			}
		}
	}

	dctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	if req, err := http.NewRequestWithContext(dctx, http.MethodPost, base+"/done", nil); err == nil {
		if resp, err := clients[0].Do(req); err == nil {
			_ = resp.Body.Close()
		}
	}
	cancel()

	dest, line := dests[0], dests[0]
	switch {
	case len(tops) > 1:
		dest, line = *out, fmt.Sprintf("%d files", len(tops))
	case m.Files[0].Dir:
		line += string(filepath.Separator)
	}
	if jsonOut {
		emit("event", "done", "path", dest, "bytes", m.Bytes)
	} else {
		fmt.Println(line)
	}
}

func newClient(quic bool, streams int) *http.Client {
	tlsCfg := &tls.Config{InsecureSkipVerify: true}
	if quic {
		return &http.Client{Transport: &http3.Transport{TLSClientConfig: tlsCfg}}
	}
	return &http.Client{Transport: &http.Transport{TLSClientConfig: tlsCfg, MaxIdleConnsPerHost: streams}}
}

// race fetches meta from every host over QUIC at once and, from 400ms on, over TCP.
// The first 200 wins; every other attempt is cancelled and its transport closed.
func race(ctx context.Context, hosts []string, token string, streams int) (*http.Client, string, meta, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	type win struct {
		c    *http.Client
		base string
		m    meta
	}
	won := make(chan win, 1)
	var first atomic.Bool
	var wg sync.WaitGroup
	start := func(quic bool) {
		for _, h := range hosts {
			wg.Go(func() {
				c, base := newClient(quic, streams), "https://"+h+"/"+token
				actx, acancel := context.WithTimeout(ctx, 3*time.Second)
				m, err := fetchMeta(actx, c, base)
				acancel()
				if err == nil && first.CompareAndSwap(false, true) {
					won <- win{c, base, m}
					cancel()
					return
				}
				if t, ok := c.Transport.(io.Closer); ok {
					_ = t.Close()
				}
				c.CloseIdleConnections()
			})
		}
	}
	start(true)
	go func() {
		select {
		case <-time.After(400 * time.Millisecond):
			start(false)
		case <-ctx.Done():
		}
		wg.Wait()
		close(won)
	}()
	w, ok := <-won
	if !ok {
		return nil, "", meta{}, errors.New("not found")
	}
	return w.c, w.base, w.m, nil
}

func fetchMeta(ctx context.Context, c *http.Client, base string) (meta, error) {
	var m meta
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/meta", nil)
	if err != nil {
		return m, err
	}
	resp, err := c.Do(req)
	if err != nil {
		return m, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return m, errors.New(resp.Status)
	}
	return m, json.NewDecoder(resp.Body).Decode(&m)
}

func createTemp(dest string, size int64) (string, error) {
	if err := os.MkdirAll(filepath.Dir(dest), 0755); err != nil {
		return "", err
	}
	tmp := filepath.Join(filepath.Dir(dest), "."+filepath.Base(dest)+".hermes.tmp")
	f, err := os.Create(tmp)
	if err != nil {
		return "", err
	}
	if err := f.Truncate(size); err != nil {
		_ = f.Close()
		return tmp, err
	}
	return tmp, f.Close()
}

// fetchAll downloads chunks over a shared pool of workers spread across clients,
// retrying each failed chunk up to maxRetries times.
func fetchAll(ctx context.Context, clients []*http.Client, base string, chunks []chunk, tmps []string, streams int, n *atomic.Int64) error {
	if len(chunks) == 0 {
		return nil
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	work := make(chan chunk, len(chunks))
	for _, c := range chunks {
		work <- c
	}
	var left atomic.Int64
	left.Store(int64(len(chunks)))
	var once sync.Once
	var ferr error
	var wg sync.WaitGroup
	for w := range min(max(streams, 1), len(chunks)) {
		client := clients[w%len(clients)]
		wg.Go(func() {
			for {
				var c chunk
				var ok bool
				select {
				case <-ctx.Done():
					return
				case c, ok = <-work:
					if !ok {
						return
					}
				}
				err := fetchChunk(ctx, client, base, tmps[c.file], c, n)
				switch {
				case err == nil:
					if left.Add(-1) == 0 {
						close(work)
					}
				case c.retry < maxRetries && ctx.Err() == nil:
					c.retry++
					work <- c
				default:
					once.Do(func() {
						ferr = err
						cancel()
					})
					return
				}
			}
		})
	}
	wg.Wait()
	if ferr != nil {
		return ferr
	}
	return ctx.Err()
}

func fetchChunk(ctx context.Context, client *http.Client, base, tmp string, c chunk, n *atomic.Int64) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/f/%d", base, c.file), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", c.start, c.end))
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusPartialContent {
		return errors.New(resp.Status)
	}
	f, err := os.OpenFile(tmp, os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	h := xxhash.New()
	cw := &countingWriter{w: io.MultiWriter(io.NewOffsetWriter(f, c.start), h), counter: n}
	if _, err := io.Copy(cw, resp.Body); err != nil {
		cw.rollback()
		return err
	}
	if fmt.Sprintf("%016x", h.Sum64()) != resp.Header.Get("ETag") {
		cw.rollback()
		return fmt.Errorf("chunk %d-%d: checksum mismatch", c.start, c.end)
	}
	return nil
}

func safePath(p string) bool {
	if strings.ContainsRune(p, '\\') || len(p) > 1 && p[1] == ':' {
		return false
	}
	for _, s := range strings.Split(p, "/") {
		if s == "" || s == "." || s == ".." {
			return false
		}
	}
	return filepath.IsLocal(filepath.FromSlash(p))
}

func finalName(name string) string {
	ext := filepath.Ext(name)
	base := strings.TrimSuffix(name, ext)
	for i := 1; ; i++ {
		if _, err := os.Lstat(name); errors.Is(err, os.ErrNotExist) {
			return name
		}
		name = fmt.Sprintf("%s.%d%s", base, i, ext)
	}
}
