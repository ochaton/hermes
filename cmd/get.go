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
	wait := flags.Duration("wait", 3*time.Second, "how long to look for the sender")
	pos := parseArgs(flags, args)
	if len(pos) != 1 {
		flags.Usage()
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), stopSignals...)
	defer stop()

	target := pos[0]
	if !strings.Contains(target, "/") {
		code := strings.ToLower(target)
		if !isCode(code) {
			fail(fmt.Errorf("not a code: %s", target))
		}
		var err error
		if target, err = discover(ctx, code, *wait); err != nil {
			fail(err)
		}
	}
	addr, token, _ := strings.Cut(target, "/")
	base := "https://" + addr + "/" + token

	clients := make([]*http.Client, max(*conns, 1))
	for i := range clients {
		clients[i] = &http.Client{Transport: &http3.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}
	}
	m, err := fetchMeta(ctx, clients[0], base)
	if err != nil && ctx.Err() == nil {
		clients = []*http.Client{{Transport: &http.Transport{
			TLSClientConfig:     &tls.Config{InsecureSkipVerify: true},
			MaxIdleConnsPerHost: *streams,
		}}}
		m, err = fetchMeta(ctx, clients[0], base)
	}
	if err != nil {
		fail(err)
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
