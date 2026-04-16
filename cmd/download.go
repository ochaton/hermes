package cmd

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/cespare/xxhash/v2"
	"github.com/quic-go/quic-go/http3"
)

const defaultChunkSize = 4 * 1024 * 1024 // 4 MB
const maxRetries = 3

type fileMeta struct {
	Filename string `json:"filename"`
	Size     int64  `json:"size"`
}

type chunkTask struct {
	client     *http.Client
	baseURL    string
	token      string
	tmpName    string
	start      int64
	end        int64
	total      int64
	downloaded *atomic.Int64
	retry      int
}

func (t chunkTask) do(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, t.baseURL+"/"+t.token, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", t.start, t.end))

	resp, err := t.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	dst, err := os.OpenFile(t.tmpName, os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer dst.Close()

	h := xxhash.New()
	n, err := io.Copy(io.MultiWriter(&offsetWriter{f: dst, offset: t.start}, h), resp.Body)
	if err != nil {
		return err
	}

	etag := resp.Header.Get("ETag")
	got := fmt.Sprintf("%016x", h.Sum64())
	if etag != "" && got != etag {
		return fmt.Errorf("chunk %d-%d: checksum mismatch", t.start, t.end)
	}

	cur := t.downloaded.Add(n)
	fmt.Fprintf(os.Stderr, "\r%d / %d bytes (%d%%)", cur, t.total, cur*100/t.total)
	return nil
}

type chunkResult struct {
	task chunkTask
	err  error
}

// offsetWriter implements io.Writer on top of *os.File at a fixed offset,
// advancing the offset as bytes are written. Allows io.Copy into a specific
// region of a file without seeking.
type offsetWriter struct {
	f      *os.File
	offset int64
}

func (w *offsetWriter) Write(p []byte) (int, error) {
	n, err := w.f.WriteAt(p, w.offset)
	w.offset += int64(n)
	return n, err
}

func Download(args []string) {
	fs := flag.NewFlagSet("download", flag.ExitOnError)
	streams := fs.Int("streams", 8, "number of parallel streams")
	fs.Parse(args)

	if fs.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "usage: hermes download [--streams N] <ip:port/token>")
		os.Exit(1)
	}

	addrPort, token, ok := strings.Cut(fs.Arg(0), "/")
	if !ok {
		fmt.Fprintln(os.Stderr, "invalid argument: expected <ip:port/token>")
		os.Exit(1)
	}
	baseURL := "https://" + addrPort

	client := &http.Client{
		Transport: &http3.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}
	defer client.CloseIdleConnections()

	meta, err := fetchMeta(client, baseURL, token)
	if err != nil {
		fmt.Fprintln(os.Stderr, "fetch meta:", err)
		os.Exit(1)
	}

	tmpName := "." + meta.Filename + ".hermes.tmp"
	tmp, err := os.OpenFile(tmpName, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := tmp.Truncate(meta.Size); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	tmp.Close() // each worker opens its own fd

	numChunks := int((meta.Size + defaultChunkSize - 1) / defaultChunkSize)
	nWorkers := min(*streams, numChunks)

	var downloaded atomic.Int64
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// workCh is unbuffered: manager dispatches one task at a time via select.
	// feedbackCh is buffered at nWorkers so workers never block sending results.
	workCh := make(chan chunkTask)
	feedbackCh := make(chan chunkResult, nWorkers)
	errCh := make(chan error, 1)

	// Workers: each opens its own file descriptor per task received.
	var wg sync.WaitGroup
	for range nWorkers {
		wg.Go(func() {
			for task := range workCh {
				feedbackCh <- chunkResult{task, task.do(ctx)}
			}
		})
	}

	// Manager: dispatches tasks, receives results, reschedules on failure.
	go func() {
		defer close(workCh)

		pending := make([]chunkTask, numChunks)
		for i := range numChunks {
			start := int64(i) * defaultChunkSize
			end := min(start+defaultChunkSize-1, meta.Size-1)
			pending[i] = chunkTask{
				client:     client,
				baseURL:    baseURL,
				token:      token,
				tmpName:    tmpName,
				start:      start,
				end:        end,
				total:      meta.Size,
				downloaded: &downloaded,
			}
		}

		inFlight := 0
		drain := func() {
			for inFlight > 0 {
				<-feedbackCh
				inFlight--
			}
		}

		for len(pending) > 0 || inFlight > 0 {
			// nil channel disables the send case when nothing is pending.
			// next must be evaluated before the select — Go evaluates all
			// case expressions unconditionally, even for nil-channel cases.
			var sendCh chan chunkTask
			var next chunkTask
			if len(pending) > 0 {
				sendCh = workCh
				next = pending[0]
			}

			select {
			case sendCh <- next:
				pending = pending[1:]
				inFlight++

			case result := <-feedbackCh:
				inFlight--
				if result.err == nil {
					continue
				}
				if ctx.Err() != nil || result.task.retry >= maxRetries {
					cancel()
					drain()
					errCh <- result.err
					return
				}
				result.task.retry++
				pending = append(pending, result.task)
			}
		}
		errCh <- nil
	}()

	wg.Wait()

	if err := <-errCh; err != nil {
		os.Remove(tmpName)
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stderr, "\nverifying file checksum...\n")
	if err := verifyFile(tmpName, token); err != nil {
		os.Remove(tmpName)
		fmt.Fprintln(os.Stderr, "checksum mismatch:", err)
		os.Exit(1)
	}

	dest := finalName(meta.Filename)
	if err := os.Rename(tmpName, dest); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "saved to %s\n", dest)
}

func fetchMeta(client *http.Client, baseURL, token string) (*fileMeta, error) {
	resp, err := client.Get(baseURL + "/" + token + "/meta")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var m fileMeta
	return &m, json.NewDecoder(resp.Body).Decode(&m)
}


func verifyFile(name, token string) error {
	f, err := os.Open(name)
	if err != nil {
		return err
	}
	defer f.Close()

	h := xxhash.New()
	if _, err := io.Copy(h, f); err != nil {
		return err
	}
	got := fmt.Sprintf("%016x", h.Sum64())
	if got != token {
		return fmt.Errorf("got %s, want %s", got, token)
	}
	return nil
}

func finalName(filename string) string {
	ext := filepath.Ext(filename)
	base := strings.TrimSuffix(filename, ext)
	name := filename
	for i := 1; ; i++ {
		if _, err := os.Stat(name); os.IsNotExist(err) {
			return name
		}
		name = fmt.Sprintf("%s.%d%s", base, i, ext)
	}
}
