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
	"time"

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
	defer func() { _ = resp.Body.Close() }()

	dst, err := os.OpenFile(t.tmpName, os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer func() { _ = dst.Close() }()

	h := xxhash.New()
	cw := &countingWriter{
		w:       io.MultiWriter(&offsetWriter{f: dst, offset: t.start}, h),
		counter: t.downloaded,
	}
	if _, err := io.Copy(cw, resp.Body); err != nil {
		cw.rollback()
		return err
	}

	etag := resp.Header.Get("ETag")
	got := fmt.Sprintf("%016x", h.Sum64())
	if etag != "" && got != etag {
		cw.rollback()
		return fmt.Errorf("chunk %d-%d: checksum mismatch", t.start, t.end)
	}

	return nil
}

// countingWriter updates an atomic counter on every Write call.
// Call rollback to subtract the accumulated tally on failure.
type countingWriter struct {
	w       io.Writer
	counter *atomic.Int64
	n       int64
}

func (cw *countingWriter) Write(p []byte) (int, error) {
	n, err := cw.w.Write(p)
	if n > 0 {
		cw.n += int64(n)
		cw.counter.Add(int64(n))
	}
	return n, err
}

func (cw *countingWriter) rollback() {
	cw.counter.Add(-cw.n)
	cw.n = 0
}

type chunkResult struct {
	task chunkTask
	err  error
}

// offsetWriter writes to a fixed offset in a file, advancing on each call.
// Allows concurrent range writes without seek races.
type offsetWriter struct {
	f      *os.File
	offset int64
}

func (w *offsetWriter) Write(p []byte) (int, error) {
	n, err := w.f.WriteAt(p, w.offset)
	w.offset += int64(n)
	return n, err
}

func fmtBytes(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.1f Gbyte", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.1f Mbyte", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1f Kbyte", float64(n)/(1<<10))
	default:
		return fmt.Sprintf("%d byte", n)
	}
}

// speedTracker computes average throughput over the last 5 seconds using a
// fixed-size ring buffer of (time, cumulative-bytes) samples, one per second.
type speedTracker struct {
	samples [5]struct {
		t time.Time
		n int64
	}
	count int
	pos   int
}

func (s *speedTracker) record(n int64) {
	s.samples[s.pos] = struct {
		t time.Time
		n int64
	}{time.Now(), n}
	s.pos = (s.pos + 1) % 5
	if s.count < 5 {
		s.count++
	}
}

func (s *speedTracker) speed() float64 {
	if s.count < 2 {
		return 0
	}
	oldest := ((s.pos-s.count)%5 + 5) % 5
	newest := (s.pos - 1 + 5) % 5
	dt := s.samples[newest].t.Sub(s.samples[oldest].t).Seconds()
	if dt <= 0 {
		return 0
	}
	return float64(s.samples[newest].n-s.samples[oldest].n) / dt
}

func renderProgress(cur, total int64, speed float64) {
	pct := int(cur * 100 / total)
	const width = 30
	filled := width * pct / 100
	var bar strings.Builder
	bar.WriteString(strings.Repeat("=", filled))
	if filled < width {
		bar.WriteByte('>')
		bar.WriteString(strings.Repeat(" ", width-filled-1))
	}
	speedStr := ""
	if speed > 0 {
		speedStr = fmt.Sprintf(" | %s/s", fmtBytes(int64(speed)))
	}
	fmt.Fprintf(os.Stderr, "\r\033[K[%s] %s / %s (%d%%%s)",
		bar.String(), fmtBytes(cur), fmtBytes(total), pct, speedStr)
}

// startProgressDisplay refreshes the progress line once per second.
// The returned function stops the goroutine and prints the final state.
func startProgressDisplay(downloaded *atomic.Int64, total int64) func() {
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		var tracker speedTracker
		for {
			select {
			case <-stop:
				renderProgress(downloaded.Load(), total, 0)
				fmt.Fprintln(os.Stderr)
				return
			case <-ticker.C:
				n := downloaded.Load()
				tracker.record(n)
				renderProgress(n, total, tracker.speed())
			}
		}
	}()
	return func() {
		close(stop)
		<-done
	}
}

func Download(args []string) {
	fs := flag.NewFlagSet("download", flag.ExitOnError)
	streams := fs.Int("streams", 8, "number of parallel streams")
	conns := fs.Int("conns", 2, "number of QUIC connections; streams are spread across them")
	_ = fs.Parse(args)

	if fs.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "usage: hermes download [--streams N] [--conns N] <ip:port/token>")
		os.Exit(1)
	}

	addrPort, token, ok := strings.Cut(fs.Arg(0), "/")
	if !ok {
		fmt.Fprintln(os.Stderr, "invalid argument: expected <ip:port/token>")
		os.Exit(1)
	}
	baseURL := "https://" + addrPort

	clients := make([]*http.Client, max(*conns, 1))
	for i := range clients {
		clients[i] = &http.Client{
			Transport: &http3.Transport{
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: true,
					MinVersion:         tls.VersionTLS13,
				},
			},
			Timeout: 30 * time.Second,
		}
		defer clients[i].CloseIdleConnections()
	}
	client := clients[0]

	fmt.Fprintf(os.Stderr, "fetching meta from %s...\n", addrPort)
	metaStart := time.Now()
	meta, err := fetchMeta(client, baseURL, token)
	if err != nil {
		fmt.Fprintln(os.Stderr, "fetch meta:", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "meta fetched in %.2fs\n", time.Since(metaStart).Seconds())

	tmpName := "." + meta.Filename + ".hermes.tmp"
	tmp, err := os.OpenFile(tmpName, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := tmp.Truncate(meta.Size); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	_ = tmp.Close() // each worker opens its own fd

	numChunks := int((meta.Size + defaultChunkSize - 1) / defaultChunkSize)
	nWorkers := min(*streams, numChunks)
	fmt.Fprintf(os.Stderr, "downloading %q (%s) using %d streams\n", meta.Filename, fmtBytes(meta.Size), nWorkers)

	var downloaded atomic.Int64
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stopProgress := startProgressDisplay(&downloaded, meta.Size)

	// workCh is unbuffered: manager dispatches one task at a time via select.
	// feedbackCh is buffered at nWorkers so workers never block sending results.
	workCh := make(chan chunkTask)
	feedbackCh := make(chan chunkResult, nWorkers)
	errCh := make(chan error, 1)

	var wg sync.WaitGroup
	for w := range nWorkers {
		c := clients[w%len(clients)]
		wg.Go(func() {
			for task := range workCh {
				task.client = c
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
				baseURL:    baseURL,
				token:      token,
				tmpName:    tmpName,
				start:      start,
				end:        end,
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
	stopProgress()

	if err := <-errCh; err != nil {
		_ = os.Remove(tmpName)
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stderr, "verifying file checksum...\n")
	if err := verifyFile(tmpName, token); err != nil {
		_ = os.Remove(tmpName)
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
	defer func() { _ = resp.Body.Close() }()
	var m fileMeta
	return &m, json.NewDecoder(resp.Body).Decode(&m)
}

func verifyFile(name, token string) error {
	f, err := os.Open(name)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

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
