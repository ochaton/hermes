package cmd

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const Usage = "hermes send <file-or-folder>...\nhermes get <ticket>\n"

var (
	jsonOut bool
	quiet   bool
	outMu   sync.Mutex
	drawn   bool // a progress line without newline is on stderr; guarded by outMu
)

func newFlags(name string) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ExitOnError)
	fs.Usage = func() {
		fmt.Fprint(os.Stderr, Usage)
		fs.PrintDefaults()
	}
	fs.BoolVar(&jsonOut, "json", false, "NDJSON events on stdout")
	fs.BoolVar(&quiet, "quiet", false, "no progress")
	return fs
}

// parseArgs allows flags before, between and after positional arguments.
func parseArgs(fs *flag.FlagSet, args []string) []string {
	var pos []string
	for {
		_ = fs.Parse(args)
		if fs.NArg() == 0 {
			return pos
		}
		pos = append(pos, fs.Arg(0))
		args = fs.Args()[1:]
	}
}

func event(kv ...any) []byte {
	b := []byte{'{'}
	for i := 0; i < len(kv); i += 2 {
		if i > 0 {
			b = append(b, ',')
		}
		k, _ := json.Marshal(kv[i])
		v, _ := json.Marshal(kv[i+1])
		b = append(append(append(b, k...), ':'), v...)
	}
	return append(b, '}', '\n')
}

func emit(kv ...any) {
	outMu.Lock()
	_, _ = os.Stdout.Write(event(kv...))
	outMu.Unlock()
}

// exit holds outMu until the process ends so no progress redraw lands after msg.
func exit(code int, msg string) {
	outMu.Lock()
	if drawn {
		fmt.Fprintln(os.Stderr)
	}
	if msg != "" {
		if jsonOut {
			_, _ = os.Stdout.Write(event("event", "error", "error", msg))
		}
		fmt.Fprintln(os.Stderr, msg)
	}
	os.Exit(code)
}

func fail(err error) {
	exit(1, err.Error())
}

type progress struct {
	name  string
	total int64
	n     atomic.Int64
	start time.Time
	tty   bool
	fancy bool
	stop  chan struct{}
	done  chan struct{}
}

func newProgress(name string, total int64) *progress {
	p := &progress{name: name, total: total, start: time.Now(), stop: make(chan struct{}), done: make(chan struct{})}
	fi, err := os.Stderr.Stat()
	p.tty = err == nil && fi.Mode()&os.ModeCharDevice != 0 && enableVT()
	p.fancy = p.tty && os.Getenv("NO_COLOR") == "" && utf8Locale()
	go p.loop()
	return p
}

func utf8Locale() bool {
	if runtime.GOOS == "windows" {
		return true
	}
	for _, k := range []string{"LC_ALL", "LC_CTYPE", "LANG"} {
		if v := strings.ToLower(os.Getenv(k)); v != "" {
			return strings.Contains(v, "utf-8") || strings.Contains(v, "utf8")
		}
	}
	return false
}

func (p *progress) loop() {
	defer close(p.done)
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()
	type sample struct {
		t time.Time
		n int64
	}
	var ring [30]sample
	ring[0].t = p.start
	lastJSON := p.start
	for i := 1; ; i++ {
		var now time.Time
		select {
		case <-p.stop:
			return
		case now = <-t.C:
		}
		n := min(p.n.Load(), p.total)
		old := ring[0]
		if i >= len(ring) {
			old = ring[i%len(ring)]
		}
		ring[i%len(ring)] = sample{now, n}
		bps := float64(n-old.n) / now.Sub(old.t).Seconds()
		switch {
		case quiet:
		case jsonOut:
			if now.Sub(lastJSON) >= time.Second {
				lastJSON = now
				emit("event", "progress", "done", n, "total", p.total, "bps", int64(bps))
			}
		case p.tty:
			eta := ""
			if bps > 0 {
				eta = fmtDur(time.Duration(float64(p.total-n) / bps * float64(time.Second)))
			}
			outMu.Lock()
			fmt.Fprint(os.Stderr, "\r\033[2K"+p.line(n, bps, eta))
			drawn = true
			outMu.Unlock()
		}
	}
}

func (p *progress) finish() {
	close(p.stop)
	<-p.done
	if quiet || jsonOut {
		return
	}
	el := time.Since(p.start)
	outMu.Lock()
	defer outMu.Unlock()
	if p.tty {
		fmt.Fprint(os.Stderr, "\r\033[2K")
	}
	fmt.Fprintln(os.Stderr, p.line(p.total, float64(p.total)/el.Seconds(), fmtDur(el)))
	drawn = false
}

func (p *progress) line(n int64, bps float64, tail string) string {
	pct := 100
	if p.total > 0 {
		pct = int(n * 100 / p.total)
	}
	fill := pct / 5
	bar := "[" + strings.Repeat("=", fill) + strings.Repeat(" ", 20-fill) + "]"
	if p.fancy {
		bar = "\033[32m" + strings.Repeat("█", fill) + "\033[0m" + strings.Repeat("░", 20-fill)
	}
	name := []rune(p.name)
	if len(name) > 24 {
		name = name[:24]
	}
	s := fmt.Sprintf("%s  %s  %d%%  %s/s", string(name), bar, pct, fmtBytes(int64(bps)))
	if tail != "" {
		s += "  " + tail
	}
	return s
}

func fmtBytes(n int64) string {
	if n < 1024 {
		return fmt.Sprintf("%d B", n)
	}
	f, i := float64(n)/1024, 0
	for f >= 1024 && i < 2 {
		f /= 1024
		i++
	}
	return fmt.Sprintf("%.1f %cB", f, "KMG"[i])
}

func fmtDur(d time.Duration) string {
	s := int(d.Seconds())
	switch {
	case s < 60:
		return fmt.Sprintf("%ds", s)
	case s < 3600:
		return fmt.Sprintf("%dm%02ds", s/60, s%60)
	}
	return fmt.Sprintf("%dh%02dm", s/3600, s/60%60)
}

// countingWriter adds every write to counter; rollback undoes this writer's share.
type countingWriter struct {
	w       io.Writer
	counter *atomic.Int64
	n       int64
}

func (cw *countingWriter) Write(p []byte) (int, error) {
	n, err := cw.w.Write(p)
	cw.n += int64(n)
	cw.counter.Add(int64(n))
	return n, err
}

func (cw *countingWriter) rollback() {
	cw.counter.Add(-cw.n)
	cw.n = 0
}
