package cmd

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"
)

const discoPort = 47043

// beacon answers "H1 Q <code>" queries with "H1 R <code> <ip> <port> <token>"
// and broadcasts the same reply every second.
func beacon(code, host string, port int, token string) {
	lc := net.ListenConfig{Control: udpControl}
	pc, err := lc.ListenPacket(context.Background(), "udp4", fmt.Sprintf(":%d", discoPort))
	if err != nil {
		return
	}
	reply := []byte(fmt.Sprintf("H1 R %s %s %d %s", code, host, port, token))
	go func() {
		for {
			for _, a := range broadcastAddrs() {
				_, _ = pc.WriteTo(reply, a)
			}
			time.Sleep(time.Second)
		}
	}()
	buf := make([]byte, 512)
	for {
		n, from, err := pc.ReadFrom(buf)
		if err != nil {
			return
		}
		f := strings.Fields(string(buf[:n]))
		if len(f) == 3 && f[0] == "H1" && f[1] == "Q" && (f[2] == code || f[2] == "-") {
			_, _ = pc.WriteTo(reply, from)
		}
	}
}

// discover broadcasts "H1 Q <code>" until a matching reply arrives or wait ends,
// and returns "<ip>:<port>/<token>". The reply's source address is used as ip:
// it is the sender interface that just reached us, unlike a VPN or other
// outbound address the sender may advertise.
func discover(ctx context.Context, code string, wait time.Duration) (string, error) {
	lc := net.ListenConfig{Control: udpControl}
	pc, err := lc.ListenPacket(ctx, "udp4", ":0")
	if err != nil {
		return "", err
	}
	qctx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	go func() {
		defer func() { _ = pc.Close() }()
		query := []byte("H1 Q " + code)
		t := time.NewTicker(500 * time.Millisecond)
		defer t.Stop()
		for {
			for _, a := range broadcastAddrs() {
				_, _ = pc.WriteTo(query, a)
			}
			select {
			case <-qctx.Done():
				return
			case <-t.C:
			}
		}
	}()
	buf := make([]byte, 512)
	for {
		n, from, err := pc.ReadFrom(buf)
		if err != nil {
			if ctx.Err() != nil {
				return "", errors.New("cancelled")
			}
			return "", fmt.Errorf("not found: %s", code)
		}
		f := strings.Fields(string(buf[:n]))
		if len(f) == 6 && f[0] == "H1" && f[1] == "R" && f[2] == code {
			return net.JoinHostPort(from.(*net.UDPAddr).IP.String(), f[4]) + "/" + f[5], nil
		}
	}
}

func broadcastAddrs() []*net.UDPAddr {
	addrs := []*net.UDPAddr{{IP: net.IPv4bcast, Port: discoPort}, {IP: net.IPv4(127, 0, 0, 1), Port: discoPort}}
	ifs, _ := net.Interfaces()
	for _, ifc := range ifs {
		if ifc.Flags&net.FlagUp == 0 || ifc.Flags&net.FlagBroadcast == 0 {
			continue
		}
		as, _ := ifc.Addrs()
		for _, a := range as {
			n, ok := a.(*net.IPNet)
			if !ok || n.IP.To4() == nil {
				continue
			}
			ip, mask := n.IP.To4(), n.Mask[len(n.Mask)-4:]
			b := make(net.IP, 4)
			for i := range b {
				b[i] = ip[i] | ^mask[i]
			}
			addrs = append(addrs, &net.UDPAddr{IP: b, Port: discoPort})
		}
	}
	return addrs
}
