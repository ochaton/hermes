package cmd

import (
	"encoding/base64"
	"encoding/hex"
	"net"
	"net/netip"
	"slices"
	"strconv"
	"strings"
)

// ticket is "h1." + base64url of, big-endian: version 1, port u16, token,
// address count u8, then per address its family (4 or 6) and bytes.
// It carries every unicast address of every up interface, VPNs included;
// loopback could only make a remote receiver dial itself.
func ticket(port int, token []byte) string {
	var ips []netip.Addr
	ifs, _ := net.Interfaces()
	for _, ifc := range ifs {
		if ifc.Flags&net.FlagUp == 0 || ifc.Flags&net.FlagLoopback != 0 {
			continue
		}
		as, _ := ifc.Addrs()
		for _, a := range as {
			n, ok := a.(*net.IPNet)
			if !ok {
				continue
			}
			ip, _ := netip.AddrFromSlice(n.IP)
			ip = ip.Unmap()
			if ip.IsValid() && !ip.IsLoopback() && !ip.IsLinkLocalUnicast() && !ip.IsMulticast() &&
				!ip.IsUnspecified() && !slices.Contains(ips, ip) {
				ips = append(ips, ip)
			}
		}
	}
	if len(ips) == 0 {
		return ""
	}
	rank := func(ip netip.Addr) int {
		switch {
		case ip.Is4() && ip.IsPrivate():
			return 0
		case ip.Is4():
			return 1
		}
		return 2
	}
	slices.SortStableFunc(ips, func(a, b netip.Addr) int { return rank(a) - rank(b) })
	ips = ips[:min(len(ips), 16)]

	b := append([]byte{1, byte(port >> 8), byte(port)}, token...)
	b = append(b, byte(len(ips)))
	for _, ip := range ips {
		fam := byte(6)
		if ip.Is4() {
			fam = 4
		}
		b = append(append(b, fam), ip.AsSlice()...)
	}
	return "h1." + base64.RawURLEncoding.EncodeToString(b)
}

// parseTicket returns "ip:port" for every address in t and the hex token,
// or nil hosts if t is not a ticket.
func parseTicket(t string) ([]string, string) {
	rest, ok := strings.CutPrefix(t, "h1.")
	b, err := base64.RawURLEncoding.DecodeString(rest)
	if !ok || err != nil || len(b) < 12 || b[0] != 1 {
		return nil, ""
	}
	port := strconv.Itoa(int(b[1])<<8 | int(b[2]))
	token := hex.EncodeToString(b[3:11])
	var hosts []string
	n, b := int(b[11]), b[12:]
	for range n {
		if len(b) == 0 || b[0] != 4 && b[0] != 6 {
			return nil, ""
		}
		size := 4
		if b[0] == 6 {
			size = 16
		}
		if len(b) < 1+size {
			return nil, ""
		}
		hosts = append(hosts, net.JoinHostPort(net.IP(b[1:1+size]).String(), port))
		b = b[1+size:]
	}
	return hosts, token
}
