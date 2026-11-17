// Package clientkey is what identifies a client: its address, shortened to a prefix. Sticky
// routing (balance hash), the limits and bans, and the pending-connection caps all count a client
// by it, so they agree on who a client is.
package clientkey

import (
	"net/netip"
)

// Of returns the key of a client address: the IPv4 address shortened to
// ipv4PrefixBits, or the IPv6 address shortened to ipv6PrefixBits (with 64, a client rotating
// through its privacy addresses stays on the node holding its rate limits). The key is the
// masked prefix, cut to whole bytes; with the defaults of 32 and 64 it is 4 or 8 bytes.
func Of(addr netip.Addr, ipv4PrefixBits, ipv6PrefixBits int) []byte {
	addr = addr.Unmap()
	if !addr.IsValid() {
		return nil
	}
	bits := ipv6PrefixBits
	if addr.Is4() {
		bits = ipv4PrefixBits
	}
	prefix, err := addr.Prefix(bits)
	if err != nil {
		return addr.AsSlice()
	}
	return prefix.Addr().AsSlice()[:(bits+7)/8]
}

// Parse returns the address of an "ip:port" remote address (as net/http and net.Conn give it),
// IPv4-mapped IPv6 as IPv4; the zero Addr if it is none
func Parse(remoteAddr string) netip.Addr {
	ap, err := netip.ParseAddrPort(remoteAddr)
	if err != nil {
		return netip.Addr{}
	}
	return ap.Addr().Unmap()
}
