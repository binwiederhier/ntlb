// Package match compiles and evaluates the match expressions of rules: boolean expressions in the
// expr language (github.com/expr-lang/expr) over a request's method, path, query, host, client IP
// and headers, e.g. method in ["GET", "POST"] && (ipIn(ip, "10.0.0.0/8") || path matches "^/x").
package match

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"sync"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/ast"
	"github.com/expr-lang/expr/vm"
)

// ipInFunc is ipIn(ip, network...): whether ip is in any of the networks (CIDR prefixes or single
// addresses). The networks must be string literals, so they are parsed once, at compile time.
const ipInFunc = "ipIn"

var vmPool = sync.Pool{
	New: func() any {
		return &vm.VM{}
	},
}

// Env is what an expression sees of a request
type Env struct {
	Method string                   `expr:"method"`
	Path   string                   `expr:"path"`  // Decoded, without the query
	Query  string                   `expr:"query"` // Raw query string
	Host   string                   `expr:"host"`
	IP     string                   `expr:"ip"`     // Client address; IPv4-mapped IPv6 as IPv4
	Header func(name string) string `expr:"header"` // First value of a header, "" if missing
}

// Program is a compiled match expression
type Program struct {
	program *vm.Program
}

// networks collects the ipIn networks of an expression while it compiles
type networks struct {
	prefixes map[string]netip.Prefix
	err      error
}

// Compile compiles and type-checks an expression, which must be boolean
func Compile(expression string) (*Program, error) {
	if strings.TrimSpace(expression) == "" {
		return nil, errors.New("empty expression")
	}
	nets := &networks{
		prefixes: make(map[string]netip.Prefix),
	}
	program, err := expr.Compile(expression,
		expr.Env(Env{}),
		expr.AsBool(),
		expr.Patch(nets),
		expr.Function(ipInFunc, nets.ipIn, new(func(string, ...string) bool)),
	)
	if err != nil {
		return nil, err
	} else if nets.err != nil {
		return nil, nets.err
	}
	return &Program{
		program: program,
	}, nil
}

// NewEnv takes what expressions see from a request, once for all rules
func NewEnv(r *http.Request) *Env {
	return &Env{
		Method: r.Method,
		Path:   r.URL.Path,
		Query:  r.URL.RawQuery,
		Host:   r.Host,
		IP:     clientIP(r.RemoteAddr),
		Header: r.Header.Get,
	}
}

// Match evaluates the expression; an expression that fails at runtime does not match
func (p *Program) Match(env *Env) bool {
	v := vmPool.Get().(*vm.VM)
	defer vmPool.Put(v)
	out, err := v.Run(p.program, env)
	if err != nil {
		return false
	}
	matched, _ := out.(bool)
	return matched
}

// Visit checks and parses the networks of every ipIn call; the first error fails the compile
func (n *networks) Visit(node *ast.Node) {
	call, ok := (*node).(*ast.CallNode)
	if !ok {
		return
	} else if callee, ok := call.Callee.(*ast.IdentifierNode); !ok || callee.Value != ipInFunc {
		return
	}
	if len(call.Arguments) < 2 {
		n.fail(fmt.Errorf("%s(ip, network...) needs at least one network", ipInFunc))
		return
	}
	for _, arg := range call.Arguments[1:] {
		s, ok := arg.(*ast.StringNode)
		if !ok {
			n.fail(fmt.Errorf("%s: networks must be string literals", ipInFunc))
			return
		}
		prefix, err := parsePrefix(s.Value)
		if err != nil {
			n.fail(err)
			return
		}
		n.prefixes[s.Value] = prefix
	}
}

func (n *networks) fail(err error) {
	if n.err == nil {
		n.err = err
	}
}

// ipIn is the runtime side of ipIn; the type checker guarantees string arguments
func (n *networks) ipIn(params ...any) (any, error) {
	addr, err := netip.ParseAddr(params[0].(string))
	if err != nil {
		return false, nil
	}
	for _, p := range params[1:] {
		if n.prefixes[p.(string)].Contains(addr) {
			return true, nil
		}
	}
	return false, nil
}

// parsePrefix parses a CIDR prefix, or a single address as a full-length prefix
func parsePrefix(s string) (netip.Prefix, error) {
	if strings.Contains(s, "/") {
		prefix, err := netip.ParsePrefix(s)
		if err != nil {
			return netip.Prefix{}, fmt.Errorf("invalid network %q: %w", s, err)
		}
		return prefix.Masked(), nil
	}
	addr, err := netip.ParseAddr(s)
	if err != nil {
		return netip.Prefix{}, fmt.Errorf("invalid network %q: %w", s, err)
	}
	return netip.PrefixFrom(addr, addr.BitLen()), nil
}

// clientIP is the address part of a remote address, IPv4-mapped IPv6 as IPv4, "" if unparseable
func clientIP(remoteAddr string) string {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return ""
	}
	addr, err := netip.ParseAddr(host)
	if err != nil {
		return ""
	} else if addr.Is4In6() {
		return addr.Unmap().String()
	}
	return host
}
