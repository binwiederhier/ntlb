// Package match compiles and evaluates the expressions of the config, in the expr language
// (github.com/expr-lang/expr): booleans over a request (rules, streams), booleans over a request
// and its outcome (response rules, access logs), and strings (redirect locations), e.g.
// method in ["GET", "POST"] && (ipIn(ip, "10.0.0.0/8") || path matches "^/x").
package match

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"reflect"
	"strings"
	"sync"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/ast"
	"github.com/expr-lang/expr/vm"
)

// ipInFunc is ipIn(ip, network...): whether ip is in any of the networks (CIDR prefixes or single
// addresses). The networks must be string literals, so they are parsed once, at compile time.
const (
	ipInFunc = "ipIn"
)

var (
	vmPool = sync.Pool{
		New: func() any {
			return &vm.VM{}
		},
	}
)

// Kind is what an expression is compiled for
type Kind int

// Expression kinds
const (
	Request  Kind = iota // A boolean over the request (Env)
	Response             // A boolean over the request and its outcome (ResponseEnv)
	String               // A string over the request (Env), e.g. a redirect location
)

// Env is what a request expression sees
type Env struct {
	Method string                     `expr:"method"`
	Path   string                     `expr:"path"`   // Decoded, without the query
	Query  string                     `expr:"query"`  // Raw query string
	URI    string                     `expr:"uri"`    // Path (escaped as sent) and query
	Host   string                     `expr:"host"`   // The Host header
	Scheme string                     `expr:"scheme"` // "http" or "https"
	IP     string                     `expr:"ip"`     // Client address; IPv4-mapped IPv6 as IPv4
	Site   string                     `expr:"site"`   // The site serving the host, "" if none does
	Header func(name string) string   `expr:"header"` // First value of a header, "" if missing
	Flag   func(names ...string) bool `expr:"flag"`   // See flag
}

// ResponseEnv is what a response expression sees when a request ended: the request without its
// headers (a stream may end hours later, and keeping its headers for that costs memory), and the
// outcome
type ResponseEnv struct {
	Method         string `expr:"method"`
	Path           string `expr:"path"`
	Query          string `expr:"query"`
	URI            string `expr:"uri"`
	Host           string `expr:"host"`
	Scheme         string `expr:"scheme"`
	IP             string `expr:"ip"`
	Site           string `expr:"site"`
	Status         int    `expr:"status"`          // Status sent to the client (499: it left before one)
	UpstreamStatus int    `expr:"upstream_status"` // Status of the upstream that answered, 0 if none did
	Rule           string `expr:"rule"`            // The rule that decided the request, "" if none did
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

// Compile compiles and type-checks an expression of a kind
func Compile(expression string, kind Kind) (*Program, error) {
	if strings.TrimSpace(expression) == "" {
		return nil, errors.New("empty expression")
	}
	nets := &networks{
		prefixes: make(map[string]netip.Prefix),
	}
	opts := []expr.Option{
		expr.Patch(nets),
		expr.Function(ipInFunc, nets.ipIn, new(func(string, ...string) bool)),
	}
	switch kind {
	case Request:
		opts = append(opts, expr.Env(Env{}), expr.AsBool())
	case Response:
		opts = append(opts, expr.Env(ResponseEnv{}), expr.AsBool())
	default:
		opts = append(opts, expr.Env(Env{}), expr.AsKind(reflect.String))
	}
	program, err := expr.Compile(expression, opts...)
	if err != nil {
		return nil, err
	} else if nets.err != nil {
		return nil, nets.err
	}
	return &Program{
		program: program,
	}, nil
}

// NewEnv takes what expressions see from a request, once for all expressions
func NewEnv(r *http.Request) *Env {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	return &Env{
		Method: r.Method,
		Path:   r.URL.Path,
		Query:  r.URL.RawQuery,
		URI:    r.URL.RequestURI(),
		Host:   r.Host,
		Scheme: scheme,
		IP:     clientIP(r.RemoteAddr),
		Header: r.Header.Get,
		Flag: func(names ...string) bool {
			return flag(r, names)
		},
	}
}

// NewResponseEnv takes the request part of a response expression's view, so that the request
// itself can be let go; the caller fills in the outcome when the request ends
func NewResponseEnv(env *Env) *ResponseEnv {
	return &ResponseEnv{
		Method: env.Method,
		Path:   env.Path,
		Query:  env.Query,
		URI:    env.URI,
		Host:   env.Host,
		Scheme: env.Scheme,
		IP:     env.IP,
		Site:   env.Site,
	}
}

// Match evaluates a boolean expression with an *Env or *ResponseEnv; an expression that fails at
// runtime does not match
func (p *Program) Match(env any) bool {
	matched, _ := p.run(env).(bool)
	return matched
}

// String evaluates a string expression; one that fails at runtime is ""
func (p *Program) String(env *Env) string {
	s, _ := p.run(env).(string)
	return s
}

func (p *Program) run(env any) any {
	v := vmPool.Get().(*vm.VM)
	defer vmPool.Put(v)
	out, err := v.Run(p.program, env)
	if err != nil {
		return nil
	}
	return out
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

// parsePrefix parses a CIDR prefix, or a single address as a full-length prefix. IPv4-mapped IPv6
// networks are refused: ip is always unmapped, so they would never match.
func parsePrefix(s string) (netip.Prefix, error) {
	var prefix netip.Prefix
	if strings.Contains(s, "/") {
		p, err := netip.ParsePrefix(s)
		if err != nil {
			return netip.Prefix{}, fmt.Errorf("invalid network %q: %w", s, err)
		}
		prefix = p.Masked()
	} else {
		addr, err := netip.ParseAddr(s)
		if err != nil {
			return netip.Prefix{}, fmt.Errorf("invalid network %q: %w", s, err)
		}
		prefix = netip.PrefixFrom(addr, addr.BitLen())
	}
	if prefix.Addr().Is4In6() {
		return netip.Prefix{}, fmt.Errorf("invalid network %q: use the IPv4 form, client IPs are never IPv4-mapped", s)
	}
	return prefix, nil
}

// flag is flag(name...), like ntfy's readParam for a boolean: the first of the names that is set
// as a header, else the first set as a query parameter, decides; it is true if 1, yes or true
func flag(r *http.Request, names []string) bool {
	for _, name := range names {
		if v := strings.TrimSpace(r.Header.Get(name)); v != "" {
			return isTrue(v)
		}
	}
	query := r.URL.Query()
	for _, name := range names {
		if v := strings.TrimSpace(query.Get(name)); v != "" {
			return isTrue(v)
		}
	}
	return false
}

func isTrue(s string) bool {
	switch strings.ToLower(s) {
	case "1", "yes", "true":
		return true
	}
	return false
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
