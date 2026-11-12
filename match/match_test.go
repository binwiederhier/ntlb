package match

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func testRequest(method, target, remoteAddr string, headers ...string) *http.Request {
	r := httptest.NewRequest(method, target, nil)
	r.RemoteAddr = remoteAddr
	for i := 0; i+1 < len(headers); i += 2 {
		r.Header.Add(headers[i], headers[i+1])
	}
	return r
}

func matches(t *testing.T, expression string, r *http.Request) bool {
	t.Helper()
	p, err := Compile(expression, Request)
	require.NoError(t, err)
	return p.Match(NewEnv(r))
}

func TestNewEnv(t *testing.T) {
	r := testRequest("POST", "http://ntfy.sh/%73p_x/json?since=10m&poll=1", "[::ffff:203.0.113.9]:1234", "User-Agent", "ntfy/1.0")
	env := NewEnv(r)
	require.Equal(t, "POST", env.Method)
	require.Equal(t, "/sp_x/json", env.Path) // Decoded
	require.Equal(t, "since=10m&poll=1", env.Query)
	require.Equal(t, "ntfy.sh", env.Host)
	require.Equal(t, "/%73p_x/json?since=10m&poll=1", env.URI) // As sent
	require.Equal(t, "http", env.Scheme)
	require.Equal(t, "203.0.113.9", env.IP) // Unmapped
	require.Equal(t, "ntfy/1.0", env.req.Header.Get("user-agent"))
	require.True(t, matches(t, `header("X-Missing") == ""`, r))

	require.Equal(t, "2001:db8::1", NewEnv(testRequest("GET", "/", "[2001:db8::1]:80")).IP)
	require.Equal(t, "192.0.2.1", NewEnv(testRequest("GET", "/", "192.0.2.1:80")).IP)
	require.Equal(t, "", NewEnv(testRequest("GET", "/", "garbage")).IP)
	require.Equal(t, "", NewEnv(testRequest("GET", "/", "localhost:80")).IP)
}

func TestMatch(t *testing.T) {
	ua := []string{"User-Agent", "ntfy/1.0"}
	tests := []struct {
		name       string
		expression string
		req        *http.Request
		want       bool
	}{
		{"method in", `method in ["GET", "POST"]`, testRequest("POST", "/", "192.0.2.1:1"), true},
		{"method not in", `method in ["GET", "POST"]`, testRequest("PUT", "/", "192.0.2.1:1"), false},
		{"path regex", `path matches "^/(sp_|spammy)"`, testRequest("GET", "/spammy/json", "192.0.2.1:1"), true},
		{"path regex near miss", `path matches "^/bad-bets($|/)"`, testRequest("GET", "/bad-betsy", "192.0.2.1:1"), false},
		{"path prefix", `path startsWith "/sp_"`, testRequest("GET", "/sp_x", "192.0.2.1:1"), true},
		{"query", `query contains "evil=1"`, testRequest("GET", "/x?evil=1", "192.0.2.1:1"), true},
		{"host", `host == "ntfy.sh"`, testRequest("GET", "http://ntfy.sh/x", "192.0.2.1:1"), true},
		{"ip equal", `ip == "192.0.2.1"`, testRequest("GET", "/", "192.0.2.1:1"), true},
		{"header, case-insensitive name and regex", `header("user-agent") matches "(?i)ntfy-spy"`, testRequest("GET", "/", "192.0.2.1:1", "User-Agent", "NTFY-Spy/2"), true},
		{"missing header is empty", `header("User-Agent") == ""`, testRequest("GET", "/", "192.0.2.1:1"), true},
		{"ipIn prefix", `ipIn(ip, "203.0.113.0/24")`, testRequest("GET", "/", "203.0.113.200:1"), true},
		{"ipIn several", `ipIn(ip, "10.0.0.0/8", "198.51.100.7")`, testRequest("GET", "/", "198.51.100.7:1"), true},
		{"ipIn neighbor", `ipIn(ip, "198.51.100.7")`, testRequest("GET", "/", "198.51.100.8:1"), false},
		{"ipIn ipv6", `ipIn(ip, "2001:db8::/48")`, testRequest("GET", "/", "[2001:db8:0:ffff::1]:1"), true},
		{"ipIn ipv4-mapped", `ipIn(ip, "203.0.113.0/24")`, testRequest("GET", "/", "[::ffff:203.0.113.9]:1"), true},
		{"ipIn unparseable address", `ipIn(ip, "0.0.0.0/0")`, testRequest("GET", "/", "garbage"), false},
		{"not", `!ipIn(ip, "10.0.0.0/8")`, testRequest("GET", "/", "192.0.2.1:1"), true},
		{
			"and/or",
			`method in ["GET", "POST"] && (ipIn(ip, "203.0.113.0/24", "198.51.100.7") || path matches "^/sp_")`,
			testRequest("GET", "/sp_x", "192.0.2.1:1", ua...),
			true,
		},
		{
			"and/or, method fails",
			`method in ["GET", "POST"] && (ipIn(ip, "203.0.113.0/24", "198.51.100.7") || path matches "^/sp_")`,
			testRequest("PUT", "/sp_x", "203.0.113.1:1", ua...),
			false,
		},
		{"topic count", `path matches "^/[^/]*(,[^/,]*){3,}"`, testRequest("GET", "/a,b,c,d/json", "192.0.2.1:1"), true},
		{"builtins", `lower(header("X-Test")) == "yes"`, testRequest("GET", "/", "192.0.2.1:1", "X-Test", "YES"), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, matches(t, tt.expression, tt.req))
		})
	}
}

func TestCompile_Invalid(t *testing.T) {
	for name, expression := range map[string]string{
		"syntax":                 `method ==`,
		"unknown identifier":     `user_agent == "x"`,
		"not boolean":            `path`,
		"bad regex":              `path matches "("`,
		"ipIn without networks":  `ipIn(ip)`,
		"ipIn non-literal":       `ipIn(ip, path)`,
		"ipIn bad network":       `ipIn(ip, "1.2.3.4/33")`,
		"ipIn bad address":       `ipIn(ip, "1.2.3")`,
		"ipIn not string":        `ipIn(ip, 5)`,
		"ipIn 4in6 network":      `ipIn(ip, "::ffff:10.0.0.0/104")`,
		"ipIn 4in6 address":      `ipIn(ip, "::ffff:10.0.0.1")`,
		"header without name":    `header() == ""`,
		"header wrong type":      `header(5) == ""`,
		"empty":                  ``,
		"assignment-like syntax": `method = "GET"`,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Compile(expression, Request)
			require.Error(t, err)
		})
	}
}

func TestMatch_RuntimeErrorDoesNotMatch(t *testing.T) {
	p, err := Compile(`len(path) % (len(path) - len(path)) == 0`, Request) // Modulo by zero
	require.NoError(t, err)
	require.False(t, p.Match(NewEnv(testRequest("GET", "/x", "192.0.2.1:1"))))
}

func TestMatch_ProgramsAreIndependent(t *testing.T) {
	// Each program has its own networks
	a, err := Compile(`ipIn(ip, "10.0.0.0/8")`, Request)
	require.NoError(t, err)
	b, err := Compile(`ipIn(ip, "192.0.2.0/24")`, Request)
	require.NoError(t, err)
	env := NewEnv(testRequest("GET", "/", "10.1.2.3:1"))
	require.True(t, a.Match(env))
	require.False(t, b.Match(env))
}

func BenchmarkMatch(b *testing.B) {
	p, err := Compile(`method in ["GET", "POST"] && (ipIn(ip, "203.0.113.0/24", "198.51.100.7", "2001:db8::/48") || path matches "^/(sp_|spammy|bad-bets($|/))")`, Request)
	require.NoError(b, err)
	env := NewEnv(testRequest("GET", "/mytopic/json", "192.0.2.1:1", "User-Agent", "ntfy/1.0"))
	b.ReportAllocs()
	for b.Loop() {
		if p.Match(env) {
			b.Fatal("unexpected match")
		}
	}
}

func TestNewEnv_Scheme(t *testing.T) {
	r := testRequest("GET", "https://ntfy.sh/x", "192.0.2.1:1")
	require.NotNil(t, r.TLS)
	require.Equal(t, "https", NewEnv(r).Scheme)
}

func TestFlag(t *testing.T) {
	// Like ntfy's readParam: headers first, then the query, each in list order; the first one set
	// decides, and it is true if it is 1, yes or true
	tests := []struct {
		name    string
		target  string
		headers []string
		want    bool
	}{
		{"none set", "/x/json", nil, false},
		{"query", "/x/json?poll=1", nil, true},
		{"query short", "/x/json?po=yes", nil, true},
		{"query false", "/x/json?poll=0", nil, false},
		{"query first name wins", "/x/json?poll=0&po=1", nil, false},
		{"header", "/x/json", []string{"X-Poll", "true"}, true},
		{"header before query", "/x/json?poll=1", []string{"X-Poll", "no"}, false},
		{"case-insensitive value", "/x/json?poll=TRUE", nil, true},
		{"whitespace", "/x/json?poll=%201%20", nil, true},
		{"empty value is unset", "/x/json?poll=&po=1", nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := testRequest("GET", tt.target, "192.0.2.1:1", tt.headers...)
			require.Equal(t, tt.want, matches(t, `flag("x-poll", "poll", "po")`, r))
		})
	}
}

func TestCompile_Kinds(t *testing.T) {
	// Response expressions see the outcome, but no headers: they run when a request ends, which for
	// a stream is long after the request could have been let go
	_, err := Compile(`status >= 400 && upstream_status == 0 && rule == "" && path startsWith "/x" && ip != ""`, Response)
	require.NoError(t, err)
	_, err = Compile(`header("User-Agent") == ""`, Response)
	require.Error(t, err)
	_, err = Compile(`flag("poll")`, Response)
	require.Error(t, err)
	_, err = Compile(`status == 200`, Request)
	require.Error(t, err)

	// Strings, for redirect locations
	p, err := Compile(`"https://docs.ntfy.sh/" + trimPrefix(path, "/docs/")`, String)
	require.NoError(t, err)
	require.Equal(t, "https://docs.ntfy.sh/publish/", p.String(NewEnv(testRequest("GET", "/docs/publish/", "192.0.2.1:1"))))
	_, err = Compile(`path == "/"`, String)
	require.Error(t, err)
	_, err = Compile(`"x"`, Request)
	require.Error(t, err)
}

func TestResponseEnv(t *testing.T) {
	r := testRequest("POST", "https://ntfy.sh/mytopic?x=1", "192.0.2.1:1")
	env := NewResponseEnv(NewEnv(r))
	env.Status, env.UpstreamStatus, env.Rule = 503, 0, "limit"
	require.Equal(t, "/mytopic", env.Path)
	require.Equal(t, "https", env.Scheme)
	p, err := Compile(`status == 503 && upstream_status == 0 && rule == "limit" && method == "POST" && query == "x=1" && host == "ntfy.sh" && uri == "/mytopic?x=1"`, Response)
	require.NoError(t, err)
	require.True(t, p.Match(env))
}

func TestString_RuntimeError(t *testing.T) {
	p, err := Compile(`string(len(path) % (len(path) - len(path)))`, String)
	require.NoError(t, err)
	require.Equal(t, "", p.String(NewEnv(testRequest("GET", "/x", "192.0.2.1:1"))))
}

func TestSite(t *testing.T) {
	// The site is set once the request's site is known, and carries over to the response
	env := NewEnv(testRequest("GET", "/x", "192.0.2.1:1"))
	env.Site = "ntfy"
	p, err := Compile(`site == "ntfy"`, Request)
	require.NoError(t, err)
	require.True(t, p.Match(env))
	p, err = Compile(`site == "ntfy"`, Response)
	require.NoError(t, err)
	require.True(t, p.Match(NewResponseEnv(env)))
}

func FuzzCompile(f *testing.F) {
	// An expression either compiles or is an error, and a compiled one never panics on a request
	f.Add(`method in ["GET", "POST"] && (ipIn(ip, "10.0.0.0/8") || path matches "^/x")`)
	f.Add(`"https://" + host + uri`)
	f.Add(`int(header("Content-Length")) > 10 && flag("poll", "po")`)
	r := testRequest("POST", "/a,b/json?poll=1&x=%20", "[2001:db8::1]:1", "User-Agent", "x", "Content-Length", "abc")
	env := NewEnv(r)
	f.Fuzz(func(t *testing.T, expression string) {
		for _, kind := range []Kind{Request, Response, String} {
			p, err := Compile(expression, kind)
			if err != nil {
				continue
			}
			switch kind {
			case Request:
				p.Match(env)
			case Response:
				p.Match(NewResponseEnv(env))
			default:
				p.String(env)
			}
		}
	})
}

func BenchmarkMatch_HeaderAndFlag(b *testing.B) {
	// ntfy's streams match and abuse rule call header() and flag() on every request
	p, err := Compile(`header("User-Agent") matches "(?i)ntfy-spy" || (method == "GET" && !flag("x-poll", "poll", "po"))`, Request)
	require.NoError(b, err)
	r := testRequest("GET", "/mytopic/json?since=10m", "192.0.2.1:1", "User-Agent", "ntfy/1.21")
	for b.Loop() {
		if !p.Match(NewEnv(r)) { // A new env per request, like the server
			b.Fatal("expected a match")
		}
	}
}
