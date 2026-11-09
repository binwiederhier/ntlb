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
	p, err := Compile(expression)
	require.NoError(t, err)
	return p.Match(NewEnv(r))
}

func TestNewEnv(t *testing.T) {
	r := testRequest("POST", "http://ntfy.sh/%6Ez_x/json?since=10m&poll=1", "[::ffff:203.0.113.9]:1234", "User-Agent", "ntfy/1.0")
	env := NewEnv(r)
	require.Equal(t, "POST", env.Method)
	require.Equal(t, "/nz_x/json", env.Path) // Decoded
	require.Equal(t, "since=10m&poll=1", env.Query)
	require.Equal(t, "ntfy.sh", env.Host)
	require.Equal(t, "203.0.113.9", env.IP) // Unmapped
	require.Equal(t, "ntfy/1.0", env.Header("user-agent"))
	require.Equal(t, "", env.Header("X-Missing"))

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
		{"path regex", `path matches "^/(nz_|nezha)"`, testRequest("GET", "/nezha/json", "192.0.2.1:1"), true},
		{"path regex near miss", `path matches "^/my-bets($|/)"`, testRequest("GET", "/my-betsy", "192.0.2.1:1"), false},
		{"path prefix", `path startsWith "/nz_"`, testRequest("GET", "/nz_x", "192.0.2.1:1"), true},
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
			`method in ["GET", "POST"] && (ipIn(ip, "203.0.113.0/24", "198.51.100.7") || path matches "^/nz_")`,
			testRequest("GET", "/nz_x", "192.0.2.1:1", ua...),
			true,
		},
		{
			"and/or, method fails",
			`method in ["GET", "POST"] && (ipIn(ip, "203.0.113.0/24", "198.51.100.7") || path matches "^/nz_")`,
			testRequest("PUT", "/nz_x", "203.0.113.1:1", ua...),
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
		"header without name":    `header() == ""`,
		"header wrong type":      `header(5) == ""`,
		"empty":                  ``,
		"assignment-like syntax": `method = "GET"`,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Compile(expression)
			require.Error(t, err)
		})
	}
}

func TestMatch_RuntimeErrorDoesNotMatch(t *testing.T) {
	p, err := Compile(`len(path) % (len(path) - len(path)) == 0`) // Modulo by zero
	require.NoError(t, err)
	require.False(t, p.Match(NewEnv(testRequest("GET", "/x", "192.0.2.1:1"))))
}

func TestMatch_ProgramsAreIndependent(t *testing.T) {
	// Each program has its own networks
	a, err := Compile(`ipIn(ip, "10.0.0.0/8")`)
	require.NoError(t, err)
	b, err := Compile(`ipIn(ip, "192.0.2.0/24")`)
	require.NoError(t, err)
	env := NewEnv(testRequest("GET", "/", "10.1.2.3:1"))
	require.True(t, a.Match(env))
	require.False(t, b.Match(env))
}

func BenchmarkMatch(b *testing.B) {
	p, err := Compile(`method in ["GET", "POST"] && (ipIn(ip, "203.0.113.0/24", "198.51.100.7", "2001:db8::/48") || path matches "^/(nz_|nezha|my-bets($|/))")`)
	require.NoError(b, err)
	env := NewEnv(testRequest("GET", "/mytopic/json", "192.0.2.1:1", "User-Agent", "ntfy/1.0"))
	b.ReportAllocs()
	for b.Loop() {
		if p.Match(env) {
			b.Fatal("unexpected match")
		}
	}
}
