// replay sends the same requests to two load balancers (ntlb and nginx) and compares the answers:
// status, Location, and for streams the first bytes. Request shapes come from the ntfy.sh access
// logs (aggregated) and the ntfy clients' code. Run it on the LB VM against localhost.
package main

import (
	"bufio"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

type target struct {
	name, http, https string
}

type result struct {
	status string // "200", "101", "closed", "timeout", ...
	extra  string // Location, first stream bytes, time to close
}

type testCase struct {
	name string
	run  func(t target) result
}

var (
	tlsConfig = &tls.Config{InsecureSkipVerify: true}
	longWait  = flag.Duration("slow", 75*time.Second, "how long the slow-request cases may take")
)

// raw sends a raw request over HTTP/1.x (TLS if https) and reads one response
func raw(addr string, useTLS bool, request string, readFor time.Duration) result {
	var conn net.Conn
	var err error
	if useTLS {
		conn, err = tls.Dial("tcp", addr, &tls.Config{InsecureSkipVerify: true, NextProtos: []string{"http/1.1"}})
	} else {
		conn, err = net.Dial("tcp", addr)
	}
	if err != nil {
		return result{status: "dial-error"}
	}
	defer conn.Close()
	conn.Write([]byte(request))
	conn.SetReadDeadline(time.Now().Add(readFor))
	r := bufio.NewReader(conn)
	resp, err := http.ReadResponse(r, nil)
	for err == nil && resp.StatusCode == 100 {
		resp, err = http.ReadResponse(r, nil)
	}
	if err != nil {
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			return result{status: "timeout"}
		}
		return result{status: "closed"}
	}
	defer resp.Body.Close()
	extra := resp.Header.Get("Location")
	if resp.StatusCode == 200 && (strings.Contains(request, "/json ") || strings.Contains(request, "/sse ") || strings.Contains(request, "/raw ")) && !strings.Contains(request, "poll=1") {
		buf := make([]byte, 64)
		n, _ := io.ReadAtLeast(resp.Body, buf, 1)
		extra = firstEvent(string(buf[:n]))
	}
	return result{status: fmt.Sprint(resp.StatusCode), extra: extra}
}

// firstEvent reduces a stream's first bytes to what is comparable across nodes
func firstEvent(s string) string {
	switch {
	case strings.Contains(s, `"event":"open"`):
		return "open-event"
	case strings.HasPrefix(s, "id:") || strings.HasPrefix(s, "event: open"):
		return "sse-open"
	case s == "\n" || strings.HasPrefix(s, "\n"):
		return "raw-newline"
	}
	return fmt.Sprintf("%.20q", s)
}

func h2(addr, method, path string, header map[string]string, body string) result {
	tr := &http.Transport{TLSClientConfig: tlsConfig, ForceAttemptHTTP2: true}
	defer tr.CloseIdleConnections()
	client := &http.Client{Transport: tr, Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	var b io.Reader
	if body != "" {
		b = strings.NewReader(body)
	}
	req, _ := http.NewRequest(method, "https://"+addr+path, b)
	req.Host = "ntfy.sh"
	for k, v := range header {
		req.Header.Set(k, v)
	}
	resp, err := client.Do(req)
	if err != nil {
		return result{status: "error"}
	}
	defer resp.Body.Close()
	extra := resp.Header.Get("Location")
	if resp.Proto != "HTTP/2.0" {
		extra += " NOT-H2:" + resp.Proto
	}
	if resp.StatusCode == 200 && method == "GET" && (strings.HasSuffix(path, "/json") || strings.HasSuffix(path, "/sse") || strings.HasSuffix(path, "/raw")) {
		buf := make([]byte, 64)
		n, _ := io.ReadAtLeast(resp.Body, buf, 1)
		extra += firstEvent(string(buf[:n]))
	}
	return result{status: fmt.Sprint(resp.StatusCode), extra: extra}
}

// slow sends the request's first part, then waits; it reports when the server gave up
func slow(addr, first string, trickle string, every time.Duration) result {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return result{status: "dial-error"}
	}
	defer conn.Close()
	start := time.Now()
	conn.Write([]byte(first))
	done := make(chan string, 1)
	go func() {
		r := bufio.NewReader(conn)
		line, err := r.ReadString('\n')
		if err != nil {
			done <- "closed"
			return
		}
		done <- strings.TrimSpace(line)
	}()
	tick := time.NewTicker(every)
	defer tick.Stop()
	deadline := time.After(*longWait)
	for {
		select {
		case line := <-done:
			return result{status: shortStatus(line), extra: fmt.Sprintf("after ~%ds", int(time.Since(start).Seconds()/10)*10)}
		case <-deadline:
			return result{status: "still-open", extra: fmt.Sprintf(">%s", *longWait)}
		case <-tick.C:
			if trickle != "" {
				conn.Write([]byte(trickle))
			}
		}
	}
}

func shortStatus(line string) string {
	if f := strings.Fields(line); len(f) >= 2 && strings.HasPrefix(f[0], "HTTP/") {
		return f[1]
	}
	return line
}

func h1(path string, extraHeaders ...string) string {
	return "GET " + path + " HTTP/1.1\r\nHost: ntfy.sh\r\n" + strings.Join(extraHeaders, "") + "Connection: close\r\n\r\n"
}

func main() {
	flag.Parse()
	targets := []target{{"ntlb", "127.0.0.1:8080", "127.0.0.1:8443"}, {"nginx", "127.0.0.1:9080", "127.0.0.1:9443"}}
	topics := func(n int) string {
		t := make([]string, n)
		for i := range t {
			t[i] = fmt.Sprintf("up%012d", i)
		}
		return strings.Join(t, ",")
	}
	body := strings.Repeat("x", 300)
	cases := []testCase{}
	add := func(name string, f func(t target) result) {
		cases = append(cases, testCase{name, f})
	}
	// Polls (most lines in the access log), every since form
	for _, since := range []string{"all", "none", "latest", "10m", "1762750000", "abcdefghijkl", "invalid!"} {
		since := since
		add("poll since="+since, func(t target) result {
			return raw(t.http, false, h1("/rtopic/json?poll=1&since="+since), 5*time.Second)
		})
	}
	add("poll x-poll header", func(t target) result {
		return raw(t.http, false, h1("/rtopic/json", "X-Poll: 1\r\n"), 5*time.Second)
	})
	add("poll po=1", func(t target) result {
		return raw(t.http, false, h1("/rtopic/json?po=1"), 5*time.Second)
	})
	add("poll 140 topics", func(t target) result {
		return raw(t.http, false, h1("/"+topics(140)+"/json?poll=1"), 5*time.Second)
	})
	add("poll 160 topics (abuse)", func(t target) result {
		return raw(t.http, false, h1("/"+topics(160)+"/json?poll=1"), 5*time.Second)
	})
	add("poll iOS one message", func(t target) result {
		return raw(t.https, true, h1("/rtopic/json?poll=1&id=abcdefghijkl", "User-Agent: ntfy/1.7.0 (build 9; iOS 26.6.2)\r\n"), 5*time.Second)
	})
	// Streams
	for _, kind := range []string{"json", "sse", "raw"} {
		kind := kind
		add("stream h1 /"+kind, func(t target) result {
			return raw(t.http, false, h1("/rtopic/"+kind), 5*time.Second)
		})
		add("stream h1+tls /"+kind, func(t target) result {
			return raw(t.https, true, h1("/rtopic/"+kind), 5*time.Second)
		})
		add("stream h2 /"+kind, func(t target) result {
			return h2(t.https, "GET", "/rtopic/"+kind, nil, "")
		})
	}
	add("stream android multi-topic h2", func(t target) result {
		return h2(t.https, "GET", "/"+topics(30)+"/json", map[string]string{"User-Agent": "ntfy/1.25.2 (play; Android 17; SDK 37)"}, "")
	})
	add("websocket h1", func(t target) result {
		return raw(t.http, false, "GET /rtopic/ws HTTP/1.1\r\nHost: ntfy.sh\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n", 5*time.Second)
	})
	add("websocket h1+tls", func(t target) result {
		return raw(t.https, true, "GET /rtopic/ws HTTP/1.1\r\nHost: ntfy.sh\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n", 5*time.Second)
	})
	// Publishes
	add("publish POST h1", func(t target) result {
		return raw(t.http, false, "POST /rtopic HTTP/1.1\r\nHost: ntfy.sh\r\nContent-Length: 300\r\nConnection: close\r\n\r\n"+body, 5*time.Second)
	})
	add("publish PUT query options h2", func(t target) result {
		return h2(t.https, "PUT", "/rtopic?title=Hi%20there&priority=high&tags=warning&message="+strings.Repeat("m", 3000), nil, "")
	})
	add("publish chunked (CLI)", func(t target) result {
		return raw(t.http, false, "PUT /rtopic HTTP/1.1\r\nHost: ntfy.sh\r\nTransfer-Encoding: chunked\r\nFilename: a.txt\r\nConnection: close\r\n\r\n5\r\nhello\r\n0\r\n\r\n", 5*time.Second)
	})
	add("publish expect 100-continue", func(t target) result {
		return raw(t.http, false, "POST /rtopic HTTP/1.1\r\nHost: ntfy.sh\r\nContent-Length: 5\r\nExpect: 100-continue\r\nConnection: close\r\n\r\nhello", 5*time.Second)
	})
	add("publish via GET /publish", func(t target) result {
		return raw(t.http, false, h1("/rtopic/publish?message=hi"), 5*time.Second)
	})
	add("publish 20 MB (ntfy 413)", func(t target) result {
		return raw(t.http, false, "PUT /rtopic HTTP/1.1\r\nHost: ntfy.sh\r\nContent-Length: 20000000\r\nConnection: close\r\n\r\n"+strings.Repeat("a", 20000000), 20*time.Second)
	})
	add("unifiedpush discovery", func(t target) result {
		return raw(t.http, false, h1("/upAbCdEfGhIjKl?up=1"), 5*time.Second)
	})
	add("unifiedpush aes128gcm", func(t target) result {
		return raw(t.https, true, "POST /upAbCdEfGhIjKl HTTP/1.1\r\nHost: ntfy.sh\r\nContent-Encoding: aes128gcm\r\nContent-Length: 4\r\nConnection: close\r\n\r\n\x00\x01\xfe\xff", 5*time.Second)
	})
	add("matrix push gateway", func(t target) result {
		return h2(t.https, "POST", "/_matrix/push/v1/notify", map[string]string{"Content-Type": "application/json"}, `{"notification":{"devices":[{"pushkey":"https://ntfy.sh/upAbCdEfGhIjKl?up=1"}]}}`)
	})
	add("matrix GET", func(t target) result {
		return h2(t.https, "GET", "/_matrix/push/v1/notify", nil, "")
	})
	// Web app and account API
	add("config", func(t target) result {
		return h2(t.https, "GET", "/v1/config", nil, "")
	})
	add("health", func(t target) result {
		return h2(t.https, "GET", "/v1/health", nil, "")
	})
	add("account no auth", func(t target) result {
		return h2(t.https, "GET", "/v1/account", nil, "")
	})
	add("account bad bearer", func(t target) result {
		return h2(t.https, "GET", "/v1/account", map[string]string{"Authorization": "Bearer tk_invalid"}, "")
	})
	add("token PATCH no auth", func(t target) result {
		return h2(t.https, "PATCH", "/v1/account/token", nil, "")
	})
	add("webpush DELETE with body", func(t target) result {
		return h2(t.https, "DELETE", "/v1/webpush", nil, `{"endpoint":"x"}`)
	})
	add("cors preflight", func(t target) result {
		return h2(t.https, "OPTIONS", "/rtopic", map[string]string{"Origin": "https://example.com", "Access-Control-Request-Method": "PUT"}, "")
	})
	add("HEAD /", func(t target) result {
		return raw(t.https, true, "HEAD / HTTP/1.1\r\nHost: ntfy.sh\r\nConnection: close\r\n\r\n", 5*time.Second)
	})
	add("HEAD /topic", func(t target) result {
		return raw(t.http, false, "HEAD /rtopic HTTP/1.1\r\nHost: ntfy.sh\r\nConnection: close\r\n\r\n", 5*time.Second)
	})
	add("file missing", func(t target) result {
		return h2(t.https, "GET", "/file/abcdefghijkl.png", nil, "")
	})
	add("auth query ws (web app)", func(t target) result {
		return raw(t.https, true, "GET /rtopic/ws?auth=QmFzaWMgZm9vOmJhcg HTTP/1.1\r\nHost: ntfy.sh\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n", 5*time.Second)
	})
	// Website and redirects
	add("http GET / redirect", func(t target) result {
		return raw(t.http, false, h1("/"), 5*time.Second)
	})
	add("https GET / website", func(t target) result {
		return raw(t.https, true, h1("/"), 5*time.Second)
	})
	add("docs redirect", func(t target) result {
		return raw(t.https, true, h1("/docs/publish/"), 5*time.Second)
	})
	add("docs-xxx topic not redirected", func(t target) result {
		return raw(t.https, true, h1("/docs-abc/json?poll=1"), 5*time.Second)
	})
	// Abuse rules
	add("abuse topic", func(t target) result {
		return raw(t.http, false, h1("/nz_abc/json?poll=1"), 5*time.Second)
	})
	add("abuse user agent", func(t target) result {
		return raw(t.http, false, h1("/rtopic/json?poll=1", "User-Agent: ntfy-spy/2\r\n"), 5*time.Second)
	})
	// Junk, as in nginx's access_error_400.log on ntfy.sh
	for name, junk := range map[string]string{
		"junk TLS on HTTP port":    "\x16\x03\x01\x00{\x01\x00\x00w\x03\x03\xef\xa4aSz\r\n\r\n",
		"junk binary":              "\x80\x13L7\x8f$\x1a\x13\xda\x82\r\n\r\n",
		"junk spaces in target":    "GET Kvissle&message=Temperaturen 14.1 C&priority=high HTTP/1.1\r\nHost: ntfy.sh\r\n\r\n",
		"junk header as line":      "Connection: close\r\n\r\n",
		"junk php probe":           "GET /index.php?file_get_contents(\"x\") HTTP/1.1\r\nHost: ntfy.sh\r\n\r\n",
		"junk no host":             "GET /rtopic/json?poll=1 HTTP/1.1\r\n\r\n",
		"junk two content-lengths": "POST /rtopic HTTP/1.1\r\nHost: ntfy.sh\r\nContent-Length: 1\r\nContent-Length: 2\r\n\r\nab",
		"junk http/1.0":            "GET /rtopic/json?poll=1 HTTP/1.0\r\n\r\n",
		"junk absolute form":       "GET http://ntfy.sh/rtopic/json?poll=1 HTTP/1.1\r\nHost: ntfy.sh\r\nConnection: close\r\n\r\n",
		"junk header 10k":          "GET /rtopic/json?poll=1 HTTP/1.1\r\nHost: ntfy.sh\r\nX-Big: " + strings.Repeat("a", 10000) + "\r\nConnection: close\r\n\r\n",
		"junk header 40k":          "GET /rtopic/json?poll=1 HTTP/1.1\r\nHost: ntfy.sh\r\nX-Big: " + strings.Repeat("a", 40000) + "\r\nConnection: close\r\n\r\n",
		"junk header 70k":          "GET /rtopic/json?poll=1 HTTP/1.1\r\nHost: ntfy.sh\r\nX-Big: " + strings.Repeat("a", 70000) + "\r\nConnection: close\r\n\r\n",
		"junk url 20k":             "GET /rtopic?message=" + strings.Repeat("a", 20000) + " HTTP/1.1\r\nHost: ntfy.sh\r\nConnection: close\r\n\r\n",
		"junk 200 headers":         "GET /rtopic/json?poll=1 HTTP/1.1\r\nHost: ntfy.sh\r\n" + strings.Repeat("X-A: b\r\n", 200) + "Connection: close\r\n\r\n",
	} {
		junk := junk
		add(name, func(t target) result {
			return raw(t.http, false, junk, 5*time.Second)
		})
	}
	// Slow clients: headers never finished, a body trickling in, a body that stops
	add("slow headers (slowloris)", func(t target) result {
		return slow(t.http, "GET /rtopic/json?poll=1 HTTP/1.1\r\nHost: ntfy.sh\r\n", "X-A: b\r\n", 10*time.Second)
	})
	add("slow body stalls", func(t target) result {
		return slow(t.http, "POST /rtopic HTTP/1.1\r\nHost: ntfy.sh\r\nContent-Length: 100\r\n\r\nab", "", time.Hour)
	})

	// Run each case against both, in parallel per case
	results := make(map[string][2]result)
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, 8)
	for _, c := range cases {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			var r [2]result
			for i, t := range targets {
				r[i] = c.run(t)
			}
			mu.Lock()
			results[c.name] = r
			mu.Unlock()
		}()
	}
	wg.Wait()
	names := make([]string, 0, len(results))
	for n := range results {
		names = append(names, n)
	}
	sort.Strings(names)
	same := 0
	for _, n := range names {
		r := results[n]
		mark := "DIFF"
		if r[0] == r[1] {
			mark, same = "same", same+1
		}
		fmt.Printf("%-4s %-34.34s ntlb=%-6s %-22.22s nginx=%-6s %.22s\n", mark, n, r[0].status, r[0].extra, r[1].status, r[1].extra)
	}
	fmt.Printf("\n%d of %d the same\n", same, len(names))
	os.Exit(0)
}
