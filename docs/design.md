# ntfy-lb design

How ntfy-lb works inside, the invariants the code relies on, and how it was tested. User-facing
docs are in the [README](../README.md).

## Packages

| Package | Role |
|---|---|
| `config` | YAML config: parse, default, validate. `Parse` is all-or-nothing, so a reload with a broken file changes nothing. |
| `balancer` | The state: upstreams, their health, the open sessions (streams), drains. No HTTP serving. |
| `server` | Listeners (HTTP, HTTPS, admin), the proxy (`proxy.go`), the admin API, the access log, cert reloads. |
| `client` | Admin API client used by the CLI. |
| `cmd` | urfave/cli v3 commands. v3 because it parses flags after positional args (`upstream drain app2 -d 5m`) and root flags are persistent. |

## Routing (`balancer/hash.go`)

`rank(key, names)` scores every upstream name with `splitmix64(fnv1a(key || 0xff || name))` and
sorts by score (name as tie-break). The key is the client IPv4 address or IPv6 /64 (`ClientKey`).
Properties the tests pin down: the order does not depend on config order or process; removing a
name only moves keys that ranked it first; adding one only moves keys to it; distribution is
even.

**Changing `score` re-routes every client** on the next request. Never change it without a reason
and a rolling plan; two load balancers with different hash functions disagree on every client.

`Balancer.Pick(key)` returns the ranked upstreams that are in the config, not draining, and
healthy. If none is healthy, it returns all non-draining ones (fail open, like lbagent). Only if
every upstream is draining does it return nothing (-> 503).

Existing streams never move because of routing changes: health changes, reloads and undrains only
affect new requests. Only drains close streams.

## Health (`balancer/health.go`)

One probe round per interval over all upstreams in parallel, `GET http://<first address><path>`,
new connection each time (keepalives off), 2xx = pass. Hysteresis via `fails`/`passes` counters.
The very first probe of an upstream decides directly (`probed` flag), so a node that is down or
draining at startup or when added by a reload never gets traffic. `Start` runs one round
synchronously before the listeners serve; `Update` probes added upstreams before swapping them in.

## Sessions and drains (`balancer/drain.go`)

Every proxied request is a `Session` (`NewSession` in `proxy.ServeHTTP`, `Close` when the handler
returns, even on panic). `Attach` binds it to an upstream address once the upstream answered.
Draining = calling the session's `context.CancelCauseFunc` with `ErrDrained`, which aborts the
reverse proxy's copy (and closes a hijacked WebSocket via its backend-conn watcher).

`drainState` paces: each tick (1s) earns `streams * elapsed / remaining` closes as credit; it closes
`round(credit)` sessions, oldest first (lowest id), and carries the remainder. N streams over D close
at about `(k - 0.5) * D / N`, i.e. evenly, never one-per-tick upfront and never bursting at the end
(as long as nothing new arrives, see below). At or after the deadline the tick closes everything
left and marks the drain `finished` (`Drained()`, state `drained`).

Drain sources:

- upstream drain by command (`DrainUpstream`), kept across reloads until `UndrainUpstream`
- upstream removed from the config (`removal` flag); re-adding it before it is gone cancels the
  drain. Once finished and empty it is deleted from the map.
- load balancer drain (`Drain`, also `Shutdown`); while it runs, the per-upstream drains wait

Invariant: **a draining thing gets no new streams**, otherwise pacing cannot converge (each tick
earns closes proportional to what is open, so arrivals push the close rate up hyperbolically
towards the deadline; seen in the lab as 67/s -> 1100/s). A draining upstream is excluded by
`Pick`. A draining load balancer turns away new *subscriptions* (`isSubscription`: GET
`/<topics>/{json,sse,raw,ws}` without `poll=1`) with 503 + `Connection: close`, but still serves
publishes and other short requests until it is drained, so publishers lose nothing.

Locking: `Balancer.mu` protects the maps, sessions, drains and each upstream's `removed`/`drain`;
`Upstream.mu` the addresses, per-address stream counts and health. Order: `Balancer.mu` before
`Upstream.mu`. `drainTick` collects victims under the lock and cancels them outside it.

## Proxy (`server/proxy.go`)

`httputil.ReverseProxy` with `proxy` as its `RoundTripper`. `RoundTrip` walks `Pick`'s candidates:

- request body wrapped in `trackingBody`: it records whether any byte was read and ignores `Close`
  (the transport closes the body on error, which would make a retry impossible; the server closes
  the real body when the handler returns)
- dial errors are wrapped in `dialError` by the transport's `DialContext`, so "never reached the
  node" is detectable and retried for any method
- other errors are retried for GET/HEAD/OPTIONS with an untouched body; a 503 with an untouched
  body is retried unless it is the last candidate (then it is passed on)
- the response body is wrapped in `idleTimeoutBody` (cancel after `upstream-read-timeout` without
  a byte); not for 101 responses, whose body must stay an `io.ReadWriteCloser`

`FlushInterval: -1` flushes every write. `BufferPool` hands out 4 KiB copy buffers: a stream holds
its buffer while it waits, and the default 32 KiB was half of a stream's heap. The transport is
swapped on reload (new dial timeout/keepalives), the old one only loses its idle connections.

`Rewrite` keeps `Host` and appends the client IP to `X-Forwarded-For` (nginx's
`$proxy_add_x_forwarded_for`; ntfy's `behind-proxy` reads it). `X-Forwarded-Host/Proto` and
`Forwarded` are dropped, as `Rewrite` does by default.

Copy errors (drained or abandoned streams) panic with `http.ErrAbortHandler` inside the reverse
proxy; the server swallows it. Anything that must run after the proxy (session `Close`, the access
log line) is deferred for that reason. `ReverseProxy.ErrorLog` goes to debug level, since every
drained stream logs there.

## Server (`server/server.go`)

`New` binds all listeners (errors surface before serving). `Serve` runs the first health round,
then serves. HTTP servers have only `ReadHeaderTimeout` (30s) and `IdleTimeout` (65s): no
read/write timeouts, as requests are long streams both ways. Client TCP keepalives are set on the
`net.ListenConfig`. HTTPS gets its certificate from an `atomic.Pointer` that `Reload` replaces.

`Shutdown(d)`: `Drain(d)`, wait until no session is open (or `d` + 5s), then `http.Server.Shutdown`
with a 5s grace. The listeners keep accepting during the drain (subscriptions get 503, publishes are
served).

## Stream path (`server/stream.go`)

Subscriptions (`isStream`: GET `/<topics>/{json,sse,raw}`, not WebSocket upgrades) bypass
`ReverseProxy`, because they are idle almost all the time and there are many of them:

- **Upstream**: a raw TCP connection per stream; the request goes out as `HTTP/1.0`, so ntfy (which
  never looks at the protocol version) answers with a body that simply ends at connection close,
  without chunking, and the bytes are copied as they are. No `http.Transport` (two goroutines plus
  ~8 KiB of bufio per connection). Same retry rules as `RoundTrip`.
- **Waiting**: `waitReadable` parks in the netpoller via `RawConn.Read` holding no buffer, then a
  pooled 4 KiB buffer is used for one read+write. It peeks a byte with `MSG_PEEK|MSG_DONTWAIT`
  first: the netpoller is edge-triggered, so it may only wait after seeing `EAGAIN`, or it could
  sleep on data that is already buffered. The read deadline (`upstream-read-timeout`) bounds the
  wait.
- **HTTP/1.1 clients**: the connection is hijacked, the response head written by hand with
  `Connection: close` (close-delimited body), and the copy runs in a new goroutine while the
  handler returns, so the server's per-connection state (bufio buffers, parsed request) is freed.
  The client is never read again, so a client that went away is noticed on the next write, at the
  latest ntfy's 45s keepalive message, or by TCP keepalive. `ServeHTTP` uses
  `context.WithoutCancel` for these, since the server cancels the request context when the handler
  returns.
- **HTTP/2 clients**: cannot be hijacked; written through the `ResponseWriter`. A client going away
  resets the stream, which cancels the context and closes the upstream connection.

Because a detached stream outlives its handler, all per-request cleanup (session `Close`, context
cancel, the access log line) is registered as `proxyRequest` finishers and run by whoever ends the
request: the outermost handler (`finishUnlessDetached`), or the stream goroutine. Finishers must
not reference the `ResponseWriter` or the request (the access log copies what it needs into an
`accessLogEntry` upfront), or they pin the very server state the detach frees.

## Memory

Measured in the lab (2026-11-04), 5000 idle JSON streams each, access log on, per stream:

| Client protocol | Before the stream path | Stream path | Heap / stack |
|---|---|---|---|
| HTTP/1.1 | 68 KiB RSS (4 goroutines) | **11 KiB** (1 goroutine) | 4.2 / 2.6 KiB |
| HTTP/1.1 + TLS | 91 KiB | **32 KiB** | 13.9 / 4.8 KiB (crypto/tls keeps ~6 KiB per connection) |
| HTTP/2 + TLS | 70 KiB | **37 KiB** | 16.4 / 11.7 KiB (the stdlib h2 server's per-stream writer and handler goroutine) |

On lb1 (2026-11-04) ~70% of subscription stream time is HTTP/2 (the Android app via OkHttp:
~90%), with ~3.4k client connections, so today's footprint is tens of MB. At 100k streams with
the same mix, expect ~3.5 GB. Turning off HTTP/2 on the LB would move the app to HTTP/1.1 over
TLS (~32 KiB) but costs a TCP+TLS connection per subscription request instead of multiplexing.

## Lab test (2026-11-04)

Proxmox VMs on box13: `ntfylb-up` (vmid 960, 3 independent ntfy nodes from main @ 05109571, app1
also on a second port via a NAT redirect) and `ntfylb-lb` (vmid 961, two ntfy-lb instances with the
same upstream names in a different order, HTTPS on one). A load generator opened 2000 JSON
subscriptions from 200 loopback source IPs (`127.0.x.y`) and published to each topic from the same
IP. The nodes are not clustered, so a message only arrives if subscribe and publish hit the same
node.

| Test | Result |
|---|---|
| Subscribe via LB a, publish via LB b | 1405/1405 delivered: both load balancers route every IP the same; requests spread 640/611/750 |
| `upstream drain app2 -d 30s` | 610 streams closed at a steady 20-21/s, clients reconnected to app1/app3, app2 `drained` with 0 streams |
| Reload removing app3 (`drain-duration: 1m`) | 12-13 closes/s for 60s, then app3 forgotten; re-adding it works; streams elsewhere untouched |
| `systemctl stop ntfy@app1` | 959 streams cut by ntfy; 693 reconnects that still hashed to app1 got connection refused and were retried transparently (0 client errors); app1 unhealthy after ~5s, healthy ~4s after restart |
| `systemctl restart` of an LB (30s drain) | `/v1/ready` 503 at once; 66-67 closes/s flat for 30s; new subscriptions 503, publishes served; exit after 30s; 2000 streams back within 5s |
| WebSocket and HTTP/2 streams | messages delivered; both cut by a drain |
| Access log | found and fixed: aborted streams were not logged (now deferred) |
| Stream path, 5000 streams over HTTP/1.1, HTTP/1.1+TLS, HTTP/2 | all messages delivered; memory as in the table above |
