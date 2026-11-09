# ntfy-lb design

How ntfy-lb works inside, the invariants the code relies on, and how it was tested. User-facing
docs are in the [README](../README.md).

## Packages

| Package | Role |
|---|---|
| `config` | YAML config: parse, default, validate. `Parse` is all-or-nothing (unknown keys, null entries and non-positive durations are errors), so a reload with a broken file changes nothing. |
| `balancer` | The state: upstreams, their health, the open sessions (streams), drains. No HTTP serving. |
| `server` | Listeners (HTTP, HTTPS, admin), the proxy (`proxy.go`), the admin API, the access log, cert reloads. |
| `client` | Admin API client used by the CLI. |
| `cmd` | urfave/cli v3 commands. v3 because it parses flags after positional args (`upstream drain app2 -d 5m`) and root flags are persistent. |

## Routing (`balancer/hash.go`)

`rank(key, names)` scores every upstream name with `splitmix64(fnv1a(key || 0xff || name))` and
sorts by score (name as tie-break). The key is the client IP shortened to `hash.ipv4-prefix-bits`
(default 32) or `hash.ipv6-prefix-bits` (default 64), masked and cut to whole bytes
(`ClientKey`); with the defaults that is 4 or 8 bytes. `TestRank_Golden` pins the hash.
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

One probe round per interval over all upstreams and all their addresses in parallel,
`GET http://<address><path>`, new connection each time (keepalives off), 2xx = pass. An upstream
passes a round if any address passed, so load balancers that list a node's ports in a different
order agree on it (the lab soak found two LBs routing differently when only the first address was
probed and it was dead); an address that failed is skipped by address selection until it passes
again. Hysteresis on the upstream via `fails`/`passes` counters.
The very first probe of an upstream decides directly (`probed` flag), so a node that is down or
draining at startup or when added by a reload never gets traffic. `Start` runs one round
synchronously before the listeners serve; `Update` probes added upstreams, and upstreams whose
addresses changed, before swapping them in (health starts over). Each address set has a
generation; a probe result for a replaced generation is dropped. `Update` commits only if nothing
it relied on changed while it probed (e.g. a removed upstream forgotten by the drain loop), else
it probes again.

## Sessions and drains (`balancer/drain.go`)

Every proxied request is a `Session` (`NewSession` in `proxy.ServeHTTP`, `Close` when the request
ends, even on panic). `Reserve` picks an address of an upstream for an attempt and counts it
against that address right away (so a reconnect storm spreads over a node's ports); `Attach` turns
the reservation into a stream once the upstream answered, and `Close` or another `Reserve` releases
it. `RecordAddressFailure` backs a failed address off (1s, doubling to 30s); a successful `Attach`
clears it. A session attached to an upstream that was already forgotten is drained on the next
tick.
Draining = calling the session's `context.CancelCauseFunc` with `ErrDrained`, which aborts the
reverse proxy's copy (and closes a hijacked WebSocket via its backend-conn watcher).

`drainState` paces: each tick (1s) earns `(open - credit) * elapsed / remaining` closes as credit;
it closes `round(credit)` sessions, oldest first (lowest id), and carries the remainder. N streams
over D close at `(k - 0.5) * D / N`, i.e. evenly, never one-per-tick upfront and never bursting at the end
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
`/<topics>/{json,sse,raw,ws}` that does not poll, see `streams` in the config) with 503 +
`Connection: close`, but still serves
publishes and other short requests until it is drained, so publishers lose nothing.

Locking: `Balancer.mu` protects the maps, sessions, drains and each upstream's `removed`/`drain`;
`Upstream.mu` the addresses, per-address stream counts and health. Order: `Balancer.mu` before
`Upstream.mu`. `drainTick` collects victims under the lock and cancels them outside it.

## Proxy (`server/proxy.go`)

`httputil.ReverseProxy` with `proxy` as its `RoundTripper`. `RoundTrip` walks `Pick`'s candidates:

- request body wrapped in `trackingBody` (records whether any byte was read, ignores `Close`: the
  transport closes the body on error, which would make a retry impossible), and each attempt reads
  it through its own `attemptBody`. The transport may still read a failed attempt's body after
  `RoundTrip` returned, so before a retry the old handle is retired: it reads nothing more, and if
  it had read anything the retry is off. (Found by the Codex review: an early 503 could otherwise
  race the old writer for the body.)
- a dial error tries the upstream's other addresses first, so its clients stay on the node that
  holds their rate limits
- dial errors are wrapped in `dialError` by the transport's `DialContext`, so "never reached the
  node" is detectable and retried for any method
- other errors are retried for GET/HEAD/OPTIONS with an untouched body; a 503 with an untouched
  body is retried unless it is the last candidate (then it is passed on)
- the response body is wrapped in `idleTimeoutBody` (cancel after `upstream-read-timeout` without
  a byte); not for 101 responses, whose body must stay an `io.ReadWriteCloser`

`FlushInterval: -1` flushes every write. Reads from the client body and writes to the client have
an inactivity deadline of `upstream-read-timeout` (`clientBody`, `deadlineWriter`), and canceling
the request (drain) sets both deadlines to now, so a stalled upload or a client that stops reading
neither holds a handler forever nor survives a drain. `BufferPool` hands out 4 KiB copy buffers: a stream holds
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

`Shutdown(d)`: `Drain(d)`, wait until no session is open (or `d` + 2s), then cancel whatever is
still open (e.g. if the drain loop already stopped) and wait for those requests' finishers (access
log, metrics), then `http.Server.Shutdown` with a 5s grace. A failed `New` closes what it opened. The listeners keep accepting during the drain (subscriptions get 503, publishes are
served).

## Stream path (`server/stream.go`)

Subscriptions (`isStream`: GET `/<topics>/{json,sse,raw}`, not WebSocket upgrades) bypass
`ReverseProxy`, because they are idle almost all the time and there are many of them:

- **Upstream**: a raw TCP connection per stream; the request goes out as `HTTP/1.0`, so ntfy (which
  never looks at the protocol version) answers with a body that simply ends at connection close,
  without chunking, and the bytes are copied as they are. No `http.Transport` (two goroutines plus
  ~8 KiB of bufio per connection). Same retry rules as `RoundTrip`. Only bodyless GETs take this
  path. Client-supplied `Forwarded`/`X-Forwarded-Host`/`X-Forwarded-Proto` are dropped, like
  `Rewrite` does. Response headers are limited to 64 KiB, 1xx responses are skipped, and a
  response that is not close-delimited (chunked, Content-Length, 204/304) is decoded by its
  framing instead of copied raw.
- **Waiting**: `waitReadable` parks in the netpoller via `RawConn.Read` holding no buffer, then a
  pooled 4 KiB buffer is used for one read+write. It peeks a byte with `MSG_PEEK|MSG_DONTWAIT`
  first: the netpoller is edge-triggered, so it may only wait after seeing `EAGAIN`, or it could
  sleep on data that is already buffered. The read deadline (`upstream-read-timeout`) bounds the
  wait. A socket error seen by the peek (e.g. a reset) is returned there, since the peek consumes
  it and the next read would only see EOF; that made a reset look like a clean end until the tests
  caught it.
- **HTTP/1.1 clients**: the connection is hijacked, the response head written by hand with
  `Connection: close` (close-delimited body), and the copy runs in a new goroutine while the
  handler returns, so the server's per-connection state (bufio buffers, parsed request) is freed.
  The client is never read again, so a client that went away is noticed on the next write, at the
  latest ntfy's 45s keepalive message, or by TCP keepalive. `ServeHTTP` uses
  `context.WithoutCancel` for these, since the server cancels the request context when the handler
  returns. Canceling the request (drain) closes the client connection too, so a blocked write
  cannot outlive a drain; every write has a deadline of `upstream-read-timeout`.
- **HTTP/2 clients**: cannot be hijacked; written through the `ResponseWriter`. A client going away
  resets the stream, which cancels the context and closes the upstream connection. An upstream
  that breaks mid-stream (reset, read timeout, drain) resets the HTTP/2 stream
  (`http.ErrAbortHandler`) instead of ending it cleanly.

Because a detached stream outlives its handler, all per-request cleanup (session `Close`, context
cancel, the access log line) is registered as `proxyRequest` finishers and run by whoever ends the
request: the outermost handler (`finishUnlessDetached`), or the stream goroutine. Finishers must
not reference the `ResponseWriter` or the request (the access log copies what it needs into an
`accessLogEntry` upfront), or they pin the very server state the detach frees.

## Metrics (`server/metrics.go`)

Prometheus client on the admin listener's `/metrics`. Request outcomes are recorded by the same
finisher as the access log (`Server.observe`), so detached streams are counted when they end:

- `ntfy_lb_requests_total{upstream,code,kind}` (kind: request, stream, websocket; upstream empty
  if none answered; code 499 if the client left before a response)
- `ntfy_lb_request_duration_seconds{kind="request"}`, `ntfy_lb_stream_duration_seconds`
- `ntfy_lb_streams{protocol}` (http1, http2, websocket), `ntfy_lb_open_requests`
- `ntfy_lb_retries_total{reason}` (connect, error, 503), `ntfy_lb_refused_total`,
  `ntfy_lb_streams_closed_total{reason}` (drained, read_timeout)
- `ntfy_lb_ready`, `ntfy_lb_draining`, `ntfy_lb_upstream_{healthy,draining,streams,requests_total,failures_total}`
- the Go and process collectors (`go_*`, `process_*`: memory, goroutines, fds)

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

## Soak test (2026-11-05)

Three hours against real ntfy (3 unclustered nodes from main, SQLite cache tuned like ntfy.sh)
behind two load balancers, sized from lb-2's production traffic (~85k connections, ~720 req/s,
~430 polls/s, ~105 publishes/s) and scaled to the lab: 20,000 streams from 2,000 client IPs
(70% HTTP/2, 20% HTTP/1.1+TLS, 5% HTTP/1.1, 3% WebSocket, 2% SSE), clients that drop and
reconnect with `since=<last id>` (mean stream lifetime 30m, like the apps), 50 publishes/s
through the *other* load balancer, 100 polls/s, 10 bad requests/s. Every published message is
tracked until its subscriber receives it.

| Phase | Result |
|---|---|
| Steady state (first 20m) | 55,513 published, 55,513 delivered, 0 lost; latency p50 1.0ms, p99 1.8ms |
| Upstream drain app2 over 5m | 6,007 streams closed at a steady pace; 1,151 messages missed during the move, 1,006 of them recovered by replay |
| Reload removing app3 (5m drain), then re-adding it | streams moved as paced; no errors |
| Restart LB-a (2m shutdown drain) | all 20k streams drained and back after restart; new subscriptions got 503 during the drain (clients in the lab have no second LB to go to) |
| `kill -9` an ntfy node | 2 failed publishes, 0 failed subscriptions or polls; its clients moved to their next node right away (4,223 connect retries) |
| 20 reloads in a row | no effect on streams |
| Whole run | 531,878 published, 0 LB panics; RSS 921 -> 999 MiB (flattening), live heap ~350 MiB, goroutines 25.5k and fds 28.5k flat |

Messages that went missing (about 2,100 net of 531,878 at the end, still falling as replays
caught up) are all from node moves: the lab's ntfy nodes are not clustered, so while a client's
publishes already go to its new node, its open stream is still on the old one until the paced
drain (or, after an undrain, its next reconnect) moves it. Clustered ntfy, as in production,
delivers across nodes. After an undrain or a re-added node, clients go back to their home node
only as they reconnect, by design (an undrain never cuts streams).

Found by the soak: the cross-LB routing disagreement with a dead first port (now every address
is probed), and the HTTP/2 cancel crash (see Testing). Memory per stream with this mix is ~50
KiB RSS (~17 KiB live heap); the rest is GC headroom, which `GOMEMLIMIT` can trade for CPU.

## nginx comparison (2026-11-05)

The same load, 35 minutes each, against ntfy-lb and against nginx 1.28 running the production
LB config (`roles/nginx` with `nginx_lb`: `hash $binary_remote_addr`, HTTP/2, the same TLS and
proxy settings, access log on; without the abuse maps, `limit_req` and the website). One load
balancer at a time, publishing through the same one, fresh ntfy nodes for each run (app1 on one
VM, app2 and app3 on another). Load: 40,000 streams from 4,000 IPs with the production protocol
mix, 150 publishes/s (3x the soak, ~1.4x lb-2), 430 polls/s (lb-2), 30 bad requests/s. Memory is
PSS summed over the unit's processes, CPU is the unit's cgroup usage.

| | ntfy-lb | nginx |
|---|---|---|
| Published / lost | 292,568 / 0 | 292,880 / 0 |
| Errors (subscribe, publish, poll) | 0 | 0 |
| Delivery latency p50 / p99 | 1.1 / 6-8 ms | 1.1 / 3.6-4.0 ms |
| Publish request p50 / p99 | 1.1 / 5-7 ms | 1.1 / 3.5-4 ms |
| Poll request p50 / p99 | 1.1 / 5-7 ms | 1.0 / 3.0 ms |
| CPU, steady state | 0.75 cores | 0.35 cores |
| Memory at idle | 15 MiB | 634 MiB (6 workers; `worker_connections 250000` and the 200m TLS session cache) |
| Memory at 40k streams | 1.99 GiB | 2.04 GiB |
| Memory per stream | ~52 KB | ~37 KB |
| Extrapolated to 85k streams (lb-2) | ~4.4 GB | ~3.8 GB |

Where ntfy-lb's CPU goes (60s profile at 40k streams): scheduler and futex wakeups ~25%, write
syscalls ~13% (the HTTP/2 server flushes each frame on its own, nginx batches), GC ~11%, TLS
handshakes ~6%, and `drainTick` ~2%: it walks every session under the balancer lock once a
second even when nothing drains. Live heap is ~700 MB of the ~2 GB; the rest is GC headroom.
The 32 KB `io.Copy` buffers of WebSocket streams are ~70 MB of it.

Cheap wins, not done yet: skip the `drainTick` scan when nothing drains, pool or shrink the
WebSocket copy buffers, and set `GOMEMLIMIT` (less memory for more GC CPU).

## Testing

Unit and integration tests run against fake upstreams (`httptest` servers and raw TCP upstreams
for malformed or misbehaving responses), with the race detector (`make race`). Coverage
(2026-11-05): balancer 99%, config 100%, cmd 99%, client 95%, server 92%. Notable guards:

- `TestRank_Golden` pins the hash function (all load balancers must agree)
- `TestStream_OneGoroutinePerHTTP1Stream` pins the memory claim: a hijacked stream leaves one
  goroutine and no server or transport state
- `TestStream_NoGoroutineLeak`, `TestServer_NewFailureLeaksNothing`: every way a stream can end
- `TestBalancer_ConcurrencyStress`: sessions, drains, undrains, updates and health rounds at once
- `TestStream_UpstreamFraming`, `TestStream_UpstreamHeaderLimit`, `TestStream_HTTP2AbnormalEndIsAnError`:
  the hand-written HTTP of the stream path

### Codex review (2026-11-05)

An adversarial review by Codex found 21 issues; all are fixed, each with a test that failed first:

| # | Finding | Fix |
|---|---|---|
| 1 | `upstreams: [null]` panics (kills the process via SIGHUP) | rejected by validation |
| 2 | re-adding an upstream while its removal finishes corrupts the map/order | `Update` re-checks before commit |
| 3 | early-503 retry races the old attempt for the request body | per-attempt `attemptBody`, retired before a retry |
| 4 | stalled uploads hold handlers forever, even through drains | client read deadline, interrupted on cancel |
| 5 | a drain does not interrupt a blocked write to the client | client conn closed / write deadline on cancel |
| 6 | poll detection differs from ntfy (headers, aliases, precedence) | ntfy's readParam order, configurable |
| 7 | a session attached after its upstream was forgotten is never drained | drained on the next tick |
| 8 | GET with a body takes the bodyless stream path | only bodyless GETs are streams |
| 9 | HTTP/2 streams end cleanly after an upstream failure | reset (`ErrAbortHandler`) |
| 10 | stream path ignores response framing (chunked, Content-Length, 1xx) | decoded by framing |
| 11 | no limit on upstream response headers on the stream path | 64 KiB |
| 12 | least-loaded address ignores attempts still connecting | `Reserve` counts them |
| 13 | a failed secondary address keeps winning | per-address backoff; other addresses tried first |
| 14 | health survives address changes; stale probes apply to new addresses | probe on change, generations |
| 15 | negative durations and unknown YAML keys are accepted | validated, `KnownFields` |
| 16 | a failed reload can still swap the certificate | open everything, then apply |
| 17 | access-log path changes are ignored on reload | swappable log destination |
| 18 | stream path forwards client `Forwarded`/`X-Forwarded-Host/Proto` | dropped, like `Rewrite` |
| 19 | Shutdown leaves detached streams open if the drain loop stopped | `closeAll` after the drain |
| 20 | pacing closes 1 stream over 100s at 40s, not 50s | `(open - credit)` credit |
| 21 | a failed `New` leaks the access log | closed on failure |

Found by the new tests and the lab, beyond the review: a reset upstream connection looked like a
clean end (the `MSG_PEEK` consumed the socket error); load balancers listing a node's ports in a
different order disagreed on routing when the first port was dead (now every address is probed);
`check-config`/`status` could not reach a wildcard `listen-admin`; CLI flag state leaked between
app instances (tests only).

The lab soak (run 2) found a crash in the fix for finding 5: a drain's cancel callback set a
deadline on an HTTP/2 `ResponseWriter` whose handler had just returned, a nil dereference in the
standard library. The load balancer crash-looped, and every restart sent its 20k clients
reconnecting at once, until the OOM killer took down the (lab) ntfy nodes. Fixes:
`clientConn` serializes deadline changes with the handler's end
(`TestProxy_CancelRacesHandlerEnd` reproduced it 3/3); goroutines the load balancer starts
itself (detached streams, cancel callbacks) recover from panics (`safely`), since one panic there
otherwise costs every stream; and the systemd unit backs off restarts (5s growing to 5m) so a
crash loop cannot hammer the upstreams.
