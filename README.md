# ntfy-lb

A small load balancer built for [ntfy](https://ntfy.sh): sticky routing by client IP that every
load balancer instance agrees on, active health checks, long-lived streams (JSON/SSE/raw, WebSocket,
HTTP/2), streaming bodies, and **paced** draining, so that taking a node or a load balancer out of
service never makes tens of thousands of clients reconnect at the same second.

It is built for ntfy, but nothing in it is ntfy-only: which requests are long-lived streams
(`streams` in the config; default: ntfy's subscriptions), the health check path and the client
key (`hash`) are configurable, so it fits any HTTP backend with long-lived streams (SSE, long
polling, WebSockets).

It is meant to replace the nginx + lbagent pair on the ntfy.sh load balancers (see
`ntfy/ansible/roles/nginx` and `roles/lbagent`). What it does and does not take over from that
setup is listed under [nginx parity](#nginx-parity). Internals: [docs/design.md](docs/design.md).

## Features

- **Sticky, consistent routing.** Rendezvous (highest random weight) hashing of the client IP (IPv4,
  or the IPv6 /64, like ntfy's `visitor-prefix-bits-ipv6`) over the upstream *names*. Any number of
  load balancers with the same upstream names send a client to the same ntfy node, which keeps its
  in-memory rate limits in one place. Removing a node moves only its clients; adding one only takes
  its share. (nginx ignores `consistent` for upstreams that resolve by DNS, as on ntfy.sh, so
  there every node change reshuffled most clients.)
- **Active health checks** (`GET /v1/ready` every 5s by default, on every address of every
  upstream) with hysteresis: out after 3 failed probes in a row, back after 2 good ones. An
  upstream is up if any of its addresses is; a dead address is skipped. A new upstream, or one
  whose addresses changed, is probed before it gets traffic. If no upstream is healthy, it fails
  open and uses all of them.
- **Retries like nginx's `proxy_next_upstream error timeout http_503`**: a connection failure goes
  to the next upstream for any request (it never reached a node); other errors only for idempotent
  methods; a 503 (a draining ntfy node) only if no body byte was sent. A body that was partly sent
  is never resent. A node's other addresses are tried before the next node.
- **Long-lived streams**: flushes every write, an upstream idle timeout (3m,
  `proxy_read_timeout`) that also bounds stalled uploads and clients that stop reading, WebSocket
  upgrades, HTTP/2 on the TLS listener. Subscriptions take a
  lean path (raw upstream connection, no buffer held while idle, hijacked HTTP/1.1 connections):
  ~11 KiB per stream over HTTP/1.1, ~32 KiB with TLS, ~37 KiB over HTTP/2.
- **Streaming bodies** in both directions (`proxy_buffering off`, `proxy_request_buffering off`,
  `client_max_body_size 0`).
- **TCP keepalives** towards clients and upstreams, configurable (idle, interval, count).
- **Several addresses per upstream** (e.g. one node's ports), each good for ~63k connections from
  one source IP; new streams go to the address with the fewest.
- **Reload without cutting streams** (`SIGHUP`, `systemctl reload`, `ntfy-lb reload`): upstreams,
  health checks, timeouts, drain durations, streams, hashing, the TLS certificate and the access
  log (path, on/off). Streams to upstreams that stay are untouched. Listener addresses and client
  keepalives need a restart; a reload says so. A broken config (or certificate, or log path) is
  rejected as a whole.
- **Paced draining**: an upstream removed from the config, an upstream drained by command, and the
  load balancer itself (by command or on `SIGTERM`) close their streams oldest first, evenly spread
  over a duration. A draining load balancer turns away new *subscriptions* (so clients back off or
  go to another load balancer instead of coming back to be drained again) but keeps serving
  publishes.
- **`/v1/health` and `/v1/ready`** like ntfy's, plus a status API, Prometheus metrics (requests by
  upstream/code/kind, durations, streams by protocol, retries, drains, Go and process metrics),
  pprof, and a CLI on top.

## Status and results

Prototype, written and lab-tested on 2026-11-04. Not deployed anywhere. All tests pass with the
race detector (`make race`); `make lint` is clean apart from errcheck.

### Soak test

Three hours, 20,000 streams with the production protocol mix against three real ntfy nodes and
two load balancers, through drains, reloads, a load balancer restart and a `kill -9` of a node:
0 messages lost in steady state, delivery p99 ~2ms, no LB panics, memory flat. Details in
[docs/design.md](docs/design.md#soak-test-2026-11-05).

### Compared to nginx

Same load against both (40,000 streams, 150 publishes/s, 430 polls/s, production protocol mix,
nginx with the production LB config): both delivered every message with no errors. Memory is
about even (1.99 vs 2.04 GiB; nginx has ~630 MiB fixed, ntfy-lb more per stream). ntfy-lb uses
~2x the CPU (0.75 vs 0.35 cores) and has a higher p99 (6-8 ms vs ~4 ms), with p50 equal at
~1 ms. Details in [docs/design.md](docs/design.md#nginx-comparison-2026-11-05).

### Lab results

Two Proxmox VMs on box13: three independent (unclustered) ntfy nodes, and two ntfy-lb instances
with the same upstream names in a different order. A load generator held 2000 JSON subscriptions
from 200 source IPs and published to each topic. Since the nodes are not clustered, a message
only arrives if the subscribe and the publish hit the same node, which makes every routing mistake
visible.

| Test | Result |
|---|---|
| Subscribe via LB a, publish via LB b | 1405/1405 delivered; both LBs route every IP the same; load spread 640/611/750 |
| Drain one upstream over 30s | 610 streams closed at a steady 20-21/s; clients moved to the other nodes |
| Reload that removes an upstream (1m drain) | 12-13 closes/s for 60s, then the upstream is forgotten; other streams untouched |
| Kill an ntfy node | 0 client errors (connection refused is retried on the next node); out after ~5s, back ~4s after restart |
| Restart an LB (30s drain) | `/v1/ready` 503 at once; flat 66-67 closes/s; new subscriptions 503, publishes served; 2000 streams back within 5s |
| WebSocket and HTTP/2 streams | delivered; both closed by drains |
| 5000 streams each over HTTP/1.1, HTTP/1.1+TLS, HTTP/2 | all messages delivered; memory below |

Found and fixed along the way:

- **Reconnect snowball.** A draining LB that still accepted subscriptions got its own drained
  clients back, and the pacing went from 67 to 1100 closes/s. Draining LBs now refuse new
  subscriptions with 503 (publishes are still served).
- **Early closes.** Rounding closes up per tick closed everything well before the deadline. Pacing
  now carries fractional credit.
- **Missing log lines.** Aborted streams were not access-logged; the log write is now deferred.

### Memory per stream

5000 idle JSON streams, access log on, measured as RSS per stream:

| Client protocol | Generic proxy path | Stream path (current) | Goroutines |
|---|---|---|---|
| HTTP/1.1 | 68 KiB | **11 KiB** | 4 -> 1 |
| HTTP/1.1 + TLS | 91 KiB | **32 KiB** | 4 -> 1 |
| HTTP/2 + TLS | 70 KiB | **37 KiB** | 3.3 -> 1.3 |

Plain HTTP/1.1 is now in nginx's range. The remaining TLS cost is crypto/tls, which keeps ~6 KiB
per connection and cannot release its buffers while idle (OpenSSL can). The remaining HTTP/2 cost
is the stdlib h2 server: a 4 KiB writer and a handler goroutine per stream, and h2 connections
cannot be hijacked.

### Production traffic (lb1, 2026-11-04)

- ~70% of subscription stream time is HTTP/2. For the Android app (OkHttp) it is ~90%.
- ~3,300 client connections on :443 and ~110 on :80.
- So today's footprint would be tens of MB. At 100k streams with the same mix, expect ~3.5 GB
  (it was ~7 GB before the stream path).

### Behavior differences from nginx

- HTTP/1.1 subscription responses end with `Connection: close` and a close-delimited body instead
  of chunked encoding. That is valid HTTP, and streams end with their connection anyway.
- A dead HTTP/1.1 subscriber is noticed on the next write (at most ntfy's 45s keepalive) or by
  TCP keepalive, because the LB no longer reads from these connections.

### Open items

- The nginx features not ported yet, listed under [nginx parity](#nginx-parity): abuse blocks,
  `limit_req`, the CrowdSec log partitioning, website and docs redirects.
- Optional: turn off HTTP/2 on the LB. The app would move to HTTP/1.1 over TLS (~32 KiB instead of
  ~37 KiB), at the cost of one TCP+TLS connection per subscription. Not worth it at today's size.
- Rollout plan (one LB at a time behind DNS, or side by side on another port first).

## Quick start

```bash
make                                   # fmt-check, vet, tests, build/ntfy-lb
sudo install -m 755 build/ntfy-lb /usr/bin/ntfy-lb
sudo install -D -m 644 packaging/config.yml /etc/ntfy-lb/config.yml   # then edit it
sudo install -m 644 packaging/ntfy-lb.service /etc/systemd/system/
ntfy-lb check-config
sudo systemctl enable --now ntfy-lb
ntfy-lb status
```

## Configuration

See [packaging/config.yml](packaging/config.yml), which documents every option with its default.
The essentials:

```yaml
listen-http: ":80"
listen-https: ":443"
cert-file: /etc/letsencrypt/live/ntfy.sh/fullchain.pem
key-file: /etc/letsencrypt/live/ntfy.sh/privkey.pem
listen-admin: "127.0.0.1:2590"   # admin API: no auth, keep it local or private
upstreams:
  - name: app1                    # the NAME is what clients hash to: keep names equal on all LBs
    addresses: ["10.108.0.11:2586", "10.108.0.11:2587"]
  - name: app3
    addresses: ["10.108.0.13:2586", "10.108.0.13:2587"]
```

A reload applies everything except `listen-*` and `client-keepalive`, which need a restart
(`ntfy-lb reload` lists them if they changed). Unknown keys are an error.

For a backend other than ntfy, set which requests are long-lived streams, and the health path:

```yaml
streams:
  path: '^/events(/.*)?$'     # GET requests whose path matches are streams
  exclude-params: [once]      # ...unless ?once=1 (or header "Once: 1")
health-check:
  path: /healthz
```

## CLI

Flags go anywhere on the command line. The CLI finds the admin API from `--admin-url`, else from
`listen-admin` in `--config` (default `/etc/ntfy-lb/config.yml`).

```
ntfy-lb serve                              run (SIGHUP reloads, SIGTERM drains then exits)
ntfy-lb check-config [-c FILE]             validate a config file
ntfy-lb status [--json]                    load balancer + upstream state, streams, counters
ntfy-lb reload                             re-read the config file
ntfy-lb drain [-d 10m] / undrain           drain the load balancer itself
ntfy-lb upstream drain NAME [-d 10m]       take a node out of rotation, close its streams over 10m
ntfy-lb upstream undrain NAME              put it back
```

```
$ ntfy-lb status
Load balancer: ready, 2000 stream(s)

UPSTREAM  STATE     STREAMS  REQUESTS  FAILURES  ADDRESSES                            NOTE
app1      healthy   724      1136      0         192.168.1.48:2186,192.168.1.48:3186
app2      draining  359      789       0         192.168.1.48:2286                    drained in 18s (02:16:41)
app3      healthy   864      1353      0         192.168.1.48:2386
```

Without `-d`, drains use `drain-duration` from the config (default 5m). A drain started by command
survives reloads until `undrain`.

## Admin API

On `listen-admin` only (the public listeners proxy everything, including ntfy's own `/v1/health`).

| Method + path | |
|---|---|
| `GET /v1/health` | `{"healthy":true}`; 200 if at least one upstream in rotation is healthy, else 503 |
| `GET /v1/ready` | `{"ready":true,"draining":false}`; 200 if healthy and not draining, else 503 |
| `GET /v1/status` | full status (JSON), what `ntfy-lb status` shows |
| `POST /v1/reload` | re-read the config; 400 with `{"error":...}` if it is invalid (nothing changes); `restart_required` lists changed settings that need a restart |
| `POST /v1/drain`, `DELETE /v1/drain` | drain / undrain the load balancer; optional body `{"duration":"10m"}` |
| `POST /v1/upstreams/{name}/drain`, `DELETE ...` | drain / undrain one upstream; same body |
| `GET /metrics` | Prometheus, see [docs/design.md](docs/design.md#metrics-servermetricsgo) |
| `GET /debug/pprof/` | Go profiles |

## Operating it

- **Take a node out**: `ntfy-lb upstream drain app3 -d 10m` on every load balancer (or remove it
  from the config and reload). Its clients move to their next node in hash order, about
  `streams / 600` per second.
- **Take a load balancer out**: `ntfy-lb drain -d 10m` (or `systemctl stop`, which uses
  `shutdown-drain-duration`). `/v1/ready` fails right away; streams close evenly over the duration;
  new subscriptions get 503 + `Connection: close`; publishes are served until the end.
- **Certificate renewal**: a certbot deploy hook of `systemctl reload ntfy-lb`. Unlike an nginx
  reload, this never cuts a stream.
- **Access log rotation**: `postrotate systemctl reload ntfy-lb`.
- **Kernel**: Go listens with a backlog of `net.core.somaxconn`, so set that to 4096 or more
  (nginx used `backlog=4096`), and widen `net.ipv4.ip_local_port_range` for upstream connections.
- **systemd**: `TimeoutStopSec` must exceed `shutdown-drain-duration` (the unit uses 5m).

## nginx parity

Taken over from the ntfy.sh nginx config: `hash $binary_remote_addr` (now consistent), the
lbagent health checks and hysteresis, `proxy_next_upstream error timeout http_503`, 5s connect
timeout, 3m read timeout, `proxy_buffering off`, `proxy_request_buffering off`,
`client_max_body_size 0`, `X-Forwarded-For` appending (`$proxy_add_x_forwarded_for`) and `Host`
passthrough, WebSocket upgrades, HTTP/2, `keepalive_timeout 65`, multiple ports per node, an access
log in the `combined_ssl` format, and `worker_shutdown_timeout`-style draining (now paced).

**Not** taken over (yet), so nginx-only today:

- the edge abuse blocks (`$abuse_topic`, `$abuse_user_agent`, over-long topic lists -> 403/414)
- `limit_req` (45r/m, burst 1000) and its exemption list
- the access-log partitioning (`access_error_*.log`) that the CrowdSec jails read
- the HTTP -> HTTPS redirect for web pages, the static website (`/`, `/_next`, `/_site`) and the
  `/docs` redirects
- `lingering_time 3s` (Go reads at most 256 KiB of an unread body, then closes)
- the nginx stub_status exporter (replaced by `/metrics`)

## Development

```bash
make           # fmt-check, vet, test, build
make race      # tests with the race detector
make lint      # golangci-lint
```

Layout: `config/` (YAML config), `balancer/` (hashing, health, sessions, drains), `server/`
(listeners, proxy, admin API, access log), `client/` (admin API client), `cmd/` (CLI).
