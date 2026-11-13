# ntlb architecture

How ntlb is put together, in diagrams: where it runs, its components, what a request goes
through, and the flows that matter for long-lived streams (subscriptions, drains, reloads,
health, bans). The diagrams are [Mermaid](https://mermaid.js.org), which GitHub renders. The
details behind each part (invariants, numbers, lab results) are in [design.md](design.md).

## Deployment

On ntfy.sh, ntlb runs on each load balancer in front of the app nodes. Clients connect over
HTTP/1.1, HTTP/2 or WebSocket; ntlb terminates TLS, applies the rules, serves the website, and
proxies to the app nodes' private listeners. Its logs feed CrowdSec, its metrics Prometheus.

```mermaid
flowchart LR
    clients["Clients<br/>Android, iOS, web app,<br/>CLI, UnifiedPush, curl"]
    dns["DNS<br/>ntfy.sh"]
    subgraph lb["Load balancer (one of several)"]
        ntlb["ntlb<br/>:80 :443"]
        www["/var/www/website"]
        logs["/var/log/nginx/*.log<br/>/var/log/ntlb/ban.log"]
        crowdsec["CrowdSec agent<br/>+ firewall bouncer"]
    end
    subgraph apps["App nodes (private network)"]
        app1["ntfy app-1<br/>:25010-25017"]
        app2["ntfy app-2<br/>:25010-25017"]
    end
    db[("Postgres<br/>(ntfy, CrowdSec)")]
    prom["Prometheus<br/>(stats host)"]

    clients --> dns --> ntlb
    ntlb -- "static rule" --> www
    ntlb -- "proxy, health checks" --> app1
    ntlb -- "proxy, health checks" --> app2
    ntlb -- "access logs, bans" --> logs
    logs --> crowdsec
    crowdsec <--> db
    app1 <--> db
    app2 <--> db
    prom -- "/metrics :25031" --> ntlb
```

Every load balancer runs the same config, and with `balance: hash` they agree on which app node
a client goes to: the hash is over the upstream names, not over anything local to one instance.

## Components

The packages and how they depend on each other. `server` owns the listeners and the request
path; `balancer` decides where requests go and owns health and drains; `rules` decides what
happens to a request before it is proxied; `config` and `match` are shared.

```mermaid
flowchart TB
    main["main.go"] --> cmd
    subgraph cmd["cmd: CLI (urfave/cli v3)"]
        serve["serve"]
        cli["status, reload, drain,<br/>undrain, upstream, check-config"]
    end
    cli -- "admin API" --> client["client: admin API client"]
    serve --> server

    subgraph server["server: listeners and the request path"]
        listeners["Listeners<br/>one http.Server per address,<br/>countingListener, connTracker"]
        certs["certStore<br/>certificate by SNI"]
        observe["observe<br/>site, validation, rules,<br/>access log, metrics"]
        gzip["gzipWriter"]
        answer["answer<br/>block, limit, redirect, static"]
        proxy["proxy<br/>httputil.ReverseProxy, RoundTrip, retries"]
        stream["stream path<br/>raw upstream connection,<br/>hijacked client"]
        accesslog["accessLog, logFiles"]
        metrics["metrics, collectors"]
        admin["admin, metrics and<br/>profiler handlers"]
    end

    subgraph balancer["balancer: routing, health, drains"]
        pick["Pick<br/>strategy.go, hash.go"]
        sessions["Sessions<br/>Reserve, Attach, Close"]
        health["Health loop<br/>probes, hysteresis"]
        drains["Drain loop<br/>paced closes"]
    end

    subgraph rules["rules: the rule engine"]
        engine["Engine.Check<br/>CheckResponse"]
        limiters["limiters<br/>token buckets"]
        bans["bans<br/>strikes, ban lines"]
    end

    match["match: expressions (expr)"]
    config["config: YAML, sites, includes,<br/>validation, defaults"]

    listeners --> certs
    listeners --> observe
    observe --> engine
    observe --> gzip
    gzip --> answer
    gzip --> proxy
    proxy --> stream
    proxy --> pick
    proxy --> sessions
    stream --> pick
    stream --> sessions
    observe --> accesslog
    observe --> metrics
    engine --> limiters
    engine --> bans
    bans -- "ban line" --> accesslog
    engine --> match
    config --> match
    server --> config
    balancer --> config
    rules --> config
    admin --> balancer
```

## The request path

What one request goes through, from the listener to the upstream and back. Everything the
outcome feeds (access log, response rules, metrics) runs once the request ends, which for a
detached stream is long after the handler returned.

```mermaid
flowchart TD
    accept["Accept (countingListener)<br/>ntlb_connections + 1"] --> tls{"TLS listener?"}
    tls -- yes --> sni["Handshake<br/>certificate by SNI"] --> parse
    tls -- no --> parse["Go parses the request"]
    parse -- "malformed, oversized,<br/>cut off" --> bad["400 by Go<br/>connTracker logs it,<br/>response rules strike"]
    parse --> retired{"Listener removed<br/>by a reload?"}
    retired -- yes --> r503["503, Connection: close"]
    retired -- no --> site["Site by Host<br/>exact, *.suffix, default"]
    site --> valid{"Valid path<br/>and Host?"}
    valid -- no --> v400["400"]
    valid -- yes --> rules{"Request rules<br/>global, then the site's"}
    rules -- "block / limit" --> deny["403 / 429<br/>+ strikes"]
    rules -- "redirect / static" --> serve["Answer from ntlb"]
    rules -- "allow / none" --> kind{"Subscription?<br/>streams.match"}
    kind -- "no: request" --> rp["ReverseProxy<br/>RoundTrip with retries"]
    kind -- "yes, Upgrade" --> ws["ReverseProxy<br/>WebSocket upgrade"]
    kind -- "yes, bodyless" --> sp["Stream path<br/>raw upstream connection"]
    rp --> up[("Upstream")]
    ws --> up
    sp --> up
    deny --> done
    serve --> done
    rp --> done["Request ends<br/>access log, response rules,<br/>metrics"]
    ws --> done
    sp --> done
    bad --> done
```

gzip sits between `observe` and both answers and the proxy, so a static file and a proxied
response are compressed alike; the stream path unwraps it, as subscriptions are never compressed.

## Balancing

`Pick(site, key)` returns the upstreams to try, best first; the first is the choice, the rest are
the retries. Draining upstreams are never picked; unhealthy ones only if no upstream is healthy
(fail open).

```mermaid
flowchart LR
    all["The site's upstreams<br/>in config order"] --> draining{"Draining<br/>or removed?"}
    draining -- yes --> skip["Skipped"]
    draining -- no --> healthy{"Any healthy?"}
    healthy -- yes --> h["The healthy ones"]
    healthy -- "no: fail open" --> u["All of them"]
    h --> order{"balance"}
    u --> order
    order -- "round-robin" --> rr["Rotated by the site's turn"]
    order -- "least-conn" --> lc["Rotated, then by load<br/>(attached + reserved)"]
    order -- "random-two-least-conn" --> r2["Less loaded of two random,<br/>then the rest by load"]
    order -- "hash" --> hash["Rendezvous hash of the<br/>client key over the names"]
```

Within an upstream, a new request goes to the address with the fewest streams plus reserved
attempts, skipping addresses that failed recently or failed their last health check.

## Sequences

### A request, with a retry

A publish whose first upstream refuses the connection. A connection failure is retried for any
request, since it never reached a node; other errors only for idempotent methods, and a 503 only
if no body byte was sent.

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant S as ntlb server (observe)
    participant R as rules Engine
    participant P as proxy (RoundTrip)
    participant B as Balancer
    participant U1 as app-1
    participant U2 as app-2
    C->>S: PUT /mytopic
    S->>R: Check(request)
    R-->>S: no rule decides
    S->>P: ServeHTTP
    P->>B: NewSession, Pick(site, key)
    B-->>P: [app-1, app-2]
    P->>B: Reserve(app-1)
    P->>U1: connect
    U1--xP: connection refused
    P->>B: RecordAddressFailure(app-1, addr)
    P->>B: Reserve(app-2)
    P->>U2: PUT /mytopic (body streamed)
    U2-->>P: 200
    P->>B: Attach(app-2)
    P-->>C: 200 (gzip if it qualifies)
    P->>B: Close(session)
    S->>S: access log line, response rules, metrics
```

### An HTTP/1.1 subscription on the stream path

Subscriptions are idle almost all the time, and there are tens of thousands of them, so they skip
`ReverseProxy`: one raw upstream connection, a hijacked client connection, one goroutine, and no
buffer held while waiting.

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant H as Handler (observe, proxy)
    participant G as Stream goroutine
    participant B as Balancer
    participant U as app node
    C->>H: GET /mytopic/json
    H->>B: NewSession, Pick, Reserve
    H->>U: GET /mytopic/json HTTP/1.0 (raw connection)
    U-->>H: 200, close-delimited body
    H->>B: Attach
    H->>C: hijack, write the response head
    H->>G: hand over (handler returns, server state freed)
    loop Until the upstream ends, the client is gone, or a drain
        G->>U: wait readable (netpoller, no buffer)
        U-->>G: message or keepalive (every 45s)
        G->>C: write (pooled 4 KiB buffer, write deadline)
    end
    G->>B: Close(session)
    G->>G: finishers: access log, response rules, metrics
```

Over HTTP/2 the same upstream side is used, but the client side cannot be hijacked: the handler
writes through the `ResponseWriter` and flushes every write.

### A WebSocket

Upgrades stay on `ReverseProxy`, which hijacks the client connection after the 101 and copies
both ways. ntlb wraps the upstream side to bound idleness in both directions, to end the client
side when the upstream closes, and to use small copy buffers (ntfy's clients send tiny frames).

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant P as ReverseProxy
    participant W as upgradedBody
    participant U as app node
    C->>P: GET /mytopic/ws, Upgrade: websocket
    P->>U: same, Upgrade: websocket
    U-->>P: 101 Switching Protocols
    P->>C: 101, hijack
    par Upstream to client (4 KiB buffer)
        U->>W: frame
        W->>C: frame, idle timer reset
    and Client to upstream (1 KiB buffer)
        C->>W: frame
        W->>U: frame, idle timer reset
    end
    U--xW: closes (e.g. no pong)
    W->>P: cancel: upstream closed
    P--xC: close (no close frame: clients do not reconnect after one)
```

### Draining an upstream

`ntlb upstream drain app-1 -d 10m`, or app-1 removed from the config: its streams close oldest
first, spread evenly over the duration, so its clients reconnect at a steady rate instead of all
at once. New requests already avoid it.

```mermaid
sequenceDiagram
    autonumber
    participant O as Operator
    participant A as Admin API
    participant B as Balancer
    participant T as Drain tick (every 1s)
    participant S as Streams on app-1
    participant C as Their clients
    O->>A: POST /v1/upstreams/app-1/drain {"duration":"10m"}
    A->>B: DrainUpstream(app-1, 10m)
    Note over B: Pick skips app-1 from now on
    loop Every second until the deadline
        T->>B: drainTickVictims
        B-->>T: this tick's share, oldest first
        T->>S: cancel(ErrDrained)
        S--xC: connection closed
        C->>B: reconnect: picked onto another node
    end
    Note over B: drained (app-1 removed from the config: forgotten once its last stream closed)
```

### Draining the load balancer (SIGTERM)

On shutdown (or `ntlb drain`) ntlb turns unready and turns away new subscriptions (so clients go
to another load balancer instead of being drained again), but keeps serving publishes until it is
drained.

```mermaid
sequenceDiagram
    autonumber
    participant SD as systemd
    participant S as ntlb
    participant B as Balancer
    participant C as Clients
    SD->>S: SIGTERM
    S->>B: Drain(shutdown-drain-duration)
    Note over S: admin /v1/ready is 503 (for an external check)
    C->>S: new subscription
    S-->>C: 503, Connection: close
    C->>S: publish
    S-->>C: proxied as usual
    loop Every second
        B->>C: close this tick's share of streams, oldest first
    end
    S->>S: close the listeners, wait for the last responses
    S-->>SD: exit
```

### A reload

`SIGHUP`, `systemctl reload ntlb` or `ntlb reload`. Everything that can fail is opened first, so a
broken config, certificate, log path or listen address rejects the reload and changes nothing.
Streams to upstreams that stay are not touched.

```mermaid
sequenceDiagram
    autonumber
    participant O as Operator
    participant S as Server.Reload
    participant B as Balancer
    participant R as rules Engine
    O->>S: SIGHUP
    S->>S: load and validate config.yml + sites.d/*.yml
    S->>S: load certificates, bind added addresses, open log files
    alt anything failed
        S-->>O: error, the running config stays
    else
        S->>S: swap certificates and log files (paths that stay are reopened)
        S->>R: Update: same rule keeps its buckets, same bans their strikes
        S->>B: Update: probe new upstreams first, drain removed ones
        S->>S: store the config, retire removed listeners
        Note over S: a retired listener closes its idle connections<br/>(HTTP/2: GOAWAY), its streams run on
        S-->>O: ok (and which settings need a restart)
    end
```

### Health checks

Every address of every upstream is probed each interval. An upstream is up if any address is;
hysteresis keeps it from flapping. The first probe decides directly, so a node that is down or
draining when ntlb starts, or when a reload adds it, never gets traffic.

```mermaid
stateDiagram-v2
    [*] --> FirstProbe: started, or added by a reload
    FirstProbe --> Healthy: passes
    FirstProbe --> Unhealthy: fails
    Healthy --> Unhealthy: fails probes in a row (default 3)
    Unhealthy --> Healthy: passes probes in a row (default 2)
    Healthy --> Draining: drain command, or removed from the config
    Unhealthy --> Draining: drain command, or removed from the config
    Draining --> Drained: deadline passed
    Draining --> Healthy: undrain, or re-added
    Drained --> Healthy: undrain
    Drained --> [*]: removed and its last stream closed
```

A probe is `GET http://<address><path>` on a new connection; 2xx and 3xx pass (redirects are not
followed). The ntfy site probes `/v1/ready`, which is 503 while a node drains.

### An upstream goes bad

What clients see depends on how the node breaks. The README's
[health check section](configuration.md#health-checks-and-what-happens-when-one-goes-bad) has the
timings; these are the paths.

**The node dies** (connections refused or reset). Requests notice before the health checks do:
a connect error never reached the node, so any request moves on, first to the node's other
addresses (each failed one backs off 1s, doubling to 30s), then to the next upstream.

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant P as proxy.RoundTrip
    participant B as Balancer
    participant A1 as app1 :25010
    participant A2 as app1 :25011
    participant N as app3

    C->>P: POST /mytopic
    P->>B: Pick(site, key)
    B-->>P: [app1, app3] (app1 still healthy)
    P->>A1: dial
    A1--xP: connection refused
    P->>B: RecordAddressFailure(app1, :25010), backoff 1s
    P->>A2: dial (another address of the same node)
    A2--xP: connection refused
    P->>B: RecordAddressFailure(app1, :25011)
    P->>N: dial app3 (untouched body, connect error: retryable)
    N-->>P: 200
    P-->>C: 200 (ntlb_retries_total{reason="connect"} +2)
```

**The node hangs** (accepts, never answers). Nothing fails fast, so requests wait for
`upstreams.<name>.timeouts.read` and get 504; they are not retried, since the node may have acted on them.
The probes time out too, and after `fails` rounds the node is out and new requests skip it.

```mermaid
sequenceDiagram
    participant C as Client
    participant LB as ntlb
    participant A as app1 (hung)
    participant H as health loop
    C->>LB: GET /x
    LB->>A: GET /x
    Note over A: no answer
    H->>A: probe (times out after 3s)
    H->>A: probe (times out)
    H->>A: probe (times out)
    Note over H: app1 out of rotation
    LB--xA: upstream.timeouts.read
    LB-->>C: 504
    C->>LB: GET /x (retry by the client)
    LB->>LB: Pick skips app1
```

**Streams on a bad node** are never moved by health: an open stream stays on its node until its
connection ends or a drain closes it. On a dead node the connection ends by itself (a reset, or
TCP keepalive after `idle + interval * count`), the client reconnects, and its new request picks
a healthy node. A node that fails its probes but still serves (say, readiness 503 because its
database is slow) keeps its streams; drain it to move them.

```mermaid
sequenceDiagram
    participant C as Subscriber
    participant LB as ntlb
    participant A as app1
    participant N as app3
    C->>LB: GET /mytopic/json (stream)
    LB->>A: GET /mytopic/json
    A-->>C: 200, keepalives every 45s
    Note over A: app1 crashes
    A--xLB: reset (or no keepalive: TCP keepalive / read timeout)
    LB--xC: stream ends
    C->>LB: GET /mytopic/json (reconnect)
    Note over LB: app1 unhealthy (or backing off)
    LB->>N: GET /mytopic/json
    N-->>C: 200, the stream goes on from app3
```

**Every node is bad.** `Pick` fails open: all upstreams that are not draining, in balance
order, so a shared problem (the database) degrades instead of turning everyone away. The admin
`/v1/health` and `/v1/ready` answer 503 meanwhile, for monitors and anything in front.

```mermaid
flowchart LR
    pick["Pick(site, key)"] --> d{"upstreams<br>not draining"}
    d -- none --> s503["503 (all draining)"]
    d -- some --> h{"any of them<br>healthy?"}
    h -- yes --> hl["the healthy ones,<br>by balance"]
    h -- no --> all["all of them, by balance<br>(fail open)<br>/v1/health: 503"]
    hl --> try["try in order,<br>retry where safe"]
    all --> try
    try --> ok["answer"]
    try --> err["502 error / 504 timeout"]
```

### Rules, strikes and bans

Rules decide a request before it is proxied (block, limit, allow, redirect, static, proxy), and response
rules look at the outcome. Both can add strikes per client; a client over the threshold within the
window gets one line in the ban file, which CrowdSec turns into a firewall ban on every host.

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant S as ntlb
    participant E as rules Engine
    participant F as /var/log/ntlb/ban.log
    participant CS as CrowdSec agent
    participant DB as Postgres (CrowdSec)
    participant FW as Bouncers (all hosts)
    loop Over the limit
        C->>S: request
        S->>E: Check: limit exceeded
        E->>E: 503, strikes += 20
    end
    E->>F: strikes > threshold: one line per window<br/>"time ip prefix ntfy/limit"
    F->>CS: tailed (ntlb-ban parser and scenario)
    CS->>DB: decision: ban the prefix, 4h
    DB->>FW: pulled by every bouncer
    FW--xC: dropped at the firewall
```

The access logs, partitioned like nginx's were (400s ntlb answered itself, 413, 5xx, abuse),
feed the other CrowdSec scenarios the same way.

## Concurrency and ownership

- `Balancer.mu` protects the upstream map, sessions and drains; `Upstream.mu` one upstream's
  addresses, counts and health. The order is always `Balancer.mu`, then `Upstream.mu`. The drain
  tick collects its victims under the lock and cancels them outside it, and scans the sessions
  only while something drains.
- The config, the proxy's transport and the rule state are swapped atomically on reload
  (`atomic.Pointer`); a request keeps the config it started with, a stream the upstream it
  attached to.
- Per-request cleanup is a list of finishers on the `proxyRequest`, run once by whoever ends the
  request: the handler, or the stream goroutine of a detached stream.
- Goroutines per connection: an HTTP/1.1 stream has one (the copy), an HTTP/2 stream its handler,
  a WebSocket the reverse proxy's two copiers. The health loop and the drain loop are one each,
  plus a flusher per log file; probes run in short-lived goroutines per round.
