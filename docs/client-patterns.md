# ntfy client request patterns (for testing ntlb)

Researched 2026-11-10 from source. Checkouts used:

- Server/web/CLI: `~/Code/ntfy/ntfy` (main checkout)
- Android: `~/Code/ntfy/android` @ d9e11bad (2026-10-19), OkHttp 5.3.2 (`android/app/build.gradle:109`)
- iOS: `~/Code/ntfy/ios` @ 96e659c (2026-06-26); `~/Code/ntfy/ios-nextgen` (third-party fork "NTFY me", same wire patterns)

Paths below are relative to those roots: `server/...` = `ntfy/ntfy/server/...`, `web/...` = `ntfy/ntfy/web/...`,
`android/...` = `ntfy/android/app/src/main/java/io/heckel/ntfy/...`, `ios/...` = `ntfy/ios/...`.

---

## 0. Server side facts a proxy must respect

| Fact | Value | Ref |
|---|---|---|
| Router | big if/else on method + path regex | `server/server.go:765-920` |
| Topic | `[-_A-Za-z0-9]{1,64}` | `server/server.go:99` |
| Subscribe paths | `^/<t>(,<t>)*/(json\|sse\|raw\|ws)$`, GET only | `server/server.go:102-105`, `:904-911` |
| Max topics per subscribe | 150 distinct (dedup'd); more = 400 | `server/server.go:185`, `:2134-2143` |
| Poll flag | `x-poll`/`poll`/`po`, header first, then query (query name must be lowercase); true = `1`/`yes`/`true` | `server/server.go:1963`, `server/util.go:33-82` |
| since | `x-since`/`since`/`si`: empty, `all`, `latest`, `none`, message ID (12 chars), unix ts, Go duration (`10m`, `12h`) | `server/server.go:2087-2112` |
| since default | stream: none; poll: all | `server/server.go:2090-2095` |
| scheduled | `x-scheduled`/`scheduled`/`sched` | `server/server.go:1964` |
| Filters | `id`, `message`, `title`, `tags`, `priority` (also as `X-` headers) | `server/types.go:57-62` |
| Stream keepalive | every 45s (`keepalive-interval`): JSON `{"event":"keepalive",...}` line; SSE `event: keepalive`; raw: empty line | `server/config.go:26`, `server/server.go:1789-1806`, `:1679` |
| Stream headers | `Content-Type: application/x-ndjson; charset=utf-8` (json), `text/event-stream` (sse), `text/plain` (raw); `Incremental: ?1`; every write flushed; no Content-Length (chunked on h1) | `server/server.go:1747-1758`, `:1738-1742` |
| Poll response | same content type, but NOT `Incremental`, ends normally; `X-Messages-Truncated: 1` if capped at 10 MiB/topic | `server/server.go:1751-1757`, `:2074-2076`, `server/config.go:89` |
| First stream line | `open` event, then replay (since=), then live | `server/server.go:1779-1784` |
| WS | requires `Upgrade: websocket` header else 400 (so a `/ws` GET without upgrade is an error, not a stream); gorilla upgrader, any Origin; server pings every 45s; read deadline 45s+15s=60s (client must pong or send something); read limit 64 bytes (client never sends data frames); write deadline 2s | `server/server.go:1809-1906`, `:195-198` |
| WS close by server | on cancel/drain the server does `conn.Close()` (TCP close, **no close frame**) | `server/server.go:1885-1888` |
| Drain | node goes read-only: new subscriptions get 503 (WS: before upgrade, so LB can retry), publishes get 503; existing streams cancelled evenly over the drain duration | `server/server.go:1697-1699`, `:1826-1827`, `:1104`, `server/server_drain.go:36-94` |
| Auth | `Authorization: Basic ...` or `Bearer ...`, or query `?auth=`/`?authorization=` = base64url("Basic base64(u:p)") or base64url("Bearer tk"); Basic with empty user = password is token | `server/server_auth.go:54-99` |
| Client IP | `behind-proxy` + `proxy-forwarded-header` (default `X-Forwarded-For`), `proxy-trusted-hosts` | `server/config.go:224-225`, `:360`, `server/util.go:86-` |
| OPTIONS | any path: 200, `Access-Control-Allow-Methods: GET, PUT, POST, PATCH, DELETE`, `-Allow-Origin: <cfg>`, `-Allow-Headers: *` | `server/server.go:890`, `:2115-2120` |
| HEAD | only `HEAD /` (empty 200) and `HEAD /file/<id>` (Content-Length, no body). Everything else HEAD = 404 (no implicit GET->HEAD mapping) | `server/server.go:768`, `:888`, `:1009` |
| /file | `^/file/<id>(\.<ext>)?$`, GET/HEAD, `io.Copy` with explicit Content-Length; **no Range support** (Range ignored, 200 full body); `Content-Disposition` if named; bandwidth charged to uploader | `server/server.go:158`, `:990-1046` |
| Static | `/static/*`, `/app.html`, `/sw.js`, `/sw.js.map`, `/docs/*`: `http.FileServer` over embedded FS with ModTime = **process start time** -> `Last-Modified`/`If-Modified-Since` -> 304, Range -> 206; gzip if `Accept-Encoding` contains `gzip` (no `Vary` set by ntfy) | `server/server_web.go:64-73`, `server/server.go:156-171`, `util/gzip_handler.go:13-30` |
| SPA routes | `GET /`, `GET /<topic>`, `GET /<host.tld>/<topic>`, `/account/email/verify/<tok>`, `/account/password/reset/<tok>` -> index.html | `server/server.go:766`, `:914-917` |
| config | `GET /v1/config` (JSON, no-cache), `GET /config.js` (JS, no-cache), `GET /manifest.webmanifest` | `server/server.go:776-781`, `server/server_web.go:28-61` |
| Health | `GET /v1/health`, `GET /v1/ready` (the ntfy site in ntlb probes `/v1/ready`) | `server/server.go:770-773` |
| Body limits | message 4096 B (bigger body becomes attachment); attachment 15 MB default (ntfy.sh tiers higher); JSON publish body 2x message limit | `server/config.go:79-82`, `server/server.go:1537-1560`, `:2348` |
| Upload length | `attachment.Write(..., r.ContentLength, ...)`: works with `Content-Length` and with -1 (chunked / h2 without length) | `server/server.go:1632` area |
| Errors | JSON `{"code":40001,"http":400,"error":...}`, `Retry-After` and rate-limit headers on 429 | `server/server.go:710-762` |

Full route list (method, path) from `server/server.go:765-920`:

```
GET  /                                  web app (or 404 if web disabled)
HEAD /                                  empty 200
GET  /v1/health | /v1/ready | /v1/version(admin) | /v1/config | /v1/stats | /v1/tiers
GET  /config.js | /manifest.webmanifest | /metrics (if enabled on main listener)
GET|POST|PUT|DELETE /v1/users, PUT|POST|DELETE /v1/users/access, GET|POST|DELETE /v1/users/tokens,
POST /v1/users/password/reset, POST|PUT|DELETE /v1/tiers                     (admin API)
POST|GET|DELETE /v1/account
POST /v1/account/password | /v1/account/login
POST|PATCH|DELETE /v1/account/token
PATCH /v1/account/settings
POST|PATCH|DELETE /v1/account/subscription
POST /v1/account/reservation, DELETE /v1/account/reservation/<topic>
POST|PUT|DELETE /v1/account/billing/subscription, GET /v1/account/billing/subscription/success/<id>
POST /v1/account/billing/portal, POST /v1/account/billing/webhook (from Stripe), GET /v1/account/billing/tiers
PUT|DELETE /v1/account/phone, PUT /v1/account/phone/verify
PUT|DELETE /v1/account/email, POST /v1/account/email/verify|primary|resend
POST /v1/account/password/reset/request | /v1/account/password/reset
POST|DELETE /v1/webpush
GET|POST /_matrix/push/v1/notify
GET  /static/... /app.html /sw.js /sw.js.map /docs/...
GET|HEAD /file/<id>[.ext]
OPTIONS *                                CORS preflight
PUT|POST /                               JSON publish ({"topic":...})
PUT|POST /<topic>                        publish (body = message or attachment)
PUT|POST /<topic>/<sequence-id>          publish update
DELETE /<topic>/<sid>, GET /<topic>/<sid>/delete     delete
GET|PUT /<topic>/<sid>/read|clear        mark read/clear
GET  /<topic>/publish|send|trigger       publish via GET (params in query/headers)
GET  /<t,...>/json|sse|raw|ws            subscribe (stream) or poll (?poll=1)
GET  /<t,...>/auth                       read-access check
GET  /<topic>?up=1                       UnifiedPush discovery {"unifiedpush":{"version":1}}
```

---

## 1. Android app (Kotlin, OkHttp 5.3.2)

### 1.1 Common properties

- **User-Agent**: `ntfy/<versionName> (<flavor>; Android <release>; SDK <int>)`, flavor = `play`|`fdroid`
  (`android/util/HttpUtil.kt:19`). Example: `ntfy/1.24.0 (play; Android 15; SDK 35)`.
- **Auth**: `Authorization: Basic base64(user:pass)` if a user is configured for the base URL (token users
  are stored as empty username + token as password, so it is still Basic) (`HttpUtil.kt:73-84`). WebSocket
  also uses the header, NOT `?auth=` (`WsConnection.kt:81`).
- **Custom headers**: user-configurable arbitrary headers per server, added to every request including WS
  and stream (`HttpUtil.kt:80-82`, `ApiService.kt:188,226,252,275`). Used for reverse-proxy auth
  (Cloudflare Access, Authelia, ...). The LB must pass unknown headers through.
- **Client certificates (mTLS)** and pinned self-signed certs per base URL (`util/CertUtil.kt:42-72`). A
  TLS-terminating LB in front of a self-hosted server that requires client certs would break these users.
- **Accept-Encoding**: OkHttp adds `Accept-Encoding: gzip` transparently when not set (and transparently
  gunzips). Applies to streams too, so an LB that gzips responses must never gzip streams (buffering).
- **No Accept header** is set explicitly.
- **Connection reuse**: every call creates a *brand-new* `OkHttpClient` via `OkHttpClient.Builder()`
  (`HttpUtil.kt:24-98`, `CertUtil.withTLSConfig`), i.e. a new connection pool each time. So every poll /
  publish / auth check / stream is its own TCP+TLS handshake; there is no h2 multiplexing across requests.
- **HTTP version**: OkHttp negotiates **h2 via ALPN over TLS** (default protocols `[h2, http/1.1]`), h1 on
  cleartext. So JSON streams, polls, publishes arrive as HTTP/2 (one stream per connection, see above).
  **WebSockets are always HTTP/1.1** (OkHttp forces `ONLY_HTTP1` for `newWebSocket`, no RFC 8441).
- **Timeouts**: default client connect/read/write 15s, call 1 min (`HttpUtil.kt:92-98`); long-call client
  (publish, attachment download) call 5 min (`:32-36`); stream client read 77s (`:41-45`); WS client no
  read timeout, ping every 3 min, connect 10s (`:65-71`).

### 1.2 Pattern A1: JSON stream subscription (default protocol)

- Default connection protocol is `jsonhttp` (`android/db/Repository.kt:364`, `:707-708`); WS is opt-in via
  settings or a banner (`ui/MainActivity.kt:320`, `ui/SettingsActivity.kt:731`).
- `GET <base>/<t1>,<t2>,...,<tN>/json?since=<id|none>` (`util/Util.kt:59`, `msg/ApiService.kt:243-266`,
  `service/JsonConnection.kt:107-126`).
  - **One connection per base URL** with *all* instant subscriptions of that server joined by commas
    (`service/SubscriberService.kt:230-257`). This includes every UnifiedPush topic (always instant,
    `up/BroadcastReceiver.kt:78-85`), so a phone with many UP apps (Element, Tusky, ...) has a long
    comma list. UP topics are `up` + 12 random chars (`up/BroadcastReceiver.kt:226-227`).
  - `since` = last notification ID across all subscriptions in that connection, or `none` if there is
    none (`JsonConnection.kt:123`). Never a timestamp. (In ApiService `since ?: all`, but the caller
    always passes something.)
  - Headers: `User-Agent`, optional `Authorization: Basic`, custom headers, `Accept-Encoding: gzip`, h2.
- **Long-lived**. Read timeout 77s against the server's 45s keepalive line (`HttpUtil.kt:43`).
- **Reconnect** (`JsonConnection.kt:117-175`):
  - Clean end of body (`source.exhausted()`, e.g. h2 END_STREAM / h1 terminating chunk / FIN after
    response complete) -> **reconnect immediately, no delay**.
  - Exception (RST_STREAM, IO error, read timeout, non-2xx status) -> backoff
    `5, 10, 15, 20, 30, 45, 60, 120, 120, ...` seconds; reset to 0 after a successful connect
    (`:128-130`, `:153-159`, `:174`).
  - 401/403 -> `NotAuthorizedException`, still retried with backoff (`ApiService.kt:256-263`).
  - Connection is re-created (close + new) when topics, protocol, user, custom headers, certs change, or
    when `connectionForceReconnectVersion` is bumped by a network change (`service/Connection.kt:36-45`,
    `app/Application.kt:34-63`). Network switch (Wi-Fi <-> cellular) = immediate new request from new IP.
  - Service restart triggers: `ServiceStartWorker` every 3h (`ui/MainActivity.kt:519-524`, `:952`),
    FCM `keepalive` on `~control` every 3h on `play` (`server/config.go:32`,
    `play/.../firebase/FirebaseService.kt:67-75`), boot, task removal.
- Frequency: 1 per server per device, continuously. Reconnects on every network change / doze exit.

### 1.3 Pattern A2: WebSocket subscription (opt-in)

- `GET <base>/<t1>,...,<tN>/ws?since=<id|none>` with `Upgrade: websocket`, `Connection: Upgrade`,
  `Sec-WebSocket-Version: 13`, `Sec-WebSocket-Key`, `Authorization: Basic` (header), custom headers,
  HTTP/1.1 only (`util/Util.kt:60`, `service/WsConnection.kt:68-84`).
- Client pings every **3 minutes** (`HttpUtil.kt:68`); server pings every 45s and OkHttp auto-pongs. No
  read timeout on the client.
- **Reconnect** (`WsConnection.kt:165-201`):
  - `onFailure` (TCP reset, handshake failure, non-101, ping timeout) -> scheduled via `AlarmManager`
    with backoff `5, 10, 15, 20, 30, 45, 60, 120` s (`:222`). Non-101 with ProtocolException ->
    "WebSocket not supported" shown to user; 401/403 -> not-authorized error.
  - **`onClosed` (clean close handshake) does NOT reconnect** (`:165-170`): state goes to Disconnected
    and nothing reschedules until a network change, a topic change, or service restart (up to 3h).
    The ntfy server never sends a close frame (it just closes TCP), so this only bites if a proxy
    sends a WS close frame (e.g. LB shutting down WS sessions "politely" with 1001). **Test: ntlb must
    never synthesize a WS close frame towards clients; drop TCP instead.**
  - Exact alarms need `SCHEDULE_EXACT_ALARM` on Android 13+; if denied, no reconnect is scheduled at all
    (`:113-124`).
- Frequency: same as A1.

### 1.4 Pattern A3: Poll (one topic per request)

- `GET <base>/<topic>/json?poll=1&since=<lastId|all>` (`util/Util.kt:62`, `msg/ApiService.kt:217-241`),
  default client (15s read, 1 min call). Response read fully, newline-separated JSON.
- Triggers:
  - Periodic `PollWorker` every **60 min** for **all** subscriptions (instant or not), one request each,
    sequentially (`ui/MainActivity.kt:469-488`, `:950`, `work/PollWorker.kt:25-50`).
  - Pull-to-refresh in main list: all subscriptions (`ui/MainActivity.kt:188`, `:766-797`).
  - Subscribe / open detail view (`ui/DetailActivity.kt:240-252`, `:809`).
  - FCM `poll_request` for a topic (`play/.../FirebaseService.kt:62`, `:83-92`) -> unique one-shot
    PollWorker for that topic. ntfy.sh sends `poll_request` instead of message content for topics that
    deny anonymous read (`server/server_firebase.go:156-165`). **Thundering herd**: every FCM device
    subscribed to that topic polls within seconds of each publish.
- Not long-lived.

### 1.5 Pattern A4: Auth check

- `GET <base>/<topic>/auth` with Basic auth, when adding a subscription / user (`util/Util.kt:61`,
  `msg/ApiService.kt:268-287`). 200 = ok, 401/403 = denied, 404 tolerated for old servers (anonymous).

### 1.6 Pattern A5: Publish (from the app's publish UI)

- **`PUT <base>/<topic>?priority=..&tags=..&title=..&delay=..&filename=..&click=..&attach=..&email=..&call=..&markdown=true[&message=..]`**
  (`msg/ApiService.kt:130-215`). All options are **query params**, not headers.
  - Text: body = message (UTF-8), Content-Length known, `Content-Type` none/`text/plain`.
  - Attachment: body = file stream with `Content-Type: <mime>` and `Content-Length: <fileSize>`
    (`ui/PublishFragment.kt:390-410`); the message moves to `?message=` with `\n` escaped as `\\n`
    (`ApiService.kt:180-182`). **Request line can be up to ~13 KB** (4096-byte message, URL-encoded
    UTF-8 can triple). If file size is unknown (-1), OkHttp sends chunked (h1) / no length (h2).
  - Long-call client: 5-min call timeout (`HttpUtil.kt:32-36`). Upload progress via `ProgressRequestBody`.
  - No `Expect: 100-continue` (OkHttp never sends it unless set).
- Rare (user-initiated).

### 1.7 Pattern A6: Attachment / icon download

- `GET <attachment.url>` (usually `<base>/file/<id>.<ext>`) with Basic auth + custom headers if the URL's
  base matches a known server (`msg/DownloadAttachmentWorker.kt:61-68`), long-call client. No Range,
  no HEAD. Icons: `GET <icon url>` with default client (`msg/DownloadIconWorker.kt:65-72`).
- Auto-download on message receipt (if enabled, size-capped) -> bursts right after a popular publish.

### 1.8 Pattern A7: User actions ("http" action buttons)

- Arbitrary method/URL/headers/body from the message (`msg/UserActionWorker.kt:84`). Can target the ntfy
  server itself (e.g. `PUT /<topic>` or `/<topic>/<sid>/clear`), including HEAD.

### 1.9 Third-party traffic caused by Android (UnifiedPush)

- UP app servers (Mastodon, Synapse via Matrix gateway, Tusky backends, common-proxies) get the endpoint
  `<base>/up<12chars>?up=1` (`util/Util.kt:58`, `up/BroadcastReceiver.kt:66-80`) and then:
  - `GET /upXXXXXXXXXXXX?up=1` -> discovery JSON `{"unifiedpush":{"version":1}}` (`server/server.go:922-928`).
  - `POST /upXXXXXXXXXXXX?up=1` (or `PUT`) with **binary** body (WebPush RFC 8291 ciphertext, up to 4 KB),
    `Content-Type: application/octet-stream`, often `Content-Encoding: aes128gcm`, `TTL:`, `Urgency:`,
    `Topic:` headers. The server treats `aes128gcm` as UnifiedPush (`server/server.go:1522-1524`).
    **The LB must not touch request Content-Encoding** (never try to decode it) and must pass binary
    bodies unmodified.
  - Matrix: Synapse posts `POST /_matrix/push/v1/notify` with JSON `{"notification":{...,"devices":[{"pushkey":"https://ntfy.sh/upXXX?up=1",...}]}}`;
    `GET /_matrix/push/v1/notify` = discovery (`server/server.go:880`, `:892`, `server/server_matrix.go`).
  - These publishers are servers with a few IPs and high volume (one Mastodon instance pushes for all its
    users) -- sticky-by-IP routing concentrates them on one node.

---

## 2. iOS app (Swift, URLSession)

- **User-Agent**: `ntfy/<version> (build <build>; iOS <osVersion>)` (`ios/ntfy/Utils/ApiService.swift:5`).
  ios-nextgen uses the same format (`ios-nextgen/ntfy/Utils/ApiService.swift:6`).
- **Auth**: `Authorization: Basic base64(user:pass)` (`ApiService.swift:152-175`).
- **No long-lived connections at all.** iOS relies on APNs (via FCM on ntfy.sh) and polls on demand.
- **HTTP version**: URLSession negotiates h2 via ALPN (and h3 if the server advertises `Alt-Svc`). A new
  `URLSession` is created per request (`ApiService.swift:161-166`), so no connection reuse.
  URLSession sends `Accept: */*`, `Accept-Language`, `Accept-Encoding: gzip, deflate, br`.

| # | Method / path | Trigger | Ref |
|---|---|---|---|
| I1 | `GET /<topic>/json?poll=1&since=<lastId\|all>` | app foreground, list view appear, pull-to-refresh, after publish, background `~poll` push (fan-out over **all** subscriptions concurrently, `DispatchGroup`) | `ApiService.swift:9-19`, `ntfy/App/AppDelegate.swift:95-135`, `ntfy/Views/Notifications/NotificationListView.swift:38,220`, `ntfy/Views/Subscriptions/SubscriptionListView.swift:88` |
| I2 | `GET /<topic>/json?poll=1&id=<messageId>` | Notification Service Extension on a `poll_request` APNs push (self-hosted servers with `upstream-base-url`, or protected topics on ntfy.sh); timeout 30s | `ApiService.swift:21-53`, `ntfyNSE/NotificationService.swift:73-95` |
| I3 | `POST /<topic>` with headers `Title:`, `Priority:`, `Tags:` and UTF-8 body; **raw non-ASCII bytes in header values** possible (no RFC 2047 encoding); timeout 10s | user publish | `ApiService.swift:55-82` |
| I4 | `GET /<topic>/auth` | add subscription / user; timeout 10s | `ApiService.swift:84-113` |
| I5 | `GET <attachment url>` (`/file/<id>.<ext>`), Basic auth, 20-30s timeout, size-capped by delegate | NSE image attachment, attachment store | `ntfy/Utils/NotificationContent.swift:95-108`, `ntfy/Utils/AttachmentFileStore.swift:20-40` |
| I6 | arbitrary (http actions) | user taps action | `ntfy/Utils/ActionExecutor.swift:38` |

- The `~poll` background-poll fan-out topic exists on the client (`AppDelegate.swift:11`, `:248-249`) but the
  server-side sender is disabled (`server/server.go:176`). If ever enabled: every iOS device polls every
  subscription at once, a fleet-wide thundering herd.
- Upstream poll forwarding (server to server): a self-hosted ntfy with `upstream-base-url` sends
  `POST https://ntfy.sh/<sha256(topicURL)>` with empty body, `User-Agent: ntfy/<version>`,
  `X-Poll-ID: <id>`, optional `Authorization: Bearer <upstream-access-token>`, 10s timeout
  (`server/server.go:1350-1380`). Many self-hosted servers; each publish = one such POST to ntfy.sh.
- ios-nextgen also polls `/<topic>/json?poll=1&since=all` for "clear through" (`ios-nextgen/ntfy/Utils/ClearThrough.swift:146`).

---

## 3. Web app (React SPA + service worker; browsers)

### 3.1 Page load / static

- `GET /` (or `/<topic>`, `/<host.tld>/<topic>`, `/account/...` SPA routes) -> index.html.
- `GET /config.js` (no-cache), `GET /manifest.webmanifest`, `GET /static/assets/*.js|css` (hashed),
  `GET /static/langs/<lang>.json`, `/static/images/*`, `/static/fonts/*`, `GET /sw.js`.
- Service worker (workbox) precaches the build, serves navigations from cache, and fetches `/config.js`
  `NetworkFirst` (`web/public/sw.js:423-457`).
- Every hour the page fetches `sw.js` with `cache: "no-store"` plus request headers
  `cache: no-store` and `cache-control: no-cache` (`web/src/registerSW.js:4-38`), then `registration.update()`.
- Conditional requests: `If-Modified-Since` against `Last-Modified` = node process start time
  (`server/server.go:165,171`). **Edge case behind an LB**: if a client alternates nodes (IP change), a node
  started earlier than the cached Last-Modified answers 304 even if its build differs (rolling deploy).
  No ETag. `Range` on static files works (206) unless gzip kicks in.
- Gzip on static only when `Accept-Encoding` contains `gzip`; ntfy sets no `Vary: Accept-Encoding`
  (`util/gzip_handler.go:13-30`). Browsers send `gzip, deflate, br, zstd`.
- Notification sounds are bundled mp3s (`web/src/app/utils.js:1-5`) played via `Audio` -> browsers send
  `Range: bytes=0-` with `Accept-Encoding: identity;q=1, *;q=0` -> 206 from `http.FileServer`.
- HTTP version: whatever the browser negotiates; h2 over TLS normally (h3 if `Alt-Svc`).

### 3.2 Pattern W1: WebSocket subscription, **one per topic**

- `new WebSocket("wss://<base>/<topic>/ws[?since=<id>][&auth=<base64url(Basic ...|Bearer ...)>]")`
  (`web/src/app/Connection.js:35-117`, `utils.js:25`). Browsers cannot set headers on WS, hence
  `?auth=`. **Access logs will contain credentials** unless the LB redacts `auth=`.
- One WS per subscription (not comma-joined): a user with 40 topics = 40 WS connections per tab, per
  device (`web/src/app/ConnectionManager.js:43-92`). Topics that have Web Push enabled are excluded
  (`web/src/components/hooks.js:27-34`). The account's `sync_topic` is also subscribed (internal)
  (`hooks.js:106-112`).
- Since: initial `subscription.last` (absent -> no since -> none); updated with each received id, used on
  reconnect (`Connection.js:61`, `:100-110`).
- Keepalive: browser auto-pongs the server's 45s pings; no client pings.
- **Reconnect**: on `onclose` with `wasClean == false` -> backoff `5, 10, 20, 30, 60, 120, 120...` s
  (`Connection.js:5`, `:67-80`). **`wasClean == true` -> no reconnect, ever** (until the page reloads or
  the subscription list changes). Same trap as Android A2: a proxy that completes a WS close handshake
  (any code) silences the web app.
- Browsers send `Origin`, `Sec-WebSocket-Extensions: permessage-deflate` (gorilla upgrader here does not
  enable compression, so it is declined), `Cookie` none (ntfy uses no cookies).
- WS over HTTP/2 (RFC 8441, extended CONNECT with `:protocol = websocket`): Chrome/Firefox use it only if
  the server's h2 SETTINGS advertise `SETTINGS_ENABLE_CONNECT_PROTOCOL`; otherwise they open a separate
  HTTP/1.1 connection for each WS. Verify what ntlb's h2 listener advertises; if it advertises it, ntlb
  must translate extended CONNECT to an h1 Upgrade towards ntfy.

### 3.3 Pattern W2: Poll (fetch, per topic)

- `GET /<topic>/json?poll=1[&since=<id>]` with `Authorization: Basic|Bearer` header
  (`web/src/app/Api.js:15-30`, `utils.js:27-29`, `utils.js:105-113`). Response read as a stream line by
  line (`fetchLinesIterator`, `utils.js:324-354`).
- Triggers: `Poller` 2s after start then **every 5 min, all subscriptions in parallel**
  (`web/src/app/Poller.js:6-40`); on adding a subscription (`pollInBackground`, `hooks.js:150`).
- Cross-origin base URLs (web app on ntfy.sh subscribed to a self-hosted server, or vice versa) ->
  `Authorization` header triggers an **OPTIONS preflight** (`Access-Control-Request-Headers: authorization`).

### 3.4 Pattern W3: Publish

- JSON: `PUT <base>/` with body `{"topic":..,"message":..,...}` (string body => `Content-Type:
  text/plain;charset=UTF-8`), optional auth (`web/src/app/Api.js:32-46`). Used for e.g. quick actions.
- Publish dialog: `PUT <base>/<topic>?title=..&tags=..&priority=..&click=..&attach=..&filename=..&email=..&call=..&delay=..&timezone=..&message=..&markdown=true`
  via XHR with upload progress (`web/src/components/PublishDialog.jsx:125-181`, `Api.js:61-100`).
  Body = the `File` (Content-Length known, `Content-Type` from the file) or the message text.
  Long query strings again (message in query when attaching).
- Cross-origin: PUT is non-simple -> preflight OPTIONS.

### 3.5 Pattern W4: Account API (logged-in users, same origin, Bearer token)

- Login: `POST /v1/account/token` with Basic auth; logout `DELETE /v1/account/token`.
- `GET /v1/account` on load and on every `sync` event received on the sync topic (`hooks.js:37-50`,
  `AccountApi.js:91-93`).
- Token extension: `PATCH /v1/account/token` 45s after start, then **every 15 min**
  (`web/src/app/AccountApi.js:32-33`, `:485-508`); also from the service worker `periodicsync`
  (min 12h, even with no tab open) (`web/public/sw.js:120-150`, `hooks.js:129-165`).
- Others on user action: `POST|PATCH|DELETE /v1/account/subscription`, `PATCH /v1/account/settings`,
  `POST /v1/account/reservation`, `DELETE /v1/account/reservation/<t>`, billing, phone, email, password
  (`web/src/app/utils.js:12-50`, `AccountApi.js:53-437`).
- `GET /v1/config` every **5 min** by `VersionChecker` (`web/src/app/VersionChecker.js:6`, `:48`).

### 3.6 Pattern W5: Web Push registration

- `POST /v1/webpush` JSON `{endpoint, auth, p256dh, topics:[...]}` whenever the topic list or permission
  changes, and on app start (`web/src/app/Api.js:118-133`, `hooks.js:188-206`).
  `DELETE /v1/webpush` JSON `{endpoint}` (DELETE with a body) (`Api.js:135-146`).
- Deliveries then go server -> browser vendor (FCM/Mozilla/Apple), not through the LB.

### 3.7 Pattern W6: Attachments

- `<img src="<base>/file/<id>.<ext>" loading="lazy">` (`web/src/components/Notifications.jsx:394-418`);
  links for others. Cross-origin images are plain GETs (no CORS). No Range, no auth header (images on
  protected servers rely on unguessable IDs).
- Right after a web upload, the web app immediately GETs the image (server retries the cache lookup
  for that case, `server/server.go:1017-1027`). With a multi-node cluster the GET may hit a node
  that has not seen the message row yet.

---

## 4. CLI / Go client (`ntfy publish`, `ntfy subscribe`)

- Uses `http.DefaultClient` (`client/client.go:115`, `:256`): **User-Agent `Go-http-client/1.1`** (h1) or
  `Go-http-client/2.0` (h2 via ALPN over TLS). `Accept-Encoding: gzip` added automatically. No custom UA.
- Headers are RFC 2047 B-encoded (`=?utf-8?b?...?=`) (`client/options.go:188-192`).
- Auth: `Authorization: Basic` (`--user`, config default) or `Bearer` (`--token`), or removed
  (`WithEmptyAuth`) (`client/options.go:103-115`, `cmd/subscribe.go:110-134`, `:223-239`).

| # | Request | Notes | Ref |
|---|---|---|---|
| C1 | `POST /<topic>` with `X-Title`, `X-Priority`, `X-Tags`, `X-Delay`, `X-Click`, `X-Icon`, `X-Actions`, `X-Attach`, `X-Markdown`, `X-Template`, `X-Filename`, `X-Sequence-ID`, `X-Email`, `X-Cache: no`, `X-Firebase: no` | message body from args (`strings.Reader`, Content-Length set) | `client/client.go:100-132`, `client/options.go:22-125` |
| C2 | `POST /<topic>` + `X-Filename`, body = `os.File` (`--file=path`) | **`*os.File` body has no ContentLength in Go -> `Transfer-Encoding: chunked`** on h1, no length on h2; `--file=-` streams stdin, also chunked; arbitrary size; no Expect | `cmd/publish.go:211-233` |
| C3 | `GET /<topic>/json?poll=1[&since=..][&scheduled=1][&message=..&title=..&priority=..&tags=..]` | `ntfy sub --poll` | `client/client.go:134-161`, `client/options.go:129-186` |
| C4 | `GET /<topic>/json[?since=..]` stream | **one stream per topic** (`--from-config` = N streams); reconnect fixed **10s**, no backoff, **same since on every reconnect** (re-replays, or loses messages if since unset) | `client/client.go:228-280` |

- `ntfy webpush`, `ntfy user/access/token/tier` are local server admin commands (DB / admin socket), not HTTP.
- Users also hit the server with plain curl: `curl -d`, `curl -T file` (curl sends
  `Expect: 100-continue` for large uploads), `curl -u`, `curl -s ntfy.sh/topic/json`, `?auth=` in URLs,
  `GET /<topic>/publish?message=...` / `/send` / `/trigger` (webhooks that can only GET), `HEAD /`
  (uptime monitors), `GET /v1/health`.

---

## 5. Frequency summary (per device / client)

| Client | Long-lived | Periodic | Event-driven |
|---|---|---|---|
| Android (jsonhttp, default) | 1 h2 JSON stream per server, all instant topics comma-joined | poll every topic hourly; service restart every 3h | reconnect on every network change; FCM `poll_request` polls; publish; attachment GETs |
| Android (ws) | 1 h1 WS per server, comma-joined; client ping 3 min | same | same |
| Android (play, non-instant only) | none | poll hourly | FCM `poll_request` -> poll |
| iOS | none | none (background `~poll` disabled server-side) | poll on foreground/refresh (all topics, parallel); NSE poll by id per push |
| Web app (tab open) | 1 WS **per topic** (non-webpush topics) + sync topic | poll all topics every 5 min; `/v1/config` every 5 min; token PATCH every 15 min; `sw.js` hourly | account GET on sync; webpush POST on changes |
| Web app (closed, webpush) | none | SW token PATCH (>=12h periodicsync) | none |
| CLI subscriber | 1 h1/h2 JSON stream **per topic** | none | 10s fixed reconnect |
| UP app servers / Matrix | none | none | POST per push (binary, aes128gcm) |
| Self-hosted ntfy (upstream) | none | none | POST to ntfy.sh per message (`X-Poll-ID`) |

---

## 6. Edge cases worth testing at the proxy

Streams and classification
1. `GET /a,b,c/json` with 1, 2, 150 and 151 topics, max-length topics (64 chars) -> request line ~9.8 KB
   (`server/server.go:185`). Android UP-heavy devices produce ~20-150 `upXXXXXXXXXXXX` topics.
2. Duplicate topics `GET /a,a,a/json` (server dedups).
3. Polls on every streaming endpoint: `?poll=1`, `?po=1`, `?x-poll=1`, `?poll=yes|true`, `?poll=0`
   (stream), `?Poll=1` (NOT a poll: query names are lowercase-only), header `Poll: 1` / `X-Poll: 1`,
   header `Poll: 0` + query `poll=1` (header wins -> stream). ntlb's default matcher
   `!flag("x-poll","poll","po")` must read headers first like ntfy (`lb/config/config.go:42-44`).
4. `since=`: empty, `all`, `none`, `latest`, 12-char ID, unknown ID (falls back), unix timestamp,
   duration `10m`/`12h`, invalid (400) (`server/server.go:2087-2112`).
5. `scheduled=1` combined with poll.
6. Filters on streams (`?message=..&priority=4,5&tags=a,b`).
7. `/sse` and `/raw` streams (only third-party clients and browsers' EventSource use them).
8. `/ws` GET **without** `Upgrade` header -> 400 from ntfy (must not be treated as a hijacked stream).
9. `GET /<topic>/auth` with comma lists (`authPathRegex` allows `a,b/auth`).
10. Long-lived stream over h2 (Android, CLI over TLS, browsers' fetch) and over h1 chunked; ensure each
    write is flushed (keepalive line every 45s is the only traffic for hours).
11. Streams with no traffic but keepalives: upstream idle timeout (3m in the ntfy config; ntlb's default is 60s) must exceed 45s keepalive with
    margin; client-side read timeouts: Android 77s, browsers none, CLI none.
12. A client that disappears without FIN (phone in a tunnel): server learns via write failure at the next
    keepalive or WS pong timeout (60s). LB TCP keepalives towards clients matter here.
13. Clean end vs reset when draining: Android JSON reconnects **immediately** on clean end, 5s+ backoff on
    reset/error. Paced drains should close cleanly so clients land elsewhere quickly but spread out.
14. **WS close frames**: Android (`onClosed`) and the web app (`wasClean`) never reconnect after a clean
    close handshake. The LB must close WS by dropping TCP (as ntfy does), never by sending a close frame,
    including on LB drain/reload/shutdown and on upstream failure.
15. 503 from a draining node before the WS 101 -> retry on another node (ntfy returns it pre-upgrade on
    purpose, `server/server.go:1826-1827`).
16. WS over h2 (RFC 8441 extended CONNECT) from browsers if advertised; WS over h1 from Android/OkHttp.
17. WS `permessage-deflate` offered by browsers, not accepted by ntfy; WS auth via `?auth=` (web) vs header
    (Android). Redact `auth=` / `authorization=` from access logs.
18. Many concurrent WS from one IP/browser (one per topic, multiple tabs) -> per-IP connection limits.

Publishing / bodies
19. `PUT`/`POST /<topic>` with: small text; 4 KB+ text (becomes attachment); 15 MB+ attachment (413);
    `Content-Length` vs `Transfer-Encoding: chunked` (Go CLI `--file`, stdin) vs h2 without length;
    slow uploads (mobile); aborted upload mid-body (must not be retried/resent).
20. `Expect: 100-continue` (curl `-T` with large files): LB must forward or answer 100 correctly, and pass
    an early 4xx (413/401/429) without reading the whole body.
21. Very long query strings on publish (`?message=` up to 4096 chars URL-encoded, plus title, actions)
    -> request lines of 10-15 KB (Android `ApiService.kt:180-182`, web `PublishDialog.jsx:158`).
22. Raw UTF-8 bytes in header values (iOS `Title:`), RFC 2047 encoded values (Go client), `X-Actions` with
    JSON or `;`-separated syntax.
23. Binary bodies with `Content-Encoding: aes128gcm` (UnifiedPush WebPush) -> pass through untouched.
24. `GET /<topic>/publish?message=..` / `/send` / `/trigger`, i.e. a GET that writes (not idempotent in
    effect; retrying it on another node after a sent request duplicates a message).
25. `PUT /` JSON publish; `DELETE` with body (`/v1/webpush`); `PATCH` (`/v1/account/*`).
26. Matrix `POST /_matrix/push/v1/notify` JSON and `GET` discovery; `GET /upXXX?up=1` discovery.
27. Stripe webhook `POST /v1/account/billing/webhook` (raw body signature check: body must be byte-exact).

Static / files
28. `HEAD /` (uptime monitors) -> 200 empty; `HEAD /file/<id>` -> Content-Length, no body; `HEAD` on other
    paths -> 404.
29. `Range` on `/file/<id>`: ntfy ignores it (200 full). Range on `/static/*` -> 206.
30. `If-Modified-Since` on `/static/*`, `/sw.js` with nodes started at different times; `/config.js`,
    `/v1/config` are `Cache-Control: no-cache`.
31. `Accept-Encoding: gzip` on static (gzipped, no `Vary`), on streams (never compressed), on
    `/file` (not compressed). If the LB compresses anything, it must exclude streams.
32. OPTIONS preflight from cross-origin web apps on any path (stream URLs, `/`, `/<topic>`, `/v1/*`) with
    `Access-Control-Request-Method: PUT|GET|DELETE|PATCH` and `-Headers: authorization,...`; must reach ntfy
    (or be answered identically), and must not be classified as a stream.
33. `/file/<id>` immediately after an upload on a different node (cluster replication lag).

Client identity / routing
34. `X-Forwarded-For` handling: clients may send their own XFF; ntfy trusts the LB per
    `proxy-trusted-hosts`. IPv6 /64 grouping (Android privacy addresses rotate).
35. Network switch on Android: same device reconnects from a different IP within seconds (lands on a
    different node by IP hash; since=<id> replay makes it lossless with a shared cache DB).
36. Thundering herds: FCM `poll_request` for a popular protected topic (all devices poll at once);
    web app 5-min poll of all topics in parallel; Android hourly poll; fleet reconnect after LB restart
    (Android backoff starts at 5s with no jitter, web at 5s, CLI fixed 10s).
37. mTLS client certificates (Android) and custom headers (Android) against self-hosted deployments.
38. Rate-limit responses (429 with `Retry-After`): no first-party client honors `Retry-After`; they just
    follow their own backoff.
