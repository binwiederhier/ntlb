package server

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"heckel.io/ntfy-lb/balancer"
)

// Request kinds, the "kind" label
const (
	kindRequest   = "request"   // Anything that is not a subscription
	kindStream    = "stream"    // A subscription on the stream path
	kindWebSocket = "websocket" // A subscription upgraded to a WebSocket
)

// Stream protocols, the "protocol" label of ntfy_lb_streams
const (
	protocolHTTP1     = "http1"
	protocolHTTP2     = "http2"
	protocolWebSocket = "websocket"
)

// Reasons for ntfy_lb_retries_total and ntfy_lb_streams_closed_total
const (
	retryConnect       = "connect" // The upstream refused or did not accept the connection
	retryError         = "error"   // Any other error before a response, for idempotent requests
	retry503           = "503"     // The upstream answered 503 (e.g. a draining ntfy node)
	closedDrained      = "drained"
	closedReadTimeout  = "read_timeout"
	metricsNamespace   = "ntfy_lb"
	streamDurationMax  = 24 * time.Hour
	requestDurationMax = 5 * time.Minute
)

// metrics are the load balancer's Prometheus metrics. Counters and histograms are updated as
// requests end; the gauges that mirror the balancer's state are read from its Status on scrape.
type metrics struct {
	registry        *prometheus.Registry
	requests        *prometheus.CounterVec
	requestDuration *prometheus.HistogramVec
	streamDuration  prometheus.Histogram
	streams         *prometheus.GaugeVec
	retries         *prometheus.CounterVec
	refused         prometheus.Counter
	streamsClosed   *prometheus.CounterVec
	ruleMatches     *prometheus.CounterVec
}

// statusCollector exports the balancer's Status as gauges and counters
type statusCollector struct {
	balancer         *balancer.Balancer
	ready            *prometheus.Desc
	draining         *prometheus.Desc
	openRequests     *prometheus.Desc
	upstreamHealthy  *prometheus.Desc
	upstreamDraining *prometheus.Desc
	upstreamStreams  *prometheus.Desc
	upstreamRequests *prometheus.Desc
	upstreamFailures *prometheus.Desc
}

func newMetrics(b *balancer.Balancer) *metrics {
	m := &metrics{
		registry: prometheus.NewRegistry(),
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "requests_total",
			Help:      "Requests by upstream (empty if none answered), status code and kind (request, stream, websocket)",
		}, []string{"upstream", "code", "kind"}),
		requestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: metricsNamespace,
			Name:      "request_duration_seconds",
			Help:      "Duration of requests that are not streams (kind request), from the first byte to the end",
			Buckets:   []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, requestDurationMax.Seconds()},
		}, []string{"kind"}),
		streamDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: metricsNamespace,
			Name:      "stream_duration_seconds",
			Help:      "Lifetime of subscription streams (stream path and WebSockets) that reached an upstream",
			Buckets:   []float64{1, 10, 60, 300, 900, 1800, 3600, 3 * 3600, 6 * 3600, 12 * 3600, streamDurationMax.Seconds()},
		}),
		streams: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "streams",
			Help:      "Open subscription streams by client protocol (http1, http2, websocket)",
		}, []string{"protocol"}),
		retries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "retries_total",
			Help:      "Requests sent to the next upstream, by reason (connect, error, 503)",
		}, []string{"reason"}),
		refused: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "refused_total",
			Help:      "Requests turned away with 503 because the load balancer drains (new subscriptions, or anything once drained)",
		}),
		streamsClosed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "streams_closed_total",
			Help:      "Subscription streams the load balancer closed, by reason (drained, read_timeout)",
		}, []string{"reason"}),
		ruleMatches: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "rule_matches_total",
			Help:      "Requests a rule blocked or limited, by rule",
		}, []string{"rule"}),
	}
	for _, protocol := range []string{protocolHTTP1, protocolHTTP2, protocolWebSocket} {
		m.streams.WithLabelValues(protocol)
	}
	for _, reason := range []string{retryConnect, retryError, retry503} {
		m.retries.WithLabelValues(reason)
	}
	for _, reason := range []string{closedDrained, closedReadTimeout} {
		m.streamsClosed.WithLabelValues(reason)
	}
	m.registry.MustRegister(
		m.requests,
		m.requestDuration,
		m.streamDuration,
		m.streams,
		m.retries,
		m.refused,
		m.streamsClosed,
		m.ruleMatches,
		newStatusCollector(b),
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return m
}

// observe records a request that ended
func (m *metrics) observe(pr *proxyRequest, status int, start time.Time) {
	m.requests.WithLabelValues(pr.upstream, strconv.Itoa(status), pr.kind).Inc()
	elapsed := time.Since(start).Seconds()
	if pr.kind == kindRequest {
		m.requestDuration.WithLabelValues(pr.kind).Observe(elapsed)
	} else if pr.upstream != "" {
		m.streamDuration.Observe(elapsed)
	}
}

func newStatusCollector(b *balancer.Balancer) *statusCollector {
	desc := func(name, help string, labels ...string) *prometheus.Desc {
		return prometheus.NewDesc(prometheus.BuildFQName(metricsNamespace, "", name), help, labels, nil)
	}
	return &statusCollector{
		balancer:         b,
		ready:            desc("ready", "Whether the load balancer is ready (healthy, not draining)"),
		draining:         desc("draining", "Whether the load balancer is draining"),
		openRequests:     desc("open_requests", "Open proxied requests, streams included"),
		upstreamHealthy:  desc("upstream_healthy", "Whether the upstream passes its health checks", "upstream"),
		upstreamDraining: desc("upstream_draining", "Whether the upstream is draining or drained", "upstream"),
		upstreamStreams:  desc("upstream_streams", "Open proxied requests per upstream", "upstream"),
		upstreamRequests: desc("upstream_requests_total", "Requests the upstream answered", "upstream"),
		upstreamFailures: desc("upstream_failures_total", "Requests the upstream failed (connection errors, timeouts)", "upstream"),
	}
}

func (c *statusCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{c.ready, c.draining, c.openRequests, c.upstreamHealthy, c.upstreamDraining, c.upstreamStreams, c.upstreamRequests, c.upstreamFailures} {
		ch <- d
	}
}

func (c *statusCollector) Collect(ch chan<- prometheus.Metric) {
	status := c.balancer.Status()
	ch <- prometheus.MustNewConstMetric(c.ready, prometheus.GaugeValue, boolFloat(status.Ready))
	ch <- prometheus.MustNewConstMetric(c.draining, prometheus.GaugeValue, boolFloat(status.Draining))
	ch <- prometheus.MustNewConstMetric(c.openRequests, prometheus.GaugeValue, float64(status.Streams))
	for _, u := range status.Upstreams {
		draining := u.State == balancer.StateDraining || u.State == balancer.StateDrained
		ch <- prometheus.MustNewConstMetric(c.upstreamHealthy, prometheus.GaugeValue, boolFloat(u.Healthy), u.Name)
		ch <- prometheus.MustNewConstMetric(c.upstreamDraining, prometheus.GaugeValue, boolFloat(draining), u.Name)
		ch <- prometheus.MustNewConstMetric(c.upstreamStreams, prometheus.GaugeValue, float64(u.Streams), u.Name)
		ch <- prometheus.MustNewConstMetric(c.upstreamRequests, prometheus.CounterValue, float64(u.Requests), u.Name)
		ch <- prometheus.MustNewConstMetric(c.upstreamFailures, prometheus.CounterValue, float64(u.Failures), u.Name)
	}
}

func boolFloat(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// streamProtocol is the "protocol" label of a subscription
func streamProtocol(kind string, protoMajor int) string {
	switch {
	case kind == kindWebSocket:
		return protocolWebSocket
	case protoMajor >= 2:
		return protocolHTTP2
	default:
		return protocolHTTP1
	}
}
