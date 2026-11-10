// Package config loads and validates the ntlb YAML config file.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
	"heckel.io/ntlb/match"
)

// Defaults, mostly mirroring the nginx + lbagent setup on ntfy.sh
const (
	DefaultFile                    = "/etc/ntlb/config.yml"
	DefaultListenAdmin             = "127.0.0.1:2590"
	DefaultUpstreamConnectTimeout  = 5 * time.Second // Private network: a node that does not accept within 5s is down
	DefaultUpstreamReadTimeout     = 3 * time.Minute // ntfy sends a keepalive every 45s, so 3m of silence is a dead stream
	DefaultDrainDuration           = 5 * time.Minute
	DefaultShutdownDrainDuration   = 2 * time.Minute
	DefaultHealthPath              = "/v1/ready"
	DefaultHealthInterval          = 5 * time.Second
	DefaultHealthTimeout           = 3 * time.Second
	DefaultHealthFails             = 3 // Failed probes in a row before an upstream leaves the rotation
	DefaultHealthPasses            = 2 // Good probes in a row before it comes back
	DefaultKeepaliveIdle           = time.Minute
	DefaultKeepaliveInterval       = 15 * time.Second
	DefaultKeepaliveCount          = 4
	DefaultClientKeyIPv4PrefixBits = 32 // Like ntfy's visitor-prefix-bits-ipv4
	DefaultClientKeyIPv6PrefixBits = 64 // Like ntfy's visitor-prefix-bits-ipv6: privacy addresses of one client stay together
	DefaultBansWindow              = 10 * time.Minute
	maxPortRange                   = 1000 // Ports in one "host:from-to" address, against typos like 2586-25860

	// DefaultStreamsMatch are ntfy's subscriptions: GET /<topics>/<json|sse|raw|ws>, minus polls
	// (poll=1 as a query parameter or header, read like ntfy's readParam)
	DefaultStreamsMatch = `method == "GET" && path matches "^/[-_A-Za-z0-9]{1,64}(,[-_A-Za-z0-9]{1,64})*/(json|sse|raw|ws)$" && !flag("x-poll", "poll", "po")`
)

var (
	upstreamNameRegex = regexp.MustCompile(`^[-_.a-zA-Z0-9]{1,64}$`)
)

// Config is the ntlb config file. Field names follow ntfy's server.yml (kebab-case).
type Config struct {
	ListenHTTP             string        `yaml:"listen-http"`
	ListenHTTPS            string        `yaml:"listen-https"`
	ListenAdmin            string        `yaml:"listen-admin"`   // Admin API, status, metrics, pprof; no auth
	ListenMetrics          string        `yaml:"listen-metrics"` // Only /metrics, e.g. for Prometheus on a private network
	AccessLogs             []*AccessLog  `yaml:"access-logs"`
	ClientKeepalive        *Keepalive    `yaml:"client-keepalive"`
	UpstreamKeepalive      *Keepalive    `yaml:"upstream-keepalive"`
	UpstreamConnectTimeout time.Duration `yaml:"upstream-connect-timeout"`
	UpstreamReadTimeout    time.Duration `yaml:"upstream-read-timeout"`
	DrainDuration          time.Duration `yaml:"drain-duration"`
	ShutdownDrainDuration  time.Duration `yaml:"shutdown-drain-duration"`
	ClientKey              *ClientKey    `yaml:"client-key"`
	Bans                   *Bans         `yaml:"bans"`
	Rules                  []*Rule       `yaml:"rules"`   // Checked for every request, before its site's rules
	Include                []string      `yaml:"include"` // Globs of site files, relative to the config file's directory
	Sites                  []*Site       `yaml:"sites"`

	sites *siteIndex // Sites by host, see SiteFor
}

// AccessLog is one access log file. A request is logged to the first file whose match (a response
// expression, see package match) is true; a file without match takes every request that gets there.
type AccessLog struct {
	File    string         `yaml:"file"`
	Match   string         `yaml:"match"`
	Program *match.Program `yaml:"-"`
}

// Keepalive configures TCP keepalive probes on a connection (see net.KeepAliveConfig)
type Keepalive struct {
	Disable  bool          `yaml:"disable"`
	Idle     time.Duration `yaml:"idle"`
	Interval time.Duration `yaml:"interval"`
	Count    int           `yaml:"count"`
}

// HealthCheck configures the active health checks against every upstream
type HealthCheck struct {
	Path     string        `yaml:"path"`
	Interval time.Duration `yaml:"interval"`
	Timeout  time.Duration `yaml:"timeout"`
	Fails    int           `yaml:"fails"`
	Passes   int           `yaml:"passes"`
}

// Streams decides which requests are long-lived streams (a request expression; bodyless requests
// only). Streams take the lean stream path, a draining load balancer turns new ones away, and
// drains close them at a pace. The default is ntfy's subscriptions; "false" turns it off.
type Streams struct {
	Match   string         `yaml:"match"`
	Program *match.Program `yaml:"-"`
}

// ClientKey is what identifies a client for sticky routing, rate limits and bans: its IP,
// shortened to a prefix
type ClientKey struct {
	IPv4PrefixBits int `yaml:"ipv4-prefix-bits"`
	IPv6PrefixBits int `yaml:"ipv6-prefix-bits"`
}

// Bans counts the strikes of rules per client key. A client that collects Threshold strikes
// within Window gets one line in File, which a ban tool (e.g. CrowdSec) turns into a ban.
type Bans struct {
	File      string        `yaml:"file"`
	Window    time.Duration `yaml:"window"`
	Threshold float64       `yaml:"threshold"`
}

// Upstream is one ntfy node. The name, not the address, is what clients are hashed to, so every
// load balancer must use the same names for the same nodes to route a client the same way. An
// address may be a port range ("10.0.0.1:2586-2589"), one address per port.
type Upstream struct {
	Name      string   `yaml:"name"`
	Addresses []string `yaml:"addresses"`
}

// Load reads, defaults and validates a config file and the site files it includes
func Load(filename string) (*Config, error) {
	b, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	return parse(b, filepath.Dir(filename))
}

// Parse defaults and validates a config from YAML. Unknown fields are rejected, so that a typo
// does not silently fall back to a default. Includes are relative to the working directory.
func Parse(b []byte) (*Config, error) {
	return parse(b, ".")
}

func parse(b []byte, dir string) (*Config, error) {
	c := &Config{}
	if err := decode(b, c); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	if err := c.loadIncludes(dir); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	c.applyDefaults()
	if err := c.validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	return c, nil
}

// decode decodes YAML strictly; an empty document leaves v as it is
func decode(b []byte, v any) error {
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	if err := dec.Decode(v); err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}

// RestartRequired returns the settings that differ between c and other but that a reload cannot
// apply, because they belong to the listening sockets
func (c *Config) RestartRequired(other *Config) []string {
	var settings []string
	if c.ListenHTTP != other.ListenHTTP {
		settings = append(settings, "listen-http")
	}
	if c.ListenHTTPS != other.ListenHTTPS {
		settings = append(settings, "listen-https")
	}
	if c.ListenAdmin != other.ListenAdmin {
		settings = append(settings, "listen-admin")
	}
	if c.ListenMetrics != other.ListenMetrics {
		settings = append(settings, "listen-metrics")
	}
	if *c.ClientKeepalive != *other.ClientKeepalive {
		settings = append(settings, "client-keepalive")
	}
	return settings
}

// KeepAliveConfig converts the keepalive settings to a net.KeepAliveConfig
func (k *Keepalive) KeepAliveConfig() net.KeepAliveConfig {
	if k.Disable {
		return net.KeepAliveConfig{Enable: false}
	}
	return net.KeepAliveConfig{
		Enable:   true,
		Idle:     k.Idle,
		Interval: k.Interval,
		Count:    k.Count,
	}
}

func (c *Config) applyDefaults() {
	if c.ListenAdmin == "" {
		c.ListenAdmin = DefaultListenAdmin
	}
	if c.UpstreamConnectTimeout == 0 {
		c.UpstreamConnectTimeout = DefaultUpstreamConnectTimeout
	}
	if c.UpstreamReadTimeout == 0 {
		c.UpstreamReadTimeout = DefaultUpstreamReadTimeout
	}
	if c.DrainDuration == 0 {
		c.DrainDuration = DefaultDrainDuration
	}
	if c.ShutdownDrainDuration == 0 {
		c.ShutdownDrainDuration = DefaultShutdownDrainDuration
	}
	if c.ClientKeepalive == nil {
		c.ClientKeepalive = &Keepalive{}
	}
	if c.UpstreamKeepalive == nil {
		c.UpstreamKeepalive = &Keepalive{}
	}
	c.ClientKeepalive.applyDefaults()
	c.UpstreamKeepalive.applyDefaults()
	if c.ClientKey == nil {
		c.ClientKey = &ClientKey{}
	}
	if c.ClientKey.IPv4PrefixBits == 0 {
		c.ClientKey.IPv4PrefixBits = DefaultClientKeyIPv4PrefixBits
	}
	if c.ClientKey.IPv6PrefixBits == 0 {
		c.ClientKey.IPv6PrefixBits = DefaultClientKeyIPv6PrefixBits
	}
	if c.Bans != nil && c.Bans.Window == 0 {
		c.Bans.Window = DefaultBansWindow
	}
	applyRuleDefaults(c.Rules)
	for _, site := range c.Sites {
		if site != nil {
			site.applyDefaults()
		}
	}
}

func (c *Config) validate() error {
	if c.ListenHTTP == "" && c.ListenHTTPS == "" {
		return errors.New("listen-http or listen-https must be set")
	}
	if c.UpstreamConnectTimeout <= 0 {
		return errors.New("upstream-connect-timeout must be positive")
	} else if c.UpstreamReadTimeout <= 0 {
		return errors.New("upstream-read-timeout must be positive")
	}
	if err := c.ClientKeepalive.validate("client-keepalive"); err != nil {
		return err
	} else if err := c.UpstreamKeepalive.validate("upstream-keepalive"); err != nil {
		return err
	}
	if c.DrainDuration < 0 || c.ShutdownDrainDuration < 0 {
		return errors.New("drain durations must not be negative")
	}
	if c.ClientKey.IPv4PrefixBits < 1 || c.ClientKey.IPv4PrefixBits > 32 {
		return errors.New("client-key.ipv4-prefix-bits must be between 1 and 32")
	}
	if c.ClientKey.IPv6PrefixBits < 1 || c.ClientKey.IPv6PrefixBits > 128 {
		return errors.New("client-key.ipv6-prefix-bits must be between 1 and 128")
	}
	if err := c.validateAccessLogs(); err != nil {
		return err
	}
	if err := c.validateBans(); err != nil {
		return err
	}
	if err := validateRules(c.Rules, "", c.Bans); err != nil {
		return err
	}
	return c.validateSites()
}

func (c *Config) validateAccessLogs() error {
	for i, l := range c.AccessLogs {
		if l == nil || l.File == "" {
			return fmt.Errorf("access-logs[%d]: file is required", i)
		}
		if l.Match != "" {
			program, err := match.Compile(l.Match, match.Response)
			if err != nil {
				return fmt.Errorf("access-logs[%d]: invalid match: %w", i, err)
			}
			l.Program = program
		}
	}
	return nil
}

func (c *Config) validateBans() error {
	if c.Bans == nil {
		return nil
	} else if c.Bans.File == "" {
		return errors.New("bans.file is required")
	} else if c.Bans.Threshold <= 0 {
		return errors.New("bans.threshold must be positive")
	} else if c.Bans.Window <= 0 {
		return errors.New("bans.window must be positive")
	}
	return nil
}

// expandAddresses checks host:port addresses and expands port ranges (host:from-to)
func expandAddresses(addresses []string) ([]string, error) {
	var expanded []string
	seen := make(map[string]bool)
	for _, addr := range addresses {
		host, port, err := net.SplitHostPort(addr)
		if err != nil || host == "" || port == "" {
			return nil, fmt.Errorf("invalid address %q, expected host:port or host:from-to", addr)
		}
		from, to, isRange := strings.Cut(port, "-")
		if !isRange {
			to = from
		}
		first, err1 := strconv.Atoi(from)
		last, err2 := strconv.Atoi(to)
		if err1 != nil || err2 != nil || first < 1 || last > 65535 || last < first || last-first >= maxPortRange {
			return nil, fmt.Errorf("invalid address %q, expected host:port or host:from-to", addr)
		}
		for p := first; p <= last; p++ {
			a := net.JoinHostPort(host, strconv.Itoa(p))
			if !isRange {
				a = addr // As written, e.g. a hostname
			}
			if seen[a] {
				return nil, fmt.Errorf("duplicate address %s", a)
			}
			seen[a] = true
			expanded = append(expanded, a)
		}
	}
	return expanded, nil
}

func (k *Keepalive) applyDefaults() {
	if k.Idle == 0 {
		k.Idle = DefaultKeepaliveIdle
	}
	if k.Interval == 0 {
		k.Interval = DefaultKeepaliveInterval
	}
	if k.Count == 0 {
		k.Count = DefaultKeepaliveCount
	}
}

// validate checks the values after defaulting, so a zero here was written as a negative
func (k *Keepalive) validate(section string) error {
	if k.Idle <= 0 {
		return fmt.Errorf("%s.idle must be positive", section)
	} else if k.Interval <= 0 {
		return fmt.Errorf("%s.interval must be positive", section)
	} else if k.Count <= 0 {
		return fmt.Errorf("%s.count must be positive", section)
	}
	return nil
}

func (h *HealthCheck) applyDefaults() {
	if h.Path == "" {
		h.Path = DefaultHealthPath
	}
	if h.Interval == 0 {
		h.Interval = DefaultHealthInterval
	}
	if h.Timeout == 0 {
		h.Timeout = DefaultHealthTimeout
	}
	if h.Fails == 0 {
		h.Fails = DefaultHealthFails
	}
	if h.Passes == 0 {
		h.Passes = DefaultHealthPasses
	}
}

// validate checks the values after defaulting. A non-positive interval would probe in a tight loop.
func (h *HealthCheck) validate() error {
	if h.Interval <= 0 {
		return errors.New("health-check.interval must be positive")
	} else if h.Timeout <= 0 {
		return errors.New("health-check.timeout must be positive")
	} else if h.Timeout >= h.Interval {
		return errors.New("health-check.timeout must be shorter than health-check.interval")
	} else if h.Fails <= 0 {
		return errors.New("health-check.fails must be positive")
	} else if h.Passes <= 0 {
		return errors.New("health-check.passes must be positive")
	}
	return nil
}
