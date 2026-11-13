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
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
	"heckel.io/ntlb/match"
)

// Balancing strategies, a site's balance
const (
	BalanceRoundRobin = "round-robin"
	BalanceLeastConn  = "least-conn"
	BalanceRandomTwo  = "random-two-least-conn"
	BalanceHash       = "hash"
)

// Defaults, like nginx's where it has one
const (
	DefaultFile                    = "/etc/ntlb/config.yml"
	DefaultListenAdmin             = "127.0.0.1:2590"
	DefaultUpstreamConnectTimeout  = 5 * time.Second // Upstreams are usually on a private network: one that does not accept within 5s is down
	DefaultUpstreamReadTimeout     = time.Minute     // nginx's proxy_read_timeout; streams need their upstream to send keepalives within it
	DefaultDrainDuration           = 5 * time.Minute
	DefaultShutdownDrainDuration   = 2 * time.Minute
	DefaultHealthPath              = "/" // Any 2xx or 3xx answer is healthy
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
	DefaultBalance                 = BalanceRoundRobin // Like nginx
	maxPortRange                   = 1000              // Ports in one "host:from-to" address, against typos like 2586-25860
	maxWeight                      = 1000              // A server's weight: plenty for shares, and bounds a round-robin round

	// DefaultStreamsMatch is no request: which ones are long-lived streams depends on the app (see
	// packaging/sites.d/ntfy.yml for ntfy's subscriptions)
	DefaultStreamsMatch = "false"

	// UpstreamDefault is the upstream of a site that takes every request no proxy rule routes
	UpstreamDefault = "default"
)

var (
	nameRegex      = regexp.MustCompile(`^[-_.a-zA-Z0-9]{1,64}$`)      // Names of sites, servers and rules
	mediaTypeRegex = regexp.MustCompile(`^[-+.a-z0-9]+/[-+.a-z0-9]+$`) // gzip.types
	balances       = []string{BalanceRoundRobin, BalanceLeastConn, BalanceRandomTwo, BalanceHash}
)

// Config is the ntlb config file. Field names follow ntfy's server.yml (kebab-case).
type Config struct {
	ListenAdmin           string        `yaml:"listen-admin"`    // Admin API, status, metrics, pprof; no auth
	ListenMetrics         string        `yaml:"listen-metrics"`  // Only /metrics, e.g. for Prometheus on a private network
	ListenHealth          string        `yaml:"listen-health"`   // Only /v1/ready, e.g. for Route53 health checks
	ListenProfiler        string        `yaml:"listen-profiler"` // Only /debug/pprof/, e.g. on a private network
	AccessLogs            []*AccessLog  `yaml:"access-logs"`     // For sites without their own, and requests no site serves
	ClientKeepalive       *Keepalive    `yaml:"client-keepalive"`
	DrainDuration         time.Duration `yaml:"drain-duration"`
	ShutdownDrainDuration time.Duration `yaml:"shutdown-drain-duration"`
	ShutdownDrainDelay    time.Duration `yaml:"shutdown-drain-delay"` // Withdrawing before the drain: not ready, still serving
	ClientKey             *ClientKey    `yaml:"client-key"`
	Bans                  *Bans         `yaml:"bans"`
	Gzip                  *Gzip         `yaml:"gzip"`    // For sites without their own
	Errors                *ErrorPages   `yaml:"errors"`  // For sites without their own, and requests no site serves
	Rules                 []*Rule       `yaml:"rules"`   // Checked for every request, before its site's rules
	Include               []string      `yaml:"include"` // Globs of files merged into this one, relative to its directory
	Sites                 []*Site       `yaml:"sites"`

	Listeners []*Listener `yaml:"-"` // The sites' listen addresses, in order of first use
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

// Gzip compresses responses of the listed media types for clients that accept it, like nginx's
// gzip with gzip_proxied any
type Gzip struct {
	Types []string `yaml:"types"` // Media types without parameters, e.g. text/html
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

// RestartRequired returns the settings that differ between c and other but that a reload cannot
// apply: the admin, metrics, health and profiler listeners, and the keepalives of the sites' listeners.
// (A reload binds and closes the sites' listen addresses.)
func (c *Config) RestartRequired(other *Config) []string {
	var settings []string
	if c.ListenAdmin != other.ListenAdmin {
		settings = append(settings, "listen-admin")
	}
	if c.ListenMetrics != other.ListenMetrics {
		settings = append(settings, "listen-metrics")
	}
	if c.ListenHealth != other.ListenHealth {
		settings = append(settings, "listen-health")
	}
	if c.ListenProfiler != other.ListenProfiler {
		settings = append(settings, "listen-profiler")
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

func (c *Config) applyDefaults() {
	if c.ListenAdmin == "" {
		c.ListenAdmin = DefaultListenAdmin
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
	c.ClientKeepalive.applyDefaults()
	if c.ClientKey == nil {
		c.ClientKey = &ClientKey{}
	}
	if c.ClientKey.IPv4PrefixBits == 0 {
		c.ClientKey.IPv4PrefixBits = DefaultClientKeyIPv4PrefixBits
	}
	if c.ClientKey.IPv6PrefixBits == 0 {
		c.ClientKey.IPv6PrefixBits = DefaultClientKeyIPv6PrefixBits
	}
	c.Bans.applyDefaults()
	if c.Errors == nil {
		c.Errors = &ErrorPages{}
	}
	c.Errors.applyDefaults()
	applyRuleDefaults(c.Rules)
	for _, site := range c.Sites {
		if site != nil {
			site.applyDefaults()
		}
	}
}

func (c *Config) validate() error {
	if err := c.ClientKeepalive.validate("client-keepalive"); err != nil {
		return err
	}
	if c.DrainDuration < 0 || c.ShutdownDrainDuration < 0 || c.ShutdownDrainDelay < 0 {
		return errors.New("drain durations must not be negative")
	}
	if c.ClientKey.IPv4PrefixBits < 1 || c.ClientKey.IPv4PrefixBits > 32 {
		return errors.New("client-key.ipv4-prefix-bits must be between 1 and 32")
	}
	if c.ClientKey.IPv6PrefixBits < 1 || c.ClientKey.IPv6PrefixBits > 128 {
		return errors.New("client-key.ipv6-prefix-bits must be between 1 and 128")
	}
	if err := validateAccessLogs(c.AccessLogs); err != nil {
		return err
	}
	if err := c.Gzip.validate(); err != nil {
		return err
	}
	if err := c.Errors.validate(); err != nil {
		return err
	}
	if err := validateBans(c.Bans); err != nil {
		return err
	}
	if err := validateRules(c.Rules, nil, c.anyBans()); err != nil {
		return err
	}
	return c.validateSites()
}

func validateAccessLogs(logs []*AccessLog) error {
	for i, l := range logs {
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

func (b *Bans) applyDefaults() {
	if b != nil && b.Window == 0 {
		b.Window = DefaultBansWindow
	}
}

// anyBans reports whether there is a bans section at all, globally or in a site: the global
// rules' strikes count for the requests of the sites that have one
func (c *Config) anyBans() bool {
	if c.Bans != nil {
		return true
	}
	for _, site := range c.Sites {
		if site != nil && site.Bans != nil {
			return true
		}
	}
	return false
}

// validate lowercases the types and checks them; nil is no compression
func (g *Gzip) validate() error {
	if g == nil {
		return nil
	} else if len(g.Types) == 0 {
		return errors.New("gzip.types is required")
	}
	for i, typ := range g.Types {
		g.Types[i] = strings.ToLower(typ)
		if !mediaTypeRegex.MatchString(g.Types[i]) {
			return fmt.Errorf("gzip.types: invalid media type %q, expected e.g. text/html without parameters", typ)
		}
	}
	return nil
}

func validateBans(b *Bans) error {
	if b == nil {
		return nil
	} else if b.File == "" {
		return errors.New("bans.file is required")
	} else if b.Threshold <= 0 {
		return errors.New("bans.threshold must be positive")
	} else if b.Window <= 0 {
		return errors.New("bans.window must be positive")
	}
	return nil
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
