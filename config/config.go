// Package config loads and validates the ntfy-lb YAML config file.
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
	"time"

	"gopkg.in/yaml.v3"
)

// Defaults, mostly mirroring the nginx + lbagent setup on ntfy.sh
const (
	DefaultFile                   = "/etc/ntfy-lb/config.yml"
	DefaultListenAdmin            = "127.0.0.1:2590"
	DefaultUpstreamConnectTimeout = 5 * time.Second // Private network: a node that does not accept within 5s is down
	DefaultUpstreamReadTimeout    = 3 * time.Minute // ntfy sends a keepalive every 45s, so 3m of silence is a dead stream
	DefaultDrainDuration          = 5 * time.Minute
	DefaultShutdownDrainDuration  = 2 * time.Minute
	DefaultHealthPath             = "/v1/ready"
	DefaultHealthInterval         = 5 * time.Second
	DefaultHealthTimeout          = 3 * time.Second
	DefaultHealthFails            = 3 // Failed probes in a row before an upstream leaves the rotation
	DefaultHealthPasses           = 2 // Good probes in a row before it comes back
	DefaultKeepaliveIdle          = time.Minute
	DefaultKeepaliveInterval      = 15 * time.Second
	DefaultKeepaliveCount         = 4
	DefaultStreamsPath            = `^/[-_A-Za-z0-9]{1,64}(,[-_A-Za-z0-9]{1,64})*/(json|sse|raw|ws)$` // ntfy subscriptions
	DefaultHashIPv4PrefixBits     = 32                                                                // Like ntfy's visitor-prefix-bits-ipv4
	DefaultHashIPv6PrefixBits     = 64                                                                // Like ntfy's visitor-prefix-bits-ipv6: privacy addresses of one client stay together
)

var (
	// DefaultStreamsExcludeParams are ntfy's poll parameters (query or header), in ntfy's readParam
	// order: a poll is short
	DefaultStreamsExcludeParams = []string{"x-poll", "poll", "po"}

	upstreamNameRegex = regexp.MustCompile(`^[-_.a-zA-Z0-9]{1,64}$`)
)

// Config is the ntfy-lb config file. Field names follow ntfy's server.yml (kebab-case).
type Config struct {
	ListenHTTP             string        `yaml:"listen-http"`
	ListenHTTPS            string        `yaml:"listen-https"`
	CertFile               string        `yaml:"cert-file"`
	KeyFile                string        `yaml:"key-file"`
	ListenAdmin            string        `yaml:"listen-admin"`
	AccessLog              string        `yaml:"access-log"`
	ClientKeepalive        *Keepalive    `yaml:"client-keepalive"`
	UpstreamKeepalive      *Keepalive    `yaml:"upstream-keepalive"`
	UpstreamConnectTimeout time.Duration `yaml:"upstream-connect-timeout"`
	UpstreamReadTimeout    time.Duration `yaml:"upstream-read-timeout"`
	DrainDuration          time.Duration `yaml:"drain-duration"`
	ShutdownDrainDuration  time.Duration `yaml:"shutdown-drain-duration"`
	HealthCheck            *HealthCheck  `yaml:"health-check"`
	Streams                *Streams      `yaml:"streams"`
	Hash                   *Hash         `yaml:"hash"`
	Upstreams              []*Upstream   `yaml:"upstreams"`
	RulesFile              string        `yaml:"rules-file"` // More rules, appended to rules; reloaded with the config
	Rules                  []*Rule       `yaml:"rules"`
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

// Streams decides which requests are long-lived streams: GET requests whose path matches, unless
// the first of the exclude params that is set (headers first, then query parameters, in list
// order, like ntfy's readParam) is 1, yes or true. Streams take the lean stream path, and a
// draining load balancer turns new ones away.
type Streams struct {
	Disable       bool           `yaml:"disable"`
	Path          string         `yaml:"path"`
	ExcludeParams []string       `yaml:"exclude-params"`
	PathRegex     *regexp.Regexp `yaml:"-"`
}

// Hash configures the client key that sticky routing hashes: the client IP, shortened to a prefix
type Hash struct {
	IPv4PrefixBits int `yaml:"ipv4-prefix-bits"`
	IPv6PrefixBits int `yaml:"ipv6-prefix-bits"`
}

// Upstream is one ntfy node. The name, not the address, is what clients are hashed to, so every
// load balancer must use the same names for the same nodes to route a client the same way.
type Upstream struct {
	Name      string   `yaml:"name"`
	Addresses []string `yaml:"addresses"`
}

// Load reads, defaults and validates a config file
func Load(filename string) (*Config, error) {
	b, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	return parse(b, filepath.Dir(filename))
}

// Parse defaults and validates a config from YAML. Unknown fields are rejected, so that a typo
// does not silently fall back to a default.
func Parse(b []byte) (*Config, error) {
	return parse(b, "")
}

// parse is Parse with the directory that a relative rules-file is relative to
func parse(b []byte, dir string) (*Config, error) {
	c := &Config{}
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	if err := dec.Decode(c); err != nil && !errors.Is(err, io.EOF) { // EOF: empty file
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	if err := c.loadRulesFile(dir); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	c.applyDefaults()
	if err := c.validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	return c, nil
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
	if c.HealthCheck == nil {
		c.HealthCheck = &HealthCheck{}
	}
	c.HealthCheck.applyDefaults()
	if c.Streams == nil {
		c.Streams = &Streams{}
	}
	if c.Hash == nil {
		c.Hash = &Hash{}
	}
	if c.Streams.Path == "" {
		c.Streams.Path = DefaultStreamsPath
	}
	if c.Streams.ExcludeParams == nil {
		c.Streams.ExcludeParams = DefaultStreamsExcludeParams
	}
	if c.Hash.IPv4PrefixBits == 0 {
		c.Hash.IPv4PrefixBits = DefaultHashIPv4PrefixBits
	}
	if c.Hash.IPv6PrefixBits == 0 {
		c.Hash.IPv6PrefixBits = DefaultHashIPv6PrefixBits
	}
	c.applyRuleDefaults()
}

func (c *Config) validate() error {
	if c.ListenHTTP == "" && c.ListenHTTPS == "" {
		return errors.New("listen-http or listen-https must be set")
	}
	if c.ListenHTTPS != "" && (c.CertFile == "" || c.KeyFile == "") {
		return errors.New("listen-https requires cert-file and key-file")
	}
	if c.UpstreamConnectTimeout <= 0 {
		return errors.New("upstream-connect-timeout must be positive")
	} else if c.UpstreamReadTimeout <= 0 {
		return errors.New("upstream-read-timeout must be positive")
	}
	if err := c.HealthCheck.validate(); err != nil {
		return err
	}
	if err := c.ClientKeepalive.validate("client-keepalive"); err != nil {
		return err
	} else if err := c.UpstreamKeepalive.validate("upstream-keepalive"); err != nil {
		return err
	}
	if c.DrainDuration < 0 || c.ShutdownDrainDuration < 0 {
		return errors.New("drain durations must not be negative")
	}
	var err error
	if c.Streams.PathRegex, err = regexp.Compile(c.Streams.Path); err != nil {
		return fmt.Errorf("invalid streams.path: %w", err)
	}
	if c.Hash.IPv4PrefixBits < 1 || c.Hash.IPv4PrefixBits > 32 {
		return errors.New("hash.ipv4-prefix-bits must be between 1 and 32")
	}
	if c.Hash.IPv6PrefixBits < 1 || c.Hash.IPv6PrefixBits > 128 {
		return errors.New("hash.ipv6-prefix-bits must be between 1 and 128")
	}
	if len(c.Upstreams) == 0 {
		return errors.New("at least one upstream is required")
	}
	names := make(map[string]bool)
	for i, u := range c.Upstreams {
		if u == nil {
			return fmt.Errorf("upstreams[%d] is empty", i)
		} else if !upstreamNameRegex.MatchString(u.Name) {
			return fmt.Errorf("invalid upstream name %q", u.Name)
		} else if names[u.Name] {
			return fmt.Errorf("duplicate upstream name %q", u.Name)
		} else if len(u.Addresses) == 0 {
			return fmt.Errorf("upstream %s has no addresses", u.Name)
		}
		names[u.Name] = true
		for _, addr := range u.Addresses {
			if _, _, err := net.SplitHostPort(addr); err != nil {
				return fmt.Errorf("upstream %s: invalid address %q, expected host:port", u.Name, addr)
			}
		}
	}
	return c.validateRules()
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
