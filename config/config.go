// Package config loads and validates the ntfy-lb YAML config file.
package config

import (
	"errors"
	"fmt"
	"net"
	"os"
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
)

var (
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
	Upstreams              []*Upstream   `yaml:"upstreams"`
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
	return Parse(b)
}

// Parse defaults and validates a config from YAML
func Parse(b []byte) (*Config, error) {
	c := &Config{}
	if err := yaml.Unmarshal(b, c); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	c.applyDefaults()
	if err := c.validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	return c, nil
}

// ListenersEqual returns true if both configs listen on the same addresses, which a reload cannot change
func (c *Config) ListenersEqual(other *Config) bool {
	return c.ListenHTTP == other.ListenHTTP && c.ListenHTTPS == other.ListenHTTPS && c.ListenAdmin == other.ListenAdmin
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
}

func (c *Config) validate() error {
	if c.ListenHTTP == "" && c.ListenHTTPS == "" {
		return errors.New("listen-http or listen-https must be set")
	}
	if c.ListenHTTPS != "" && (c.CertFile == "" || c.KeyFile == "") {
		return errors.New("listen-https requires cert-file and key-file")
	}
	if c.HealthCheck.Timeout >= c.HealthCheck.Interval {
		return errors.New("health-check.timeout must be shorter than health-check.interval")
	}
	if c.DrainDuration < 0 || c.ShutdownDrainDuration < 0 {
		return errors.New("drain durations must not be negative")
	}
	if len(c.Upstreams) == 0 {
		return errors.New("at least one upstream is required")
	}
	names := make(map[string]bool)
	for _, u := range c.Upstreams {
		if !upstreamNameRegex.MatchString(u.Name) {
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
