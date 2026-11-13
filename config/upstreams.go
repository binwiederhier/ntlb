package config

import (
	"errors"
	"fmt"
	"maps"
	"net"
	"slices"
	"strconv"
	"strings"
	"time"
)

// HealthCheck configures the active health checks against every upstream
type HealthCheck struct {
	Path     string        `yaml:"path"`
	Interval time.Duration `yaml:"interval"`
	Timeout  time.Duration `yaml:"timeout"`
	Fails    int           `yaml:"fails"`
	Passes   int           `yaml:"passes"`
}

// Upstream is a site's backend servers and how requests reach them, like nginx's upstream block
// and its proxy_*_timeout settings
type Upstream struct {
	Name        string       `yaml:"-"`           // Its key in the site's upstreams
	ID          string       `yaml:"-"`           // "<site>/<name>", which identifies it among all sites' upstreams
	Balance     string       `yaml:"balance"`     // round-robin (default), least-conn, random-two or hash
	Timeouts    *Timeouts    `yaml:"timeouts"`    //
	Keepalive   *Keepalive   `yaml:"keepalive"`   // TCP keepalive towards the servers
	HealthCheck *HealthCheck `yaml:"healthcheck"` // Active health checks against every server
	Servers     []*Server    `yaml:"servers"`     // Server names are per site
}

// Timeouts bound the connections to a site's servers
type Timeouts struct {
	Connect time.Duration `yaml:"connect"` // An address that does not accept within this is down for the request
	Read    time.Duration `yaml:"read"`    // Cut a response (or a WebSocket) after this long without a byte
}

// Server is one backend node. The name, not the address, is what clients are hashed to, so every
// load balancer must use the same names for the same nodes to route a client the same way. An
// address may be a port range ("10.0.0.1:2586-2589"), one address per port.
type Server struct {
	Name      string   `yaml:"name"`
	Addresses []string `yaml:"addresses"`
	Weight    int      `yaml:"weight"` // Its share relative to the others, 1 (default) to maxWeight
	Backup    bool     `yaml:"backup"` // Only used if no other server is healthy, and as their last retries
}

// UpstreamCount returns the number of servers over all sites and their upstreams
func (c *Config) UpstreamCount() int {
	n := 0
	for _, site := range c.Sites {
		for _, u := range site.AllUpstreams() {
			n += len(u.Servers)
		}
	}
	return n
}

// AllUpstreams returns the site's upstreams, the default first, then the others by name
func (s *Site) AllUpstreams() []*Upstream {
	return s.upstreams
}

// DefaultUpstream returns the site's upstream that takes every request no proxy rule routes
func (s *Site) DefaultUpstream() *Upstream {
	return s.Upstreams[UpstreamDefault]
}

// UpstreamNamed returns the site's upstream with a name ("" for the default), or nil
func (s *Site) UpstreamNamed(name string) *Upstream {
	if name == "" {
		name = UpstreamDefault
	}
	return s.Upstreams[name]
}

// validateUpstreams checks the site's upstreams, and that server names are unique over all of
// them: a name is what drains, the status and the metrics know a node by
func (s *Site) validateUpstreams() error {
	if _, ok := s.Upstreams[UpstreamDefault]; !ok {
		return errors.New("upstreams.default is required")
	}
	others := slices.DeleteFunc(slices.Sorted(maps.Keys(s.Upstreams)), func(name string) bool {
		return name == UpstreamDefault
	})
	s.upstreams = nil
	for _, name := range slices.Concat([]string{UpstreamDefault}, others) {
		u := s.Upstreams[name]
		if !nameRegex.MatchString(name) {
			return fmt.Errorf("invalid upstream name %q", name)
		} else if u == nil {
			return fmt.Errorf("upstreams.%s is empty", name)
		}
		u.Name, u.ID = name, s.Name+"/"+name
		if err := u.validate("upstreams." + name); err != nil {
			return err
		}
		s.upstreams = append(s.upstreams, u)
	}
	names := make(map[string]bool)
	for _, u := range s.upstreams {
		for _, srv := range u.Servers {
			if names[srv.Name] {
				return fmt.Errorf("duplicate server name %q", srv.Name)
			}
			names[srv.Name] = true
		}
	}
	return nil
}

func (u *Upstream) applyDefaults() {
	if u.Balance == "" {
		u.Balance = DefaultBalance
	}
	if u.Timeouts == nil {
		u.Timeouts = &Timeouts{}
	}
	if u.Timeouts.Connect == 0 {
		u.Timeouts.Connect = DefaultUpstreamConnectTimeout
	}
	if u.Timeouts.Read == 0 {
		u.Timeouts.Read = DefaultUpstreamReadTimeout
	}
	if u.Keepalive == nil {
		u.Keepalive = &Keepalive{}
	}
	u.Keepalive.applyDefaults()
	if u.HealthCheck == nil {
		u.HealthCheck = &HealthCheck{}
	}
	u.HealthCheck.applyDefaults()
}

// validate checks the upstream; section is where it is in the site, for errors ("upstreams.<name>")
func (u *Upstream) validate(section string) error {
	if !slices.Contains(balances, u.Balance) {
		return fmt.Errorf("invalid %s.balance %q, expected one of %s", section, u.Balance, strings.Join(balances, ", "))
	} else if u.Timeouts.Connect <= 0 {
		return fmt.Errorf("%s.timeouts.connect must be positive", section)
	} else if u.Timeouts.Read <= 0 {
		return fmt.Errorf("%s.timeouts.read must be positive", section)
	} else if err := u.Keepalive.validate(section + ".keepalive"); err != nil {
		return err
	} else if err := u.HealthCheck.validate(section + ".healthcheck"); err != nil {
		return err
	}
	return validateServers(u.Servers, section)
}

func validateServers(servers []*Server, section string) error {
	if len(servers) == 0 {
		return fmt.Errorf("at least one server is required (%s.servers)", section)
	}
	names := make(map[string]bool)
	for i, srv := range servers {
		if srv == nil {
			return fmt.Errorf("%s.servers[%d] is empty", section, i)
		} else if !nameRegex.MatchString(srv.Name) {
			return fmt.Errorf("invalid server name %q", srv.Name)
		} else if names[srv.Name] {
			return fmt.Errorf("duplicate server name %q", srv.Name)
		} else if len(srv.Addresses) == 0 {
			return fmt.Errorf("server %s has no addresses", srv.Name)
		}
		names[srv.Name] = true
		if srv.Weight == 0 {
			srv.Weight = 1
		} else if srv.Weight < 1 || srv.Weight > maxWeight {
			return fmt.Errorf("server %s: weight must be between 1 and %d", srv.Name, maxWeight)
		}
		addresses, err := expandAddresses(srv.Addresses)
		if err != nil {
			return fmt.Errorf("server %s: %w", srv.Name, err)
		}
		srv.Addresses = addresses
	}
	hasPrimary := slices.ContainsFunc(servers, func(srv *Server) bool {
		return !srv.Backup
	})
	if !hasPrimary {
		return errors.New("at least one server must not be a backup")
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
		first, last, err := parsePortRange(from, to)
		if err != nil {
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

// parsePortRange parses the first and last port of a range, at most maxPortRange ports
func parsePortRange(from, to string) (int, int, error) {
	first, err := strconv.Atoi(from)
	if err != nil {
		return 0, 0, err
	}
	last, err := strconv.Atoi(to)
	if err != nil {
		return 0, 0, err
	} else if first < 1 || last > 65535 || last < first || last-first >= maxPortRange {
		return 0, 0, errors.New("invalid port range")
	}
	return first, last, nil
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
func (h *HealthCheck) validate(section string) error {
	if h.Interval <= 0 {
		return fmt.Errorf("%s.interval must be positive", section)
	} else if h.Timeout <= 0 {
		return fmt.Errorf("%s.timeout must be positive", section)
	} else if h.Timeout >= h.Interval {
		return fmt.Errorf("%s.timeout must be shorter than its interval", section)
	} else if h.Fails <= 0 {
		return fmt.Errorf("%s.fails must be positive", section)
	} else if h.Passes <= 0 {
		return fmt.Errorf("%s.passes must be positive", section)
	}
	return nil
}
