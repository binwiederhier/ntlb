package config

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strings"

	"gopkg.in/yaml.v3"
	"heckel.io/ntlb/match"
)

var (
	hostRegex = regexp.MustCompile(`^(\*\.)?[-_a-z0-9]+(\.[-_a-z0-9]+)*$`)
)

// Site is one site: the addresses it listens on, the hosts it serves there, its certificate,
// access logs and the upstreams it proxies to. On each listener, a request goes to the site
// whose hosts match its Host header (exact names first, then the longest wildcard), or else to
// the listener's site without hosts, if there is one. Global rules are checked before the site's
// own.
type Site struct {
	Name        string       `yaml:"name"`
	ListenHTTP  ListenAddrs  `yaml:"listen-http"`  // One address or a list; sites may share them
	ListenHTTPS ListenAddrs  `yaml:"listen-https"` // Needs cert-file and key-file
	Hosts       []string     `yaml:"hosts"`        // Names and wildcards ("*.ntfy.sh": any subdomain); none: the listeners' default site
	CertFile    string       `yaml:"cert-file"`    // Picked by SNI among the certificates of a listener's sites
	KeyFile     string       `yaml:"key-file"`     //
	AccessLogs  []*AccessLog `yaml:"access-logs"`  // Replace the global access logs for this site
	Bans        *Bans        `yaml:"bans"`         // Replace the global bans settings for this site (usually there are only global ones)
	Gzip        *Gzip        `yaml:"gzip"`         // Replace the global gzip settings for this site
	Streams     *Streams     `yaml:"streams"`      // Which requests are long-lived streams
	Upstream    *Upstream    `yaml:"upstream"`     // The servers, and how requests reach them
	Rules       []*Rule      `yaml:"rules"`        // Checked after the global rules

	file string // The file the site came from, for errors; "" for the main file
}

// ListenAddrs are listen addresses, written as one address or a list
type ListenAddrs []string

// Listener is one public listen address and the sites served on it
type Listener struct {
	Addr  string
	TLS   bool
	Sites []*Site // In config order

	index *siteIndex
}

// siteIndex finds a listener's site by host
type siteIndex struct {
	exact     map[string]*Site
	wildcards []*wildcard // Longest suffix first
	fallback  *Site       // The site without hosts, or nil
}

type wildcard struct {
	suffix string // ".ntfy.sh"
	site   *Site
}

// UnmarshalYAML takes a single address as a list of one
func (l *ListenAddrs) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind == yaml.ScalarNode {
		*l = ListenAddrs{node.Value}
		return nil
	}
	var addrs []string
	if err := node.Decode(&addrs); err != nil {
		return err
	}
	*l = addrs
	return nil
}

// SiteFor returns the site for a request to a listener (its configured address) with a Host
// header (a port and a trailing dot are ignored), or nil if no site there serves it
func (c *Config) SiteFor(listen, host string) *Site {
	l := c.Listener(listen)
	if l == nil {
		return nil
	}
	h := normalizeHost(host)
	if site, ok := l.index.exact[h]; ok {
		return site
	}
	for _, w := range l.index.wildcards {
		if strings.HasSuffix(h, w.suffix) {
			return w.site
		}
	}
	return l.index.fallback
}

// Listener returns the listener with a configured address, or nil
func (c *Config) Listener(addr string) *Listener {
	for _, l := range c.Listeners {
		if l.Addr == addr {
			return l
		}
	}
	return nil
}

// Site returns the site with a name, or nil
func (c *Config) Site(name string) *Site {
	for _, site := range c.Sites {
		if site.Name == name {
			return site
		}
	}
	return nil
}

// AccessLogsFor returns the access logs for a request to a site: its own, else the global ones.
// site is nil for a request no site serves.
func (c *Config) AccessLogsFor(site *Site) []*AccessLog {
	if site != nil && len(site.AccessLogs) > 0 {
		return site.AccessLogs
	}
	return c.AccessLogs
}

// BansFor returns the bans settings for a request to a site: its own, else the global ones, or
// nil if none count its strikes. site is nil for a request no site serves.
func (c *Config) BansFor(site *Site) *Bans {
	if site != nil && site.Bans != nil {
		return site.Bans
	}
	return c.Bans
}

// GzipFor returns the gzip settings for a request to a site: its own, else the global ones, or
// nil for no compression
func (c *Config) GzipFor(site *Site) *Gzip {
	if site != nil && site.Gzip != nil {
		return site.Gzip
	}
	return c.Gzip
}

// UpstreamCount returns the number of upstreams over all sites
func (c *Config) UpstreamCount() int {
	n := 0
	for _, site := range c.Sites {
		n += len(site.Upstream.Servers)
	}
	return n
}

// loadIncludes merges the files that the include globs match into c, in file name order per glob.
// A relative glob is relative to dir.
func (c *Config) loadIncludes(dir string) error {
	for _, pattern := range c.Include {
		if !filepath.IsAbs(pattern) {
			pattern = filepath.Join(dir, pattern)
		}
		files, err := filepath.Glob(pattern)
		if err != nil {
			return fmt.Errorf("invalid include %q: %w", pattern, err)
		}
		for _, file := range files {
			if err := c.mergeFile(file); err != nil {
				return err
			}
		}
	}
	return nil
}

// mergeFile merges a config file into c: lists are appended, anything else may only be set in
// one file
func (c *Config) mergeFile(file string) error {
	b, err := os.ReadFile(file)
	if err != nil {
		return err
	}
	part := &Config{}
	if err := decode(b, part); err != nil {
		return fmt.Errorf("%s: %w", file, err)
	} else if part.Include != nil {
		return fmt.Errorf("%s: include is only allowed in the main file", file)
	}
	for _, site := range part.Sites {
		if site != nil {
			site.file = file
		}
	}
	dst, src := reflect.ValueOf(c).Elem(), reflect.ValueOf(part).Elem()
	for i := 0; i < dst.NumField(); i++ {
		field := dst.Type().Field(i)
		from, to := src.Field(i), dst.Field(i)
		if field.Tag.Get("yaml") == "-" || from.IsZero() {
			continue
		} else if from.Kind() == reflect.Slice {
			to.Set(reflect.AppendSlice(to, from))
		} else if !to.IsZero() {
			return fmt.Errorf("%s: %s is set in another file", file, field.Tag.Get("yaml"))
		} else {
			to.Set(from)
		}
	}
	return nil
}

func (s *Site) applyDefaults() {
	if s.Upstream != nil {
		s.Upstream.applyDefaults()
	}
	if s.Streams == nil {
		s.Streams = &Streams{}
	}
	if s.Streams.Match == "" {
		s.Streams.Match = DefaultStreamsMatch
	}
	s.Bans.applyDefaults()
	applyRuleDefaults(s.Rules)
}

// validateSites checks every site and indexes the listeners' sites by host
func (c *Config) validateSites() error {
	if len(c.Sites) == 0 {
		return errors.New("at least one site is required (sites, here or in an included file)")
	}
	names := make(map[string]bool)
	for i, site := range c.Sites {
		if site == nil {
			return fmt.Errorf("sites[%d] is empty", i)
		} else if !nameRegex.MatchString(site.Name) {
			return fmt.Errorf("invalid site name %q", site.Name)
		} else if names[site.Name] {
			return fmt.Errorf("duplicate site name %q", site.Name)
		}
		names[site.Name] = true
		if err := site.validate(c.Bans); err != nil {
			if site.file != "" {
				return fmt.Errorf("site %s (%s): %w", site.Name, site.file, err)
			}
			return fmt.Errorf("site %s: %w", site.Name, err)
		}
		if err := c.addListeners(site); err != nil {
			return fmt.Errorf("site %s: %w", site.Name, err)
		}
	}
	for _, l := range c.Listeners {
		if err := l.indexSites(); err != nil {
			return fmt.Errorf("listener %s: %w", l.Addr, err)
		}
	}
	return nil
}

func (s *Site) validate(bans *Bans) error {
	if len(s.ListenHTTP) == 0 && len(s.ListenHTTPS) == 0 {
		return errors.New("listen-http or listen-https is required")
	}
	for i, host := range s.Hosts {
		h := strings.TrimSuffix(strings.ToLower(host), ".")
		if !hostRegex.MatchString(h) {
			return fmt.Errorf("invalid host %q, expected a name or *.name, without a port", host)
		}
		s.Hosts[i] = h
	}
	if (s.CertFile == "") != (s.KeyFile == "") {
		return errors.New("cert-file and key-file go together")
	} else if len(s.ListenHTTPS) > 0 && s.CertFile == "" {
		return errors.New("listen-https requires cert-file and key-file")
	}
	if err := validateAccessLogs(s.AccessLogs); err != nil {
		return err
	} else if err := validateBans(s.Bans); err != nil {
		return err
	} else if err := s.Gzip.validate(); err != nil {
		return err
	}
	var err error
	if s.Streams.Program, err = match.Compile(s.Streams.Match, match.Request); err != nil {
		return fmt.Errorf("invalid streams.match: %w", err)
	}
	if s.Upstream == nil {
		return errors.New("upstream is required")
	} else if err := s.Upstream.validate(); err != nil {
		return err
	}
	return validateRules(s.Rules, s.Name, bans != nil || s.Bans != nil)
}

// addListeners adds the site to its listeners, creating those not seen yet
func (c *Config) addListeners(site *Site) error {
	seen := make(map[string]bool)
	add := func(addr string, tls bool) error {
		if _, _, err := net.SplitHostPort(addr); err != nil {
			return fmt.Errorf("invalid listen address %q, expected host:port or :port", addr)
		} else if seen[addr] {
			return fmt.Errorf("duplicate listen address %s", addr)
		}
		seen[addr] = true
		l := c.Listener(addr)
		if l == nil {
			for _, other := range c.Listeners {
				if overlaps(addr, other.Addr) {
					return fmt.Errorf("listen address %s overlaps with %s (same port, and the same or a wildcard IP)", addr, other.Addr)
				}
			}
			l = &Listener{
				Addr: addr,
				TLS:  tls,
			}
			c.Listeners = append(c.Listeners, l)
		} else if l.TLS != tls {
			return fmt.Errorf("%s is listen-http in one site and listen-https in another", addr)
		}
		l.Sites = append(l.Sites, site)
		return nil
	}
	for _, addr := range site.ListenHTTP {
		if err := add(addr, false); err != nil {
			return err
		}
	}
	for _, addr := range site.ListenHTTPS {
		if err := add(addr, true); err != nil {
			return err
		}
	}
	return nil
}

// indexSites indexes the listener's sites by host
func (l *Listener) indexSites() error {
	l.index = &siteIndex{
		exact: make(map[string]*Site),
	}
	suffixes := make(map[string]bool)
	for _, site := range l.Sites {
		if len(site.Hosts) == 0 {
			if l.index.fallback != nil {
				return fmt.Errorf("sites %s and %s both have no hosts; only one site may take the rest", l.index.fallback.Name, site.Name)
			}
			l.index.fallback = site
		}
		for _, host := range site.Hosts {
			if suffix, ok := strings.CutPrefix(host, "*"); ok {
				if suffixes[suffix] {
					return fmt.Errorf("duplicate host %q", host)
				}
				suffixes[suffix] = true
				l.index.wildcards = append(l.index.wildcards, &wildcard{
					suffix: suffix,
					site:   site,
				})
			} else if _, ok := l.index.exact[host]; ok {
				return fmt.Errorf("duplicate host %q", host)
			} else {
				l.index.exact[host] = site
			}
		}
	}
	slices.SortStableFunc(l.index.wildcards, func(a, b *wildcard) int {
		return len(b.suffix) - len(a.suffix)
	})
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

func (u *Upstream) validate() error {
	if !slices.Contains(balances, u.Balance) {
		return fmt.Errorf("invalid upstream.balance %q, expected one of %s", u.Balance, strings.Join(balances, ", "))
	} else if u.Timeouts.Connect <= 0 {
		return errors.New("upstream.timeouts.connect must be positive")
	} else if u.Timeouts.Read <= 0 {
		return errors.New("upstream.timeouts.read must be positive")
	} else if err := u.Keepalive.validate("upstream.keepalive"); err != nil {
		return err
	} else if err := u.HealthCheck.validate(); err != nil {
		return err
	}
	return validateServers(u.Servers)
}

func validateServers(servers []*Server) error {
	if len(servers) == 0 {
		return errors.New("at least one server is required (upstream.servers)")
	}
	names := make(map[string]bool)
	for i, srv := range servers {
		if srv == nil {
			return fmt.Errorf("upstream.servers[%d] is empty", i)
		} else if !nameRegex.MatchString(srv.Name) {
			return fmt.Errorf("invalid server name %q", srv.Name)
		} else if names[srv.Name] {
			return fmt.Errorf("duplicate server name %q", srv.Name)
		} else if len(srv.Addresses) == 0 {
			return fmt.Errorf("server %s has no addresses", srv.Name)
		}
		names[srv.Name] = true
		addresses, err := expandAddresses(srv.Addresses)
		if err != nil {
			return fmt.Errorf("server %s: %w", srv.Name, err)
		}
		srv.Addresses = addresses
	}
	return nil
}

// overlaps reports whether two different listen addresses would bind the same socket: the same
// port (not 0, any free port), and the same IP, or the wildcard on either side. Hostnames are only
// compared as written.
func overlaps(a, b string) bool {
	hostA, portA, _ := net.SplitHostPort(a) // Both were checked
	hostB, portB, _ := net.SplitHostPort(b)
	if portA != portB || portA == "0" {
		return false
	}
	ipA, errA := netip.ParseAddr(hostA)
	ipB, errB := netip.ParseAddr(hostB)
	wildcardA := hostA == "" || (errA == nil && ipA.IsUnspecified())
	wildcardB := hostB == "" || (errB == nil && ipB.IsUnspecified())
	if wildcardA || wildcardB {
		return true
	}
	return errA == nil && errB == nil && ipA.Unmap() == ipB.Unmap()
}

// normalizeHost lowercases a Host header and strips its port and trailing dot
func normalizeHost(host string) string {
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	return strings.TrimSuffix(strings.ToLower(host), ".")
}
