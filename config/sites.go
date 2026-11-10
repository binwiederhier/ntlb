package config

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"heckel.io/ntlb/match"
)

var (
	siteNameRegex = regexp.MustCompile(`^[-_.a-zA-Z0-9]{1,64}$`)
	hostRegex     = regexp.MustCompile(`^(\*\.)?[-_a-z0-9]+(\.[-_a-z0-9]+)*$`)
)

// Site is one site: the hosts it serves, its certificate, and the upstreams it proxies to. A
// request goes to the site whose hosts match its Host header (exact names first, then the
// longest wildcard), or else to the site without hosts, if there is one. Global rules are
// checked before the site's own.
type Site struct {
	Name        string       `yaml:"name"`         // Defaults to the file name for an included site
	Hosts       []string     `yaml:"hosts"`        // Names and wildcards ("*.ntfy.sh": any subdomain); none: the default site
	CertFile    string       `yaml:"cert-file"`    // Picked by SNI among the sites' certificates
	KeyFile     string       `yaml:"key-file"`     //
	HealthCheck *HealthCheck `yaml:"health-check"` // Active health checks against every upstream
	Streams     *Streams     `yaml:"streams"`      // Which requests are long-lived streams
	Upstreams   []*Upstream  `yaml:"upstreams"`    // Upstream names are per site
	Rules       []*Rule      `yaml:"rules"`        // Checked after the global rules

	file string // The file the site came from, for errors; "" for the main file
}

// siteIndex finds a site by host
type siteIndex struct {
	exact     map[string]*Site
	wildcards []*wildcard // Longest suffix first
	fallback  *Site       // The site without hosts, or nil
}

type wildcard struct {
	suffix string // ".ntfy.sh"
	site   *Site
}

// SiteFor returns the site for a request's Host header (a port and a trailing dot are ignored),
// or nil if no site serves it
func (c *Config) SiteFor(host string) *Site {
	h := normalizeHost(host)
	if site, ok := c.sites.exact[h]; ok {
		return site
	}
	for _, w := range c.sites.wildcards {
		if strings.HasSuffix(h, w.suffix) {
			return w.site
		}
	}
	return c.sites.fallback
}

// UpstreamCount returns the number of upstreams over all sites
func (c *Config) UpstreamCount() int {
	n := 0
	for _, site := range c.Sites {
		n += len(site.Upstreams)
	}
	return n
}

// loadIncludes appends the sites of the files that the include globs match, in file name order
// per glob. A relative glob is relative to dir.
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
			site, err := loadSite(file)
			if err != nil {
				return err
			}
			c.Sites = append(c.Sites, site)
		}
	}
	return nil
}

func loadSite(file string) (*Site, error) {
	b, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}
	site := &Site{
		file: file,
	}
	if err := decode(b, site); err != nil {
		return nil, fmt.Errorf("%s: %w", file, err)
	}
	if site.Name == "" {
		site.Name = strings.TrimSuffix(filepath.Base(file), filepath.Ext(file))
	}
	return site, nil
}

func (s *Site) applyDefaults() {
	if s.HealthCheck == nil {
		s.HealthCheck = &HealthCheck{}
	}
	s.HealthCheck.applyDefaults()
	if s.Streams == nil {
		s.Streams = &Streams{}
	}
	if s.Streams.Match == "" {
		s.Streams.Match = DefaultStreamsMatch
	}
	applyRuleDefaults(s.Rules)
}

// validateSites checks every site and indexes them by host
func (c *Config) validateSites() error {
	if len(c.Sites) == 0 {
		return errors.New("at least one site is required (sites or include)")
	}
	c.sites = &siteIndex{
		exact: make(map[string]*Site),
	}
	names := make(map[string]bool)
	suffixes := make(map[string]bool)
	hasCert := false
	for i, site := range c.Sites {
		if site == nil {
			return fmt.Errorf("sites[%d] is empty", i)
		} else if !siteNameRegex.MatchString(site.Name) {
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
		hasCert = hasCert || site.CertFile != ""

		// Index the hosts
		if len(site.Hosts) == 0 {
			if c.sites.fallback != nil {
				return fmt.Errorf("sites %s and %s both have no hosts; only one site may take the rest", c.sites.fallback.Name, site.Name)
			}
			c.sites.fallback = site
		}
		for _, host := range site.Hosts {
			if suffix, ok := strings.CutPrefix(host, "*"); ok {
				if suffixes[suffix] {
					return fmt.Errorf("duplicate host %q", host)
				}
				suffixes[suffix] = true
				c.sites.wildcards = append(c.sites.wildcards, &wildcard{suffix: suffix, site: site})
			} else if _, ok := c.sites.exact[host]; ok {
				return fmt.Errorf("duplicate host %q", host)
			} else {
				c.sites.exact[host] = site
			}
		}
	}
	slices.SortStableFunc(c.sites.wildcards, func(a, b *wildcard) int {
		return len(b.suffix) - len(a.suffix)
	})
	if c.ListenHTTPS != "" && !hasCert {
		return errors.New("listen-https requires a site with cert-file and key-file")
	}
	return nil
}

func (s *Site) validate(bans *Bans) error {
	for i, host := range s.Hosts {
		h := strings.TrimSuffix(strings.ToLower(host), ".")
		if !hostRegex.MatchString(h) {
			return fmt.Errorf("invalid host %q, expected a name or *.name, without a port", host)
		}
		s.Hosts[i] = h
	}
	if (s.CertFile == "") != (s.KeyFile == "") {
		return errors.New("cert-file and key-file go together")
	}
	if err := s.HealthCheck.validate(); err != nil {
		return err
	}
	var err error
	if s.Streams.Program, err = match.Compile(s.Streams.Match, match.Request); err != nil {
		return fmt.Errorf("invalid streams.match: %w", err)
	}
	if err := validateUpstreams(s.Upstreams); err != nil {
		return err
	}
	return validateRules(s.Rules, s.Name, bans)
}

func validateUpstreams(upstreams []*Upstream) error {
	if len(upstreams) == 0 {
		return errors.New("at least one upstream is required")
	}
	names := make(map[string]bool)
	for i, u := range upstreams {
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
		addresses, err := expandAddresses(u.Addresses)
		if err != nil {
			return fmt.Errorf("upstream %s: %w", u.Name, err)
		}
		u.Addresses = addresses
	}
	return nil
}

// normalizeHost lowercases a Host header and strips its port and trailing dot
func normalizeHost(host string) string {
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	return strings.TrimSuffix(strings.ToLower(host), ".")
}
