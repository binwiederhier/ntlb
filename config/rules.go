package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
	"heckel.io/ntfy-lb/match"
)

// Rule actions
const (
	ActionBlock = "block" // Answer with the rule's status (default 403)
	ActionLimit = "limit" // Rate limit per client key; over the limit, answer with the status (default 429)
	ActionAllow = "allow" // Stop evaluating rules, e.g. to exempt clients from a limit below
)

// Rule defaults
const (
	DefaultBlockStatus = http.StatusForbidden
	DefaultLimitStatus = http.StatusTooManyRequests
)

var (
	ruleNameRegex = regexp.MustCompile(`^[-_.a-zA-Z0-9]{1,64}$`)
	ratePeriods   = map[string]float64{"s": 1, "m": 60, "h": 3600}
)

// Rule blocks, limits or allows the requests its match expression matches (see package match);
// a rule without one matches every request. Rules are checked in order; the first block or allow,
// or a limit that is exceeded, decides.
type Rule struct {
	Name   string `yaml:"name"`
	Match  string `yaml:"match"`
	Action string `yaml:"action"`
	Status int    `yaml:"status"`
	Rate   string `yaml:"rate"`  // Limit only: requests per period, e.g. 45/m (s, m, h)
	Burst  int    `yaml:"burst"` // Limit only: extra requests allowed at once (like limit_req burst, nodelay)
	Log    string `yaml:"log"`   // Requests the rule blocks or limits, in the access log format

	Program       *match.Program `yaml:"-"` // Compiled Match; nil matches every request
	RatePerSecond float64        `yaml:"-"`
}

// rulesFile is the format of the rules-file, the same as the rules section of the config
type rulesFile struct {
	Rules []*Rule `yaml:"rules"`
}

// loadRulesFile appends the rules of the rules-file, if any. A relative path is relative to dir,
// the config file's directory.
func (c *Config) loadRulesFile(dir string) error {
	if c.RulesFile == "" {
		return nil
	}
	filename := c.RulesFile
	if !filepath.IsAbs(filename) && dir != "" {
		filename = filepath.Join(dir, filename)
	}
	b, err := os.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("cannot read rules-file: %w", err)
	}
	f := &rulesFile{}
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	if err := dec.Decode(f); err != nil && !errors.Is(err, io.EOF) { // EOF: empty file
		return fmt.Errorf("rules-file %s: %w", filename, err)
	}
	c.Rules = append(c.Rules, f.Rules...)
	return nil
}

func (c *Config) applyRuleDefaults() {
	for _, r := range c.Rules {
		if r == nil {
			continue
		}
		if r.Action == "" {
			r.Action = ActionBlock
		}
		if r.Status == 0 {
			switch r.Action {
			case ActionBlock:
				r.Status = DefaultBlockStatus
			case ActionLimit:
				r.Status = DefaultLimitStatus
			}
		}
	}
}

func (c *Config) validateRules() error {
	names := make(map[string]bool)
	for i, r := range c.Rules {
		if r == nil {
			return fmt.Errorf("rules[%d] is empty", i)
		} else if !ruleNameRegex.MatchString(r.Name) {
			return fmt.Errorf("invalid rule name %q", r.Name)
		} else if names[r.Name] {
			return fmt.Errorf("duplicate rule name %q", r.Name)
		}
		names[r.Name] = true
		if err := r.compile(); err != nil {
			return fmt.Errorf("rule %s: %w", r.Name, err)
		}
	}
	return nil
}

// compile validates the rule and fills in its compiled fields
func (r *Rule) compile() error {
	// Action and the settings that belong to it
	switch r.Action {
	case ActionBlock, ActionLimit:
		if r.Status < 100 || r.Status > 599 {
			return fmt.Errorf("invalid status %d", r.Status)
		}
	case ActionAllow:
		if r.Status != 0 || r.Log != "" {
			return errors.New("an allow rule takes no status or log")
		}
	default:
		return fmt.Errorf("unknown action %q, expected block, limit or allow", r.Action)
	}
	if r.Action == ActionLimit {
		perSecond, err := parseRate(r.Rate)
		if err != nil {
			return err
		} else if r.Burst < 0 {
			return errors.New("burst must not be negative")
		}
		r.RatePerSecond = perSecond
	} else if r.Rate != "" || r.Burst != 0 {
		return errors.New("rate and burst are for limit rules only")
	}

	// Match expression
	if r.Match != "" {
		program, err := match.Compile(r.Match)
		if err != nil {
			return fmt.Errorf("invalid match: %w", err)
		}
		r.Program = program
	}
	return nil
}

// parseRate parses "<n>/<s|m|h>" into requests per second
func parseRate(s string) (float64, error) {
	count, period, ok := strings.Cut(s, "/")
	n, err := strconv.Atoi(count)
	seconds, known := ratePeriods[period]
	if !ok || err != nil || !known || n <= 0 {
		return 0, fmt.Errorf("invalid rate %q, expected e.g. 45/m (per s, m or h)", s)
	}
	return float64(n) / seconds, nil
}
