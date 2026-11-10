package config

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"heckel.io/ntlb/match"
)

// Rule phases
const (
	OnRequest  = "request"  // Before the request is proxied: the rule may decide what happens to it
	OnResponse = "response" // When the request ended: the rule may only add strikes
)

// Rule actions
const (
	ActionBlock    = "block"    // Answer with status (default 403)
	ActionLimit    = "limit"    // Rate limit per client key; over the limit, answer with status (default 429)
	ActionAllow    = "allow"    // Stop checking rules and proxy the request, e.g. to exempt clients from a limit
	ActionRedirect = "redirect" // Answer with a redirect to location (default 302)
	ActionStatic   = "static"   // Serve a file from root, or fallback if there is none
)

// Rule defaults
const (
	DefaultBlockStatus    = http.StatusForbidden
	DefaultLimitStatus    = http.StatusTooManyRequests
	DefaultRedirectStatus = http.StatusFound
)

var (
	ratePeriods = map[string]float64{"s": 1, "m": 60, "h": 3600}
)

// Rule is one rule. Request rules are checked in order before a request is proxied: the first
// block, allow, redirect or static whose match is true decides, and so does a limit that is
// exceeded; a request no rule decides is proxied. Response rules are checked when a request
// ended, all of them, and only add strikes. A rule without match matches every request.
type Rule struct {
	Name     string  `yaml:"name"`
	On       string  `yaml:"on"`       // request (default) or response
	Match    string  `yaml:"match"`    // A request or response expression, see package match
	Action   string  `yaml:"action"`   // Request rules only, required
	Status   int     `yaml:"status"`   // block, limit, redirect
	Rate     string  `yaml:"rate"`     // limit: requests per period, e.g. 45/m (s, m, h)
	Burst    int     `yaml:"burst"`    // limit: extra requests allowed at once (like limit_req burst, nodelay)
	Location string  `yaml:"location"` // redirect: a string expression, e.g. '"https://" + host + uri'
	Root     string  `yaml:"root"`     // static: the directory to serve
	Fallback string  `yaml:"fallback"` // static: the file (relative to root) for paths that are no file
	Strike   float64 `yaml:"strike"`   // Strikes per match (block; limit: per limited request; response rules), see bans

	ID              string         `yaml:"-"` // Name, or site/name for a site's rule
	Program         *match.Program `yaml:"-"` // Compiled Match; nil matches every request
	LocationProgram *match.Program `yaml:"-"`
	RatePerSecond   float64        `yaml:"-"`
}

func applyRuleDefaults(rules []*Rule) {
	for _, r := range rules {
		if r == nil {
			continue
		}
		if r.On == "" {
			r.On = OnRequest
		}
		if r.Status == 0 {
			switch r.Action {
			case ActionBlock:
				r.Status = DefaultBlockStatus
			case ActionLimit:
				r.Status = DefaultLimitStatus
			case ActionRedirect:
				r.Status = DefaultRedirectStatus
			}
		}
	}
}

// validateRules checks and compiles rules; site is "" for the global rules. Strikes need a bans
// section that counts them.
func validateRules(rules []*Rule, site string, canStrike bool) error {
	names := make(map[string]bool)
	for i, r := range rules {
		if r == nil {
			return fmt.Errorf("rules[%d] is empty", i)
		} else if !nameRegex.MatchString(r.Name) {
			return fmt.Errorf("invalid rule name %q", r.Name)
		} else if names[r.Name] {
			return fmt.Errorf("duplicate rule name %q", r.Name)
		}
		names[r.Name] = true
		r.ID = r.Name
		if site != "" {
			r.ID = site + "/" + r.Name
		}
		if err := r.compile(); err != nil {
			return fmt.Errorf("rule %s: %w", r.ID, err)
		} else if r.Strike > 0 && !canStrike {
			return fmt.Errorf("rule %s: strike needs a bans section", r.ID)
		}
	}
	return nil
}

// compile validates the rule and fills in its compiled fields
func (r *Rule) compile() error {
	if r.Strike < 0 {
		return errors.New("strike must not be negative")
	}
	kind := match.Request
	switch r.On {
	case OnRequest:
		if err := r.compileAction(); err != nil {
			return err
		}
	case OnResponse:
		kind = match.Response
		if r.Action != "" || r.Status != 0 || r.Rate != "" || r.Burst != 0 || r.Location != "" || r.Root != "" || r.Fallback != "" {
			return errors.New("a response rule takes match and strike only")
		} else if r.Strike == 0 {
			return errors.New("a response rule needs a strike")
		}
	default:
		return fmt.Errorf("unknown on %q, expected request or response", r.On)
	}
	if r.Match != "" {
		program, err := match.Compile(r.Match, kind)
		if err != nil {
			return fmt.Errorf("invalid match: %w", err)
		}
		r.Program = program
	}
	return nil
}

// compileAction checks that a request rule has an action and only the settings of its action
func (r *Rule) compileAction() error {
	hasRate := r.Rate != "" || r.Burst != 0
	hasStatic := r.Root != "" || r.Fallback != ""
	switch r.Action {
	case ActionBlock:
		if hasRate || r.Location != "" || hasStatic {
			return errors.New("block takes status and strike only")
		}
	case ActionLimit:
		if r.Location != "" || hasStatic {
			return errors.New("limit takes rate, burst, status and strike only")
		}
		perSecond, err := parseRate(r.Rate)
		if err != nil {
			return err
		} else if r.Burst < 0 {
			return errors.New("burst must not be negative")
		}
		r.RatePerSecond = perSecond
	case ActionAllow:
		if r.Status != 0 || hasRate || r.Location != "" || hasStatic || r.Strike != 0 {
			return errors.New("allow takes no settings")
		}
	case ActionRedirect:
		if hasRate || hasStatic || r.Strike != 0 {
			return errors.New("redirect takes location and status only")
		} else if r.Location == "" {
			return errors.New("redirect needs a location")
		} else if r.Status < 300 || r.Status > 399 {
			return fmt.Errorf("invalid redirect status %d", r.Status)
		}
		program, err := match.Compile(r.Location, match.String)
		if err != nil {
			return fmt.Errorf("invalid location: %w", err)
		}
		r.LocationProgram = program
	case ActionStatic:
		if r.Status != 0 || hasRate || r.Location != "" || r.Strike != 0 {
			return errors.New("static takes root and fallback only")
		} else if r.Root == "" {
			return errors.New("static needs a root")
		} else if r.Fallback != "" && !strings.HasPrefix(r.Fallback, "/") {
			return errors.New("fallback must start with /")
		}
	case "":
		return errors.New("action is required (block, limit, allow, redirect or static)")
	default:
		return fmt.Errorf("unknown action %q, expected block, limit, allow, redirect or static", r.Action)
	}
	if r.Status != 0 && (r.Status < 100 || r.Status > 599) {
		return fmt.Errorf("invalid status %d", r.Status)
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
