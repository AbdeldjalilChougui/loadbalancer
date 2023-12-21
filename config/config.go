// Package config loads the load balancer's JSON configuration.
package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"time"
)

// Duration is a time.Duration that reads as a string like "5s" in JSON.
type Duration time.Duration

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("duration must be a string like \"5s\": %w", err)
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(v)
	return nil
}

func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(time.Duration(d).String()) }

// D returns the value as a time.Duration.
func (d Duration) D() time.Duration { return time.Duration(d) }

type Backend struct {
	URL    string `json:"url"`
	Weight int    `json:"weight,omitempty"` // default 1
}

type HealthCheck struct {
	Path               string   `json:"path"` // empty disables active checks
	Interval           Duration `json:"interval"`
	Timeout            Duration `json:"timeout"`
	HealthyThreshold   int      `json:"healthy_threshold"`
	UnhealthyThreshold int      `json:"unhealthy_threshold"`
}

type CircuitBreaker struct {
	FailureThreshold int      `json:"failure_threshold"` // consecutive failures to open; 0 disables
	Cooldown         Duration `json:"cooldown"`          // time open before a half-open trial
}

type Config struct {
	Listen         string         `json:"listen"`
	Admin          string         `json:"admin"`    // serves /metrics and /status
	Strategy       string         `json:"strategy"` // round_robin | weighted | least_conn
	Retries        int            `json:"retries"`  // extra attempts for idempotent requests
	Backends       []Backend      `json:"backends"`
	HealthCheck    HealthCheck    `json:"health_check"`
	CircuitBreaker CircuitBreaker `json:"circuit_breaker"`
}

// Strategies lists the accepted strategy names.
var Strategies = []string{"round_robin", "weighted", "least_conn"}

// Load reads and validates a config file.
func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Parse(b)
}

// Parse decodes, defaults and validates a config.
func Parse(b []byte) (*Config, error) {
	var c Config
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	c.applyDefaults()
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

func (c *Config) applyDefaults() {
	if c.Listen == "" {
		c.Listen = ":8080"
	}
	if c.Admin == "" {
		c.Admin = ":9090"
	}
	if c.Strategy == "" {
		c.Strategy = "round_robin"
	}
	for i := range c.Backends {
		if c.Backends[i].Weight == 0 {
			c.Backends[i].Weight = 1
		}
	}
	h := &c.HealthCheck
	if h.Interval == 0 {
		h.Interval = Duration(5 * time.Second)
	}
	if h.Timeout == 0 {
		h.Timeout = Duration(time.Second)
	}
	if h.HealthyThreshold == 0 {
		h.HealthyThreshold = 2
	}
	if h.UnhealthyThreshold == 0 {
		h.UnhealthyThreshold = 2
	}
	if c.CircuitBreaker.Cooldown == 0 {
		c.CircuitBreaker.Cooldown = Duration(10 * time.Second)
	}
}

func (c *Config) Validate() error {
	var errs []error
	ok := false
	for _, s := range Strategies {
		ok = ok || s == c.Strategy
	}
	if !ok {
		errs = append(errs, fmt.Errorf("unknown strategy %q (want one of %v)", c.Strategy, Strategies))
	}
	if len(c.Backends) == 0 {
		errs = append(errs, errors.New("at least one backend is required"))
	}
	seen := map[string]bool{}
	for _, b := range c.Backends {
		u, err := url.Parse(b.URL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			errs = append(errs, fmt.Errorf("backend %q: need an absolute http(s) URL", b.URL))
		}
		if b.Weight < 0 {
			errs = append(errs, fmt.Errorf("backend %q: weight must be positive", b.URL))
		}
		if seen[b.URL] {
			errs = append(errs, fmt.Errorf("backend %q listed twice", b.URL))
		}
		seen[b.URL] = true
	}
	if c.Retries < 0 {
		errs = append(errs, errors.New("retries must be >= 0"))
	}
	if c.HealthCheck.Interval.D() <= 0 || c.HealthCheck.Timeout.D() <= 0 {
		errs = append(errs, errors.New("health_check interval and timeout must be positive"))
	}
	return errors.Join(errs...)
}
