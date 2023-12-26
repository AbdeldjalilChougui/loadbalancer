package config

import (
	"strings"
	"testing"
	"time"
)

func TestParseDefaults(t *testing.T) {
	c, err := Parse([]byte(`{"backends":[{"url":"http://a:1"}],"health_check":{"interval":"250ms"}}`))
	if err != nil {
		t.Fatal(err)
	}
	if c.Strategy != "round_robin" || c.Listen != ":8080" || c.Backends[0].Weight != 1 {
		t.Fatalf("defaults not applied: %+v", c)
	}
	if c.HealthCheck.Interval.D() != 250*time.Millisecond || c.HealthCheck.Timeout.D() != time.Second {
		t.Fatalf("durations: %+v", c.HealthCheck)
	}
}

func TestValidate(t *testing.T) {
	bad := map[string]string{
		`{"backends":[]}`: "at least one backend",
		`{"strategy":"random","backends":[{"url":"http://a"}]}`:           "unknown strategy",
		`{"backends":[{"url":"a:80"}]}`:                                   "absolute http(s) URL",
		`{"backends":[{"url":"http://a"},{"url":"http://a"}]}`:            "listed twice",
		`{"backends":[{"url":"http://a"}],"retries":-1}`:                  "retries",
		`{"backends":[{"url":"http://a"}],"health_check":{"interval":5}}`: "duration",
	}
	for in, want := range bad {
		_, err := Parse([]byte(in))
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: got %v, want error containing %q", in, err, want)
		}
	}
}
