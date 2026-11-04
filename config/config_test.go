package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParse_Defaults(t *testing.T) {
	c, err := Parse([]byte(`
listen-http: ":80"
upstreams:
  - name: app1
    addresses: ["10.0.0.1:2586", "10.0.0.1:2587"]
`))
	require.NoError(t, err)
	require.Equal(t, DefaultListenAdmin, c.ListenAdmin)
	require.Equal(t, DefaultHealthPath, c.HealthCheck.Path)
	require.Equal(t, DefaultHealthFails, c.HealthCheck.Fails)
	require.Equal(t, DefaultUpstreamReadTimeout, c.UpstreamReadTimeout)
	require.Equal(t, DefaultKeepaliveIdle, c.ClientKeepalive.Idle)
	require.Equal(t, DefaultKeepaliveCount, c.UpstreamKeepalive.Count)
	require.Len(t, c.Upstreams, 1)
	require.Equal(t, []string{"10.0.0.1:2586", "10.0.0.1:2587"}, c.Upstreams[0].Addresses)
}

func TestParse_Overrides(t *testing.T) {
	c, err := Parse([]byte(`
listen-https: ":443"
cert-file: /tmp/cert.pem
key-file: /tmp/key.pem
drain-duration: 10m
client-keepalive:
  idle: 3m
  interval: 30s
  count: 5
upstream-keepalive:
  disable: true
health-check:
  path: /v1/health
  interval: 2s
  timeout: 1s
upstreams:
  - name: app1
    addresses: ["10.0.0.1:2586"]
`))
	require.NoError(t, err)
	require.Equal(t, 10*time.Minute, c.DrainDuration)
	require.Equal(t, 3*time.Minute, c.ClientKeepalive.KeepAliveConfig().Idle)
	require.Equal(t, 5, c.ClientKeepalive.KeepAliveConfig().Count)
	require.False(t, c.UpstreamKeepalive.KeepAliveConfig().Enable)
	require.Equal(t, "/v1/health", c.HealthCheck.Path)
	require.Equal(t, 2*time.Second, c.HealthCheck.Interval)
}

func TestParse_Invalid(t *testing.T) {
	tests := []struct {
		name   string
		config string
		errMsg string
	}{
		{
			name:   "no listener",
			config: "upstreams: [{name: a, addresses: ['1.2.3.4:80']}]",
			errMsg: "listen-http or listen-https",
		},
		{
			name:   "https without cert",
			config: "listen-https: ':443'\nupstreams: [{name: a, addresses: ['1.2.3.4:80']}]",
			errMsg: "cert-file",
		},
		{
			name:   "no upstreams",
			config: "listen-http: ':80'",
			errMsg: "at least one upstream",
		},
		{
			name:   "duplicate name",
			config: "listen-http: ':80'\nupstreams: [{name: a, addresses: ['1.2.3.4:80']}, {name: a, addresses: ['1.2.3.5:80']}]",
			errMsg: "duplicate upstream name",
		},
		{
			name:   "bad name",
			config: "listen-http: ':80'\nupstreams: [{name: 'a b', addresses: ['1.2.3.4:80']}]",
			errMsg: "invalid upstream name",
		},
		{
			name:   "no addresses",
			config: "listen-http: ':80'\nupstreams: [{name: a}]",
			errMsg: "no addresses",
		},
		{
			name:   "bad address",
			config: "listen-http: ':80'\nupstreams: [{name: a, addresses: ['1.2.3.4']}]",
			errMsg: "invalid address",
		},
		{
			name:   "timeout not below interval",
			config: "listen-http: ':80'\nhealth-check: {interval: 1s, timeout: 1s}\nupstreams: [{name: a, addresses: ['1.2.3.4:80']}]",
			errMsg: "health-check.timeout",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse([]byte(tt.config))
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.errMsg)
		})
	}
}
