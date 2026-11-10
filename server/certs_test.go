package server

import (
	"crypto/tls"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCerts_PickedBySNI(t *testing.T) {
	// Each site's certificate is served for its hosts, the first one for anything else; a
	// reload re-reads them all
	dir := t.TempDir()
	for _, name := range []string{"a.com", "b.com"} {
		writeTestCert(t, filepath.Join(dir, name+".pem"), filepath.Join(dir, name+".key"), name)
	}
	app1 := newTestUpstream(t, "app1")
	filename := filepath.Join(dir, "config.yml")
	writeSitesConfig := func() {
		require.NoError(t, os.WriteFile(filename, []byte(fmt.Sprintf(`
listen-https: '127.0.0.1:0'
listen-admin: '127.0.0.1:0'
sites:
  - {name: a, hosts: [a.com], cert-file: %[1]s/a.com.pem, key-file: %[1]s/a.com.key, health-check: {interval: 1h, timeout: 1s}, upstreams: [{name: app1, addresses: [%[2]q]}]}
  - {name: b, hosts: [b.com], cert-file: %[1]s/b.com.pem, key-file: %[1]s/b.com.key, health-check: {interval: 1h, timeout: 1s}, upstreams: [{name: app1, addresses: [%[2]q]}]}
  - {name: c, hosts: [c.com], cert-file: %[1]s/a.com.pem, key-file: %[1]s/a.com.key, health-check: {interval: 1h, timeout: 1s}, upstreams: [{name: app1, addresses: [%[2]q]}]}
`, dir, app1.addr())), 0600))
	}
	writeSitesConfig()
	s := startTestServer(t, filename)
	served := func(serverName string) string {
		conn, err := tls.Dial("tcp", s.HTTPSAddr(), &tls.Config{
			ServerName:         serverName,
			InsecureSkipVerify: true,
		})
		require.NoError(t, err)
		defer conn.Close()
		return conn.ConnectionState().PeerCertificates[0].Subject.CommonName
	}
	require.Equal(t, "a.com", served("a.com"))
	require.Equal(t, "b.com", served("b.com"))
	require.Equal(t, "a.com", served("other.com"))
	require.Equal(t, "a.com", served(""))

	// Renewed
	writeTestCert(t, filepath.Join(dir, "b.com.pem"), filepath.Join(dir, "b.com.key"), "b.com")
	before := s.certs.get
	requireReload(t, s)
	require.Equal(t, "b.com", served("b.com"))
	require.NotNil(t, before)

	// A certificate that cannot be read rejects the reload
	require.NoError(t, os.Remove(filepath.Join(dir, "b.com.key")))
	_, err := s.Reload()
	require.ErrorContains(t, err, "certificate")
	require.Equal(t, "b.com", served("b.com"))
}
