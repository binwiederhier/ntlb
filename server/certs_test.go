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
	// Each site's certificate is served for its hosts on its listeners, the listener's first one
	// for anything else; a reload re-reads them all
	dir := t.TempDir()
	for _, name := range []string{"a.com", "b.com"} {
		writeTestCert(t, filepath.Join(dir, name+".pem"), filepath.Join(dir, name+".key"), name)
	}
	app1 := newTestUpstream(t, "app1")
	filename := filepath.Join(dir, "config.yml")
	require.NoError(t, os.WriteFile(filename, []byte(fmt.Sprintf(`
listen-admin: '127.0.0.1:0'
sites:
  - {name: a, listen-https: '127.0.0.3:0', hosts: [a.com], cert-file: %[1]s/a.com.pem, key-file: %[1]s/a.com.key, health-check: {interval: 1h, timeout: 1s}, upstreams: [{name: app1, addresses: [%[2]q]}]}
  - {name: b, listen-https: '127.0.0.3:0', hosts: [b.com], cert-file: %[1]s/b.com.pem, key-file: %[1]s/b.com.key, health-check: {interval: 1h, timeout: 1s}, upstreams: [{name: app1, addresses: [%[2]q]}]}
  - {name: c, listen-https: '127.0.0.2:0', hosts: [c.com], cert-file: %[1]s/b.com.pem, key-file: %[1]s/b.com.key, health-check: {interval: 1h, timeout: 1s}, upstreams: [{name: app1, addresses: [%[2]q]}]}
`, dir, app1.addr())), 0600))
	s := startTestServer(t, filename)
	served := func(serverName string) string {
		return servedBy(t, s.ListenerAddr("127.0.0.3:0"), serverName)
	}
	require.Equal(t, "a.com", served("a.com"))
	require.Equal(t, "b.com", served("b.com"))
	require.Equal(t, "a.com", served("other.com"))
	require.Equal(t, "a.com", served(""))

	// A listener has only its own sites' certificates
	require.Equal(t, "b.com", servedBy(t, s.ListenerAddr("127.0.0.2:0"), "a.com"))

	// Renewed
	writeTestCert(t, filepath.Join(dir, "b.com.pem"), filepath.Join(dir, "b.com.key"), "b.com")
	requireReload(t, s)
	require.Equal(t, "b.com", served("b.com"))

	// A certificate that cannot be read rejects the reload
	require.NoError(t, os.Remove(filepath.Join(dir, "b.com.key")))
	_, err := s.Reload()
	require.ErrorContains(t, err, "certificate")
	require.Equal(t, "b.com", served("b.com"))
}

func TestCerts_ListenerWithoutCertificates(t *testing.T) {
	// A reload that leaves an HTTPS listener without sites (until the restart) fails handshakes
	store := &certStore{}
	store.store(nil)
	_, err := store.getter("127.0.0.1:443")(&tls.ClientHelloInfo{ServerName: "a.com"})
	require.ErrorIs(t, err, errNoCertificate)
}

func servedBy(t *testing.T, addr, serverName string) string {
	conn, err := tls.Dial("tcp", addr, &tls.Config{
		ServerName:         serverName,
		InsecureSkipVerify: true,
	})
	require.NoError(t, err)
	defer conn.Close()
	return conn.ConnectionState().PeerCertificates[0].Subject.CommonName
}
