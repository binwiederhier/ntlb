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
  - {name: a, listen-https: '127.0.0.3:0', hosts: [a.com], cert-file: %[1]s/a.com.pem, key-file: %[1]s/a.com.key, `+testNtfySite+`, upstreams: {default: {`+testNtfyUpstream+`, servers: [{name: app1, addresses: [%[2]q]}]}}}
  - {name: b, listen-https: '127.0.0.3:0', hosts: [b.com], cert-file: %[1]s/b.com.pem, key-file: %[1]s/b.com.key, `+testNtfySite+`, upstreams: {default: {`+testNtfyUpstream+`, servers: [{name: app1, addresses: [%[2]q]}]}}}
  - {name: c, listen-https: '127.0.0.2:0', hosts: [c.com], cert-file: %[1]s/b.com.pem, key-file: %[1]s/b.com.key, `+testNtfySite+`, upstreams: {default: {`+testNtfyUpstream+`, servers: [{name: app1, addresses: [%[2]q]}]}}}
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

func TestCerts_OnlyAEADCiphersForTLS12(t *testing.T) {
	// Like the nginx config (Mozilla intermediate): TLS 1.2 with CBC suites is refused
	dir := t.TempDir()
	certFile, keyFile := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	writeTestCert(t, certFile, keyFile, "test")
	app1 := newTestUpstream(t, "app1")
	s, _ := newTestServer(t, "listen-https: '127.0.0.3:0'\ncert-file: "+certFile+"\nkey-file: "+keyFile, app1)
	dial := func(suite uint16) error {
		conn, err := tls.Dial("tcp", s.HTTPSAddr(), &tls.Config{
			InsecureSkipVerify: true,
			MaxVersion:         tls.VersionTLS12,
			CipherSuites:       []uint16{suite},
		})
		if err == nil {
			conn.Close()
		}
		return err
	}
	require.NoError(t, dial(tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256))
	require.NoError(t, dial(tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256))
	require.Error(t, dial(tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA))
	require.Error(t, dial(tls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA))
}
