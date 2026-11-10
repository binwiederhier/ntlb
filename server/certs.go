package server

import (
	"crypto/tls"
	"fmt"
	"sync/atomic"

	"heckel.io/ntlb/config"
)

// certStore holds the sites' TLS certificates, swapped on reload so a renewal needs no restart
// and cuts no stream
type certStore struct {
	certs atomic.Pointer[[]*tls.Certificate] // In site order; never empty
}

// loadCerts loads the certificate of every site that has one, each file pair once
func loadCerts(conf *config.Config) ([]*tls.Certificate, error) {
	var certs []*tls.Certificate
	seen := make(map[[2]string]bool)
	for _, site := range conf.Sites {
		files := [2]string{site.CertFile, site.KeyFile}
		if site.CertFile == "" || seen[files] {
			continue
		}
		seen[files] = true
		cert, err := tls.LoadX509KeyPair(site.CertFile, site.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("site %s: cannot load certificate: %w", site.Name, err)
		}
		certs = append(certs, &cert)
	}
	return certs, nil
}

// store replaces the certificates; an empty list (no site has one, so HTTPS is going away with
// the next restart) keeps the old ones
func (c *certStore) store(certs []*tls.Certificate) {
	if len(certs) > 0 {
		c.certs.Store(&certs)
	}
}

// get picks the first certificate that is valid for the client's SNI name, or the first one if
// none is (or the client sent no name), like nginx's default server
func (c *certStore) get(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
	certs := *c.certs.Load()
	if hello.ServerName != "" {
		for _, cert := range certs {
			if hello.SupportsCertificate(cert) == nil {
				return cert, nil
			}
		}
	}
	return certs[0], nil
}
