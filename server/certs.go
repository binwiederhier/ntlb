package server

import (
	"crypto/tls"
	"errors"
	"fmt"
	"slices"
	"sync/atomic"

	"heckel.io/ntlb/config"
)

var (
	errNoCertificate = errors.New("no certificate for this listener")
)

// certStore holds the TLS certificates of each HTTPS listener's sites, swapped on reload so a
// renewal needs no restart and cuts no stream
type certStore struct {
	certs atomic.Pointer[map[string][]*tls.Certificate] // By listener address, in site order
}

// loadCerts loads the certificates of every HTTPS listener's sites, each file pair once
func loadCerts(conf *config.Config) (map[string][]*tls.Certificate, error) {
	loaded := make(map[[2]string]*tls.Certificate)
	certs := make(map[string][]*tls.Certificate)
	for _, l := range conf.Listeners {
		if !l.TLS {
			continue
		}
		for _, site := range l.Sites {
			files := [2]string{site.CertFile, site.KeyFile}
			cert, ok := loaded[files]
			if !ok {
				c, err := tls.LoadX509KeyPair(site.CertFile, site.KeyFile)
				if err != nil {
					return nil, fmt.Errorf("site %s: cannot load certificate: %w", site.Name, err)
				}
				cert = &c
				loaded[files] = cert
			}
			if !slices.Contains(certs[l.Addr], cert) {
				certs[l.Addr] = append(certs[l.Addr], cert)
			}
		}
	}
	return certs, nil
}

// store replaces the certificates
func (c *certStore) store(certs map[string][]*tls.Certificate) {
	c.certs.Store(&certs)
}

// getter returns the tls.Config.GetCertificate of a listener: the first of its sites'
// certificates that is valid for the client's SNI name, or the first one if none is (or the
// client sent no name), like nginx's default server. A listener that a reload left without
// certificates (it goes away with the next restart) fails the handshake.
func (c *certStore) getter(listen string) func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	return func(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
		certs := (*c.certs.Load())[listen]
		if len(certs) == 0 {
			return nil, errNoCertificate
		}
		if hello.ServerName != "" {
			for _, cert := range certs {
				if hello.SupportsCertificate(cert) == nil {
					return cert, nil
				}
			}
		}
		return certs[0], nil
	}
}
