package server

import (
	"crypto/tls"
	"fmt"
	"sync/atomic"
)

// certStore holds the TLS certificate, swapped on reload so a renewal needs no restart and
// cuts no stream
type certStore struct {
	cert atomic.Pointer[tls.Certificate]
}

func (c *certStore) load(certFile, keyFile string) error {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return fmt.Errorf("cannot load certificate: %w", err)
	}
	c.cert.Store(&cert)
	return nil
}

func (c *certStore) get(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	return c.cert.Load(), nil
}
