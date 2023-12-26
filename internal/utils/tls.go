package utils

import (
	"crypto/tls"
	"crypto/x509"
	"os"
)

func setDefaults(cfg *tls.Config) {
	cfg.MinVersion = tls.VersionTLS12
	cfg.CurvePreferences = []tls.CurveID{tls.CurveP521, tls.CurveP384, tls.CurveP256}
	cfg.CipherSuites = []uint16{
		tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
		tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
		tls.TLS_RSA_WITH_AES_256_GCM_SHA384,
		tls.TLS_RSA_WITH_AES_256_CBC_SHA,
	}
}

func TLSConfig(cacert, cert, key string) (*tls.Config, error) {
	if cacert == "" && cert == "" && key == "" {
		return nil, nil
	}

	cfg := &tls.Config{}
	setDefaults(cfg)

	if cert != "" && key != "" {
		tlscert, err := tls.LoadX509KeyPair(cert, key)
		if err != nil {
			return nil, err
		}
		cfg.Certificates = []tls.Certificate{tlscert}
	}

	if cacert != "" {
		tlscert, err := os.ReadFile(cacert)
		if err != nil {
			return nil, err
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(tlscert)

		cfg.RootCAs = caCertPool
	}

	return cfg, nil
}
