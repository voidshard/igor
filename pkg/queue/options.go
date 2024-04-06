package queue

import (
	"crypto/tls"
)

// Options are options for the queue.
type Options struct {
	// URL encodes how we'll connect to the queue.
	URL string

	// TLSConfig needed to connect to the queue (optional).
	TLSConfig *tls.Config
}
