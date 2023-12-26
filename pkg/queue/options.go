package queue

import (
	"crypto/tls"
)

// Options are options for the queue.
type Options struct {
	// URL encodes how we'll connect to the queue.
	URL string

	// Username is the username we'll use to connect to the queue (optional).
	Username string

	// Password is the password we'll use to connect to the queue (optional).
	Password string

	// TLSConfig needed to connect to the queue (optional).
	TLSConfig *tls.Config
}
