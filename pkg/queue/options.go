package queue

import (
	"crypto/tls"
)

const (
	defaultPasswordEnvVar = "QUEUE_PASSWORD"
	defaultUsernameEnvVar = "QUEUE_USER"
)

// Options are options for the queue.
type Options struct {
	// URL encodes how we'll connect to the queue.
	URL string

	// PasswordEnvVar is the environment variable that contains the password for the database.
	// Defaults to "QUEUE_PASSWORD".
	PasswordEnvVar string

	// UsernameEnvVar is the environment variable that contains the username for the database.
	// Defaults to "QUEUE_USER".
	UsernameEnvVar string

	// TLSConfig needed to connect to the queue (optional).
	TLSConfig *tls.Config
}

func (o *Options) SetDefaults() {
	if o.PasswordEnvVar == "" {
		o.PasswordEnvVar = defaultPasswordEnvVar
	}
	if o.UsernameEnvVar == "" {
		o.UsernameEnvVar = defaultUsernameEnvVar
	}
}
