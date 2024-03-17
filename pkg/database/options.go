package database

const (
	defaultPasswordEnvVar = "DATABASE_PASSWORD"
	defaultUsernameEnvVar = "DATABASE_USER"
)

// Options are options for the database.
type Options struct {
	// URL encodes how we'll connect to the database.
	URL string

	// PasswordEnvVar is the environment variable that contains the password for the database.
	// This value is substituted into the URL (ie. "postgres://username:$PASS@localhost:5432" will have "PASS" subbed in).
	// Defaults to "DATABASE_PASSWORD".
	PasswordEnvVar string

	// UsernameEnvVar is the environment variable that contains the username for the database.
	// This value is substituted into the URL (ie. "postgres://$USER:password@localhost:5432" will have "USER" subbed in).
	// Defaults to "DATABASE_USER".
	UsernameEnvVar string
}

func (o *Options) SetDefaults() {
	if o.PasswordEnvVar == "" {
		o.PasswordEnvVar = defaultPasswordEnvVar
	}
	if o.UsernameEnvVar == "" {
		o.UsernameEnvVar = defaultUsernameEnvVar
	}
}
