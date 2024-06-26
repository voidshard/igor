package main

import (
	"os"

	"github.com/jessevdk/go-flags"
)

type optsDatabase struct {
	DatabaseURL string `long:"database-url" env:"DATABASE_URL" description:"Database connection string" default:"postgres://postgres:test@localhost:5432/igor?sslmode=disable"`
}

type optsQueue struct {
	QueueTLSCert   string `long:"queue-tls-cert" env:"QUEUE_TLS_CERT" description:"Queue TLS certificate"`
	QueueTLSKey    string `long:"queue-tls-key" env:"QUEUE_TLS_KEY" description:"Queue TLS key"`
	QueueTLSCaCert string `long:"queue-tls-ca-cert" env:"QUEUE_TLS_CA_CERT" description:"Queue TLS CA certificate"`

	QueueURL string `long:"queue-url" env:"QUEUE_URL" default:"redis://localhost:6379/0" description:"Queue connection string. If your redis requires a username and password for Asynq you can set the following environment variables: ASYNQ_REDIS_USER, ASYNQ_REDIS_PASSWORD"`
}

type optsGeneral struct {
	Debug bool `long:"debug" env:"DEBUG" description:"Enable debug logging"`
}

var cmdAPI optsAPI
var cmdWorker optsWorker
var parser = flags.NewParser(nil, flags.Default)

func init() {
	parser.AddCommand("api", "Run Igor API Server", docApi, &cmdAPI)
	parser.AddCommand("worker", "Run Igor background worker", docWorker, &cmdWorker)
	parser.AddCommand("migrate", "Postgres Migration Operations", docMigrate, &optsMigrate{})
}

func main() {
	if _, err := parser.Parse(); err != nil {
		switch flagsErr := err.(type) {
		case flags.ErrorType:
			if flagsErr == flags.ErrHelp {
				os.Exit(0)
			}
			os.Exit(1)
		default:
			os.Exit(1)
		}
	}
}
