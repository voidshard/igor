package main

import (
	"os"

	"github.com/jessevdk/go-flags"

	"github.com/voidshard/igor/pkg/api"
	"github.com/voidshard/igor/pkg/api/http"
	"github.com/voidshard/igor/pkg/database"
	"github.com/voidshard/igor/pkg/queue"
)

const (
	// defaults are set in the cmd/db_migrate/run.sh file, which obviously you should set
	defaultDatabaseURL = "postgres://igorreadwrite:readwrite@localhost:5432/igor?sslmode=disable&search_path=igor"

	// default to local redis no pass
	defaultRedisURL = "redis://localhost:6379/0"
)

var CLI struct {
	Addr string `long:"addr" env:"ADDR" description:"Address to bind to" default:"localhost:8100"`

	DatabaseURL string `long:"database-url" env:"DATABASE_URL" description:"Database connection string"`

	RedisURL string `long:"redis-url" env:"REDIS_URL" description:"Redis connection string"`

	Debug bool `long:"debug" env:"DEBUG" description:"Enable debug logging"`
}

func main() {
	var parser = flags.NewParser(&CLI, flags.Default)
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

	if CLI.DatabaseURL == "" {
		CLI.DatabaseURL = defaultDatabaseURL
	}
	if CLI.RedisURL == "" {
		CLI.RedisURL = defaultRedisURL
	}

	api, err := api.NewFromOptions(
		&database.Options{URL: CLI.DatabaseURL},
		&queue.Options{URL: CLI.RedisURL},
		nil, // uses default API options
	)
	if err != nil {
		panic(err)
	}

	s := http.NewServer(CLI.Addr, CLI.Debug)
	s.ServeForever(api)
}
