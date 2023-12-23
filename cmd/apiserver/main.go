package main

import (
	"os"

	"github.com/jessevdk/go-flags"

	"github.com/voidshard/igor/pkg/api"
	"github.com/voidshard/igor/pkg/api/http/server"
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

	StaticDir string `long:"static-dir" env:"STATIC_DIR" default:"" description:"Serve static files from this directory"`
}

func main() {
	// This main runs an API server (in this case, http) so that callers can interact with Igor over HTTP.
	// Since this is configured with OptionsClientDefault it does not run any background routines
	// that Igor needs to function (ie, to process events, queue tasks etc).
	//
	// This is intended purely to serve Igor's service API to clients over the network. Though you could
	// have one server type do both if you wanted.
	//
	// If you wished to interact with Igor via. importing the pkg libraries, then you don't need to run this.
	//
	// Alternatively, you could add more servers under pkg/api/ to serve Igor's API over other protocols like
	// gRPC, thrift or whatever you like and modifiy this to serve them all.

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

	api, err := api.New(
		&database.Options{URL: CLI.DatabaseURL},
		&queue.Options{URL: CLI.RedisURL},
		api.OptionsClientDefault(),
	)
	if err != nil {
		panic(err)
	}

	s := server.NewServer(CLI.Addr, CLI.StaticDir, CLI.Debug)
	s.ServeForever(api)
}
