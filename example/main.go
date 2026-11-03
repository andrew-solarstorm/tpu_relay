package main

import (
	"os"
	"strings"
	"time"

	"github.com/andrew-solarstorm/digo"
	"github.com/joho/godotenv"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// logLevel reads LOG_LEVEL (trace, debug, info, warn, error), defaulting to info.
func logLevel() zerolog.Level {
	val := os.Getenv("LOG_LEVEL")
	if val == "" {
		return zerolog.InfoLevel
	}
	lvl, err := zerolog.ParseLevel(strings.ToLower(val))
	if err != nil {
		log.Warn().Str("LOG_LEVEL", val).Msg("invalid log level, using info")
		return zerolog.InfoLevel
	}
	return lvl
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Error().Err(err).Msg("failed to load env")
		return
	}

	log.Logger = log.Output(zerolog.ConsoleWriter{
		Out:        os.Stdout,
		TimeFormat: time.RFC3339, // Format the timestamp nicely
	})
	zerolog.SetGlobalLevel(logLevel())

	container, err := digo.New(
		// config
		&Config{},

		/// Services
		&RelayService{},
		&SlotFeedService{},
		&HTTPService{},
	)
	if err != nil {
		return
	}

	err = container.Run()
	if err != nil {
		return
	}
}
