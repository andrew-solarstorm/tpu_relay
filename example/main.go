package main

import (
	"github.com/andrew-solarstorm/digo"
	"github.com/joho/godotenv"
	"github.com/rs/zerolog/log"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Error().Err(err).Msg("failed to load env")
		return
	}

	container, err := digo.New(
		// config
		&Config{},

		/// Services
		// started in this order, HTTPService blocks so it stays last
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
