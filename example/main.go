package main

import (
	"github.com/andrew-solarstorm/digo"
	"github.com/andrew-solarstorm/tpu_relay"
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
		&tpu_relay.Config{},

		/// Services
		&tpu_relay.TPUService{},
		&tpu_relay.ScheduleService{},
		&tpu_relay.WsSlotIngestService{},
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
