package tpu_relay

import (
	_ "embed"

	"github.com/bytedance/sonic"
	"github.com/gagliardetto/solana-go"
)

type leaderInfo struct {
	Pubkey     solana.PK `json:"pubkey"`
	VotePubkey solana.PK `json:"votePubkey"`
	Name       string    `json:"name"`
	IPCountry  string    `json:"ipCountry"`
	IPCity     string    `json:"ipCity"`
	IsJITO     bool      `json:"isJito"`
	Client     string    `json:"softwareClient"`
}

//go:embed validators.json
var validatorsContents string
var validatorInfos map[solana.PK]leaderInfo

func init() {
	infos := make([]*leaderInfo, 0)
	err := sonic.Unmarshal([]byte(validatorsContents), &infos)
	if err != nil {
		return
	}

	leaderInfos := make(map[solana.PK]*leaderInfo, len(infos))
	for _, info := range infos {
		leaderInfos[info.Pubkey] = info
	}

	validatorsContents = ""
}
