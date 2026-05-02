package tpu_relay

import (
	"context"

	"github.com/andrew-solarstorm/digo"
	"github.com/andrew-solarstorm/yellowstone-grpc-client-go"
	pb "github.com/andrew-solarstorm/yellowstone-grpc-client-go/proto"
)

const SLOT_SVC = "slot-ingest-service"

type SlotIngestService struct {
	digo.BaseDIInstance

	grpc     *yellowstone.GeyserGrpcClient
	endpoint string
	token    string

	scheduleSvc *ScheduleService
}

func NewSlotIngest(url string, token string, scheduler *ScheduleService) *SlotIngestService {
	return &SlotIngestService{
		endpoint:    url,
		token:       token,
		scheduleSvc: scheduler,
	}
}

func (svc *SlotIngestService) ID() string {
	return SLOT_SVC
}

func (svc *SlotIngestService) Configure(c digo.IContainer) error {
	conf := digo.GetConfig[*Config](c)
	svc.token = conf.GRPCToken
	svc.endpoint = conf.GRPCUrl
	svc.scheduleSvc = digo.Instance[*ScheduleService](SCHEDULER_SVC, c)
	return nil
}

func (svc *SlotIngestService) Start() error {
	builder, err := yellowstone.BuildFromShared(svc.endpoint)
	if err != nil {
		return err
	}
	builder.XToken(svc.token).KeepAliveWhileIdle(true)

	cli, err := builder.Connect(context.Background())
	if err != nil {
		return err
	}
	svc.grpc = cli

	interUpd := false
	fiterByCommitmentLevel := true
	req := &pb.SubscribeRequest{
		Slots: map[string]*pb.SubscribeRequestFilterSlots{
			"slot": {
				InterslotUpdates:   &interUpd,
				FilterByCommitment: &fiterByCommitmentLevel,
			},
		},
		Commitment: pb.CommitmentLevel_PROCESSED.Enum(),
	}

	stream, err := svc.grpc.SubscribeWithRequest(context.Background(), req)
	if err != nil {
		return err
	}

	go svc.grpc.Start(stream, func(su *pb.SubscribeUpdate) error {
		switch su.GetUpdateOneof().(type) {
		case *pb.SubscribeUpdate_Slot:
			slotUpd := su.GetSlot()
			svc.scheduleSvc.OnSlot(slotUpd.Slot)
		default:
		}
		return nil
	})

	return nil
}

func (svc *SlotIngestService) Stop() error { return svc.grpc.Close() }
