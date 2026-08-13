package indexer

import (
	"testing"

	"github.com/onbloc/gno-ibc-relayer-api/internal/config"
	"github.com/pashagolub/pgxmock/v4"
)

func TestNew(t *testing.T) {
	relayerDB, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("pgxmock.NewPool: %v", err)
	}
	t.Cleanup(relayerDB.Close)
	bridgeDB := newFakeBridgeDB()
	cfg := config.IndexerConfig{PollIntervalSec: 5, BatchSize: 100}
	chains := []config.ChannelChain{{SrcChainID: "dev", DstChainID: "11155111"}}

	idx := New(relayerDB, bridgeDB, cfg, chains)

	if idx.relayerDB != relayerDB {
		t.Error("New() did not wire relayerDB")
	}
	if idx.bridgeDB != bridgeDB {
		t.Error("New() did not wire bridgeDB")
	}
	if idx.cfg != cfg {
		t.Errorf("New() cfg = %+v, want %+v", idx.cfg, cfg)
	}
	if len(idx.chains) != 1 {
		t.Errorf("New() chains = %+v, want 1 entry", idx.chains)
	}
}

func TestAckErrMessage(t *testing.T) {
	cases := []struct {
		ackErr string
		want   string
	}{
		{"", "ack error"},
		{"insufficient funds", "ack error: insufficient funds"},
	}
	for _, tc := range cases {
		if got := ackErrMessage(tc.ackErr); got != tc.want {
			t.Errorf("ackErrMessage(%q) = %q, want %q", tc.ackErr, got, tc.want)
		}
	}
}
