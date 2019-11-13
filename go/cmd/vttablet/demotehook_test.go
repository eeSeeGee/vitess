package main

import (
	"context"
	"flag"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"vitess.io/vitess/go/vt/logutil"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	"vitess.io/vitess/go/vt/wrangler"
	"vitess.io/vitess/go/vt/wrangler/testlib"
)

func TestDemoteHook(t *testing.T) {
	ctx := context.Background()
	ks := "test_app"
	shard := "0"
	cell := "cell1"
	ts := memorytopo.NewServer(cell)
	_ = flag.Set("tablet_hostname", "localhost")
	_ = flag.Set("demote_master_type", "SPARE")

	if err := ts.CreateKeyspace(ctx, ks, &topodatapb.Keyspace{}); err != nil {
		t.Fatalf("CreateKeyspace failed: %v", err)
	}

	err := topotools.RebuildKeyspace(ctx, logutil.NewConsoleLogger(), ts, ks, []string{cell})
	assert.NoError(t, err)

	if err := ts.CreateShard(ctx, ks, shard); err != nil {
		t.Fatalf("CreateShard failed: %v", err)
	}

	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())
	tabletOption := testlib.TabletKeyspaceShard(t, ks, shard)
	tabletType := topodatapb.TabletType_SPARE
	var tablets []*testlib.FakeTablet

	for i := 0; i < 3; i++ {
		//Make tablet 2 the master
		if i == 2 {
			tabletType = topodatapb.TabletType_MASTER
		}
		tablet := testlib.NewFakeTablet(t, wr, cell, uint32(i), tabletType, nil, tabletOption)
		tablets = append(tablets, tablet)
		tablet.StartActionLoop(t, wr)
		tablet.Agent.MasterDemoteTabletType = topodatapb.TabletType_SPARE
		defer tablet.StopActionLoop(t)
	}

	demoteHandler := &DemoteHandler{
		agent:      tablets[2].Agent,
		tabletType: topodatapb.TabletType_SPARE,
		timeout:    time.Duration(60 * time.Second),
	}

	err = demoteHandler.Run()
	assert.NoError(t, err)

	timeout := time.After(demoteHandler.timeout)

Wait:
	for {
		select {
		case <-timeout:
			assert.Fail(t, "Still master tablet, demote hook did not complete in time")
			break Wait
		default:
			si, err := ts.GetShard(ctx, ks, shard)
			assert.NoError(t, err)
			if !topoproto.TabletAliasEqual(si.MasterAlias, demoteHandler.agent.TabletAlias) {
				break Wait
			}
			time.Sleep(1 * time.Second)
		}
	}
	assert.Equal(t, topodatapb.TabletType_SPARE, demoteHandler.agent.Tablet().Type)
}
