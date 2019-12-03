package tabletmanagertest

import (
	"context"
	"flag"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/topo/topoproto"

	"github.com/stretchr/testify/assert"
	"vitess.io/vitess/go/vt/logutil"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	"vitess.io/vitess/go/vt/wrangler"
	"vitess.io/vitess/go/vt/wrangler/testlib"

	// import the gRPC client implementation for tablet manager
	_ "vitess.io/vitess/go/vt/vttablet/grpctmclient"

	// import the gRPC client implementation for query service
	_ "vitess.io/vitess/go/vt/vttablet/grpctabletconn"
)

func init() {
	*tmclient.TabletManagerProtocol = "grpc"
}

func TestMasterRepair(t *testing.T) {
	freq := 3 * time.Second
	tabletmanager.MasterCheckFreq = &freq
	checkTimeout := 60 * time.Second
	tabletmanager.MasterCheckTimeout = &checkTimeout

	ctx := context.Background()
	ks := "test_app"
	shard := "0"
	cell := "cell1"
	ts := memorytopo.NewServer(cell)
	_ = flag.Set("tablet_hostname", "localhost")
	_ = flag.Set("tablet_externally_reparented_demote_type", "SPARE")
	_ = flag.Set("disable_active_reparents", "true")

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
		if i == 2 {
			tabletType = topodatapb.TabletType_MASTER
		}
		tablet := testlib.NewFakeTablet(t, wr, cell, uint32(i), tabletType, nil, tabletOption)
		tablets = append(tablets, tablet)
		tablet.StartActionLoop(t, wr)

		if i != 2 {
			defer tablet.StopActionLoop(t)
		}
	}

	si, err := ts.GetShard(ctx, ks, shard)
	assert.NoError(t, err)
	assert.NotNil(t, si.MasterAlias)

	tablets[2].StopActionLoop(t)

	newMasterInTopology := waitWithTimeout(*tabletmanager.MasterCheckTimeout*2, func() bool {
		currentSi, err := ts.GetShard(ctx, ks, shard)
		if err != nil {
			return false
		}

		return !topoproto.TabletAliasEqual(si.MasterAlias, currentSi.MasterAlias)
	})

	assert.True(t, newMasterInTopology)
}

func TestOldMasterDemoted(t *testing.T) {
	freq := 3 * time.Second
	tabletmanager.MasterCheckFreq = &freq
	checkTimeout := 60 * time.Second
	tabletmanager.MasterCheckTimeout = &checkTimeout

	ctx := context.Background()
	ks := "test_app"
	shard := "0"
	cell := "cell1"
	ts := memorytopo.NewServer(cell)
	err := flag.Set("tablet_hostname", "localhost")
	assert.NoError(t, err)
	err = flag.Set("tablet_externally_reparented_demote_type", "SPARE")
	assert.NoError(t, err)
	err = flag.Set("disable_active_reparents", "true")
	assert.NoError(t, err)

	if err = ts.CreateKeyspace(ctx, ks, &topodatapb.Keyspace{}); err != nil {
		t.Fatalf("CreateKeyspace failed: %v", err)
	}

	err = topotools.RebuildKeyspace(ctx, logutil.NewConsoleLogger(), ts, ks, []string{cell})
	assert.NoError(t, err)

	if err := ts.CreateShard(ctx, ks, shard); err != nil {
		t.Fatalf("CreateShard failed: %v", err)
	}

	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())
	tabletOption := testlib.TabletKeyspaceShard(t, ks, shard)
	var tablets []*testlib.FakeTablet

	// Initialize two tablets as master, shard sync should demote first tablet back to SPARE
	// as a new master is initialized
	for i := 0; i < 2; i++ {
		tablet := testlib.NewFakeTablet(t, wr, cell, uint32(i), topodatapb.TabletType_MASTER, nil, tabletOption, testlib.ForceInitTablet())
		tablets = append(tablets, tablet)
		tablet.StartActionLoop(t, wr)
		defer tablet.StopActionLoop(t)
	}

	// Wait till oldMaster is demoted back to SPARE
	oldMasterDemoted := waitWithTimeout(60*time.Second, func() bool {
		si, err := ts.GetShard(ctx, ks, shard)
		assert.NoError(t, err)
		return topoproto.TabletAliasEqual(si.MasterAlias, tablets[1].Tablet.Alias)
	})
	assert.True(t, oldMasterDemoted)
	assert.Equal(t, topodatapb.TabletType_SPARE, tablets[0].Agent.Tablet().Type)
}

func waitWithTimeout(duration time.Duration, condition func() bool) bool {
	for {
		select {
		case <-time.After(duration):
			return false
		default:
			result := condition()
			if result {
				return true
			}
			continue
		}
	}
}
