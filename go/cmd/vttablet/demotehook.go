package main

import (
	"flag"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"time"

	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/log"
	_ "vitess.io/vitess/go/vt/topo/zk2topo"
	_ "vitess.io/vitess/go/vt/vttablet/grpctmclient"
)

const (
	topoOpTimeout = time.Duration(10) * time.Second
	longOpTimeout = time.Duration(60) * time.Second
)

var (
	reparentHookTabletType = flag.String("reparent_hook_tablet_type", "SPARE", "tablet type to reparent to")
	reparentHookTimeout    = flag.Duration("reparent_hook_timeout", 10*time.Minute, "timeout for the reparent hook execution")
	expFactor              = flag.Int("reparent_hook_exponential_factor", 2, "exponential factor sleep interval. Defaults to 2 = 1, 2, 4, 8, ...")
	initialBackoff         = flag.Duration("reparent_hook_initial_backoff", time.Second, "the unit of time to apply to exponential factor")
)

func initDemoteHandler(agent *tabletmanager.ActionAgent) {
	tabletType, err := topoproto.ParseTabletType(*reparentHookTabletType)
	if err != nil {
		log.Fatalf("could not parse reparent_hook_tablet_type: %s", err)
	}
	hook := &DemoteHandler{agent: agent, tabletType: tabletType, timeout: *reparentHookTimeout}
	http.Handle("/sq/demote", hook)
}

// DemoteHandler defines the shutdown reparent hook to run during graceful shutdown
type DemoteHandler struct {
	agent      *tabletmanager.ActionAgent
	tabletType topodata.TabletType
	timeout    time.Duration
}

func (s *DemoteHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	log.Infof("starting demote")
	err := s.Run()
	if err != nil {
		log.Errorf("demote failed: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("demote failed"))
	}
}

// Run executed the reparent hook
func (s *DemoteHandler) Run() error {
	span, spanCtx := trace.NewSpan(context.Background(), "demote")
	defer span.Finish()

	ctx, cancel := context.WithTimeout(spanCtx, s.timeout)
	defer cancel()
	return doUntillSuccessOrTimeout(ctx, func() error {
		return s.demoteTablet(ctx)
	})
}

func (s *DemoteHandler) demoteTablet(ctx context.Context) error {
	shardInfo, err := s.getShardInfo(ctx)
	if err != nil {
		log.Errorf("error retrieving shard info %v: %v", *agent.TabletAlias, err)
		return err
	}

	tablet := s.agent.Tablet()
	if topoproto.TabletAliasEqual(tablet.Alias, shardInfo.MasterAlias) {
		log.Infof("tablet %s is the master, reparenting to available %v in keyspace %s, shard %s", s.agent.TabletAlias, s.tabletType, tablet.Keyspace, tablet.Shard)

		targetTablet, err := s.findTargetTablet(ctx)
		if err != nil {
			log.Errorf("error finding tablet by type %v, keyspace: %s, shard: %s: %v", s.tabletType, tablet.Keyspace, tablet.Shard, err)
			return err
		}

		err = s.reparentTablet(ctx, targetTablet)
		if err != nil {
			log.Errorf("error reparenting to tablet %v: %v", *targetTablet.Alias, err)
			return err
		}
	}

	return nil
}

func (s *DemoteHandler) getShardInfo(ctx context.Context) (*topo.ShardInfo, error) {
	var shardInfo *topo.ShardInfo
	err := retryWithBackoff(ctx, func() error {
		ctx, cancel := context.WithTimeout(ctx, topoOpTimeout)
		defer cancel()

		var err error
		tablet := s.agent.Tablet()
		shardInfo, err = s.agent.TopoServer.GetShard(ctx, tablet.Keyspace, tablet.Shard)
		return err
	})
	return shardInfo, err
}

func (s *DemoteHandler) reparentTablet(ctx context.Context, targetTablet *topodata.Tablet) error {
	log.Infof("reparenting to: %v", topoproto.TabletAliasString(targetTablet.Alias))
	tmc := tmclient.NewTabletManagerClient()

	err := retryWithBackoff(ctx, func() error {
		hcCtx, hcCancel := context.WithTimeout(ctx, longOpTimeout)
		defer hcCancel()
		err := tmc.RunHealthCheck(hcCtx, targetTablet)
		if err != nil {
			log.Errorf("failed to healthcheck tablet alias %v: %v", targetTablet.Alias, err)
		}
		return err
	})

	if err != nil {
		return err
	}

	err = retryWithBackoff(ctx, func() error {
		terCtx, terCancel := context.WithTimeout(ctx, longOpTimeout)
		defer terCancel()

		err := tmc.TabletExternallyReparented(terCtx, targetTablet, "")
		if err != nil {
			log.Errorf("could not reparent to tablet alias %v: %v", targetTablet.Alias, err)
		}

		return err
	})
	if err != nil {
		return err
	}

	// We gotta wait until we are no longer master as TER runs asynchronously
	err = waitUntil(ctx, func() (bool, error) {
		tablet := s.agent.Tablet()
		if tablet.Type == topodata.TabletType_MASTER {
			log.Infof("still MASTER tablet type, waiting a bit longer")
			return false, nil
		}

		shardInfo, err := s.getShardInfo(ctx)
		if err != nil {
			log.Errorf("error retrieving shard info %v: %v", *agent.TabletAlias, err)
			return false, err
		}
		if topoproto.TabletAliasEqual(tablet.Alias, shardInfo.MasterAlias) {
			log.Infof("still shard master, waiting a bit longer")
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		log.Errorf("failed to wait for demote to complete: %v", err)
	}

	log.Infof("TabletExternallyReparented to %v done", topoproto.TabletAliasString(targetTablet.Alias))
	return err
}

func waitUntil(ctx context.Context, predicate func() (bool, error)) error {
	for {
		result, err := predicate()
		if err != nil {
			return err
		}
		if result {
			return nil
		}
		select {
		case <-ctx.Done():
			// We timed out out, oh bugger, game over
			return vterrors.Errorf(vtrpc.Code_DEADLINE_EXCEEDED, "time out")
		case <-time.After(time.Duration(*initialBackoff)):
		}
	}
}

func (s *DemoteHandler) findTargetTablet(ctx context.Context) (*topodata.Tablet, error) {
	sourceTablet := s.agent.Tablet()
	var targetTablets []*topodata.Tablet

	err := retryWithBackoff(ctx, func() error {
		ctx, cancel := context.WithTimeout(ctx, topoOpTimeout)
		defer cancel()

		tabletInfos, err := s.agent.TopoServer.GetTabletMapForShardByCell(ctx, sourceTablet.Keyspace, sourceTablet.Shard, []string{sourceTablet.Alias.Cell})
		if err != nil {
			log.Errorf("could not get tablet infos for shard %s: %v", sourceTablet.Shard, err)
			return err
		}

		for _, ti := range tabletInfos {
			if ti.Tablet.Type == s.tabletType {
				targetTablets = append(targetTablets, ti.Tablet)
				break
			}
		}
		if len(targetTablets) == 0 {
			return fmt.Errorf("could not find any %v tablets in shard %s to become MASTER", s.tabletType, sourceTablet.Shard)
		}

		return nil
	})

	targetTablet := targetTablets[rand.Intn(len(targetTablets))]

	return targetTablet, err
}

func retryWithBackoff(ctx context.Context, f func() error) error {
	attempt := 0
	base := *expFactor
	multiplier := *initialBackoff
	for {
		err := f()

		if err != nil {
			interval := math.Pow(float64(base), float64(attempt))
			attempt = attempt + 1
			select {
			case <-ctx.Done():
				// We timed out out, oh bugger, game over
				return err
			case <-time.After(time.Duration(interval) * multiplier):
			}
		} else {
			return nil
		}
	}
}

func doUntillSuccessOrTimeout(ctx context.Context, f func() error) error {
	done := make(chan error)

	for {
		go func() {
			done <- f()
		}()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-done:
			if err == nil {
				return nil
			}
		}
	}
}
