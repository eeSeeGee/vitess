/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package tabletmanager

import (
	"context"
	"flag"
	"math/rand"
	"time"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
)

var (
	// MasterCheckFreq specifies the sleep time in between master checks
	MasterCheckFreq = flag.Duration("master-check-freq", 0*time.Second, "frequency to check master status")
	// MasterCheckTimeout specifies the timeout for the check master operation (not inlcuding the reparent)
	MasterCheckTimeout = flag.Duration("master-check-timeout", 30*time.Second, "master check timeout")
)

type masterRepairJob struct {
	agent *ActionAgent
	done  chan bool
	tmc   tmclient.TabletManagerClient
}

func startNewMasterRepairJob(ts *topo.Server, agent *ActionAgent) *masterRepairJob {
	if *MasterCheckFreq == 0 {
		return nil
	}

	done := make(chan bool)
	job := &masterRepairJob{
		agent: agent,
		done:  done,
		tmc:   tmclient.NewTabletManagerClient(),
	}

	job.start()
	servenv.OnTerm(func() {
		job.stop()
	})

	return job
}

func (job *masterRepairJob) start() {
	log.Infof("Starting master repair job")
	go func() {
		for {
			time.Sleep(job.calculateDelay())

			select {
			case <-job.done:
				log.Infof("Exiting master repair job")
				return
			default:
				job.checkMaster()
			}
		}
	}()
}

func (job *masterRepairJob) stop() {
	log.Infof("Stopping master repair job")
	job.done <- true
}

func (job *masterRepairJob) checkMaster() {
	ctx, cancel := context.WithTimeout(context.Background(), *MasterCheckTimeout)
	defer cancel()

	agent := job.agent
	tablet := agent.Tablet()
	ks := tablet.Keyspace
	shard := tablet.Shard
	alias := tablet.Alias
	shardInfo, err := job.agent.TopoServer.GetShard(ctx, ks, shard)
	if err != nil {
		log.Error("could not retrieve shard info for keyspace: %s, shard: %s", ks, shard)
		return
	}

	masterAlias := shardInfo.GetMasterAlias()
	if masterAlias == nil {
		log.Infof("No master found in topology for shard %s/%s, skip master check", shardInfo.Keyspace(), shardInfo.ShardName())
		return
	}

	if tablet.Type != topodata.TabletType_MASTER {
		if topoproto.TabletAliasEqual(masterAlias, alias) {
			// we're master in topology but we are not ourselves a MASTER tablet type
			// let's TER again to resolve that
			log.Warningf("Supposed to be master of the shard but have the wrong tablet type %v, running TER to self to repair", tablet.Type)
			rsCtx, rsCancel := context.WithTimeout(context.Background(), *MasterCheckTimeout)
			defer rsCancel()
			if err := agent.TabletExternallyReparented(rsCtx, topoproto.TabletAliasString(tablet.Alias)); err != nil {
				log.Errorf("Supposed to be master but could not run TER to self to repair: %v", err)
			}
			return
		}

		// don't attempt to repair if master is in a different cell
		if alias.Cell != masterAlias.Cell {
			log.Infof("Master is in a different cell, not attempting repair: tablet cell: %s, master cell: %s", alias.Cell, shardInfo.MasterAlias.Cell)
			return
		}

		masterInfo, err := agent.TopoServer.GetTablet(ctx, shardInfo.MasterAlias)
		if err != nil {
			log.Errorf("could not retrieve tablet info for master: %v, error: %v", topoproto.TabletAliasString(shardInfo.MasterAlias), err)
			return
		}

		hcCtx, hcCancel := context.WithTimeout(context.Background(), *MasterCheckTimeout)
		defer hcCancel()
		err = job.tmc.RunHealthCheck(hcCtx, masterInfo.Tablet)
		if err != nil {
			log.Errorf("failed to run explicit healthcheck on tablet: %v err: %v", masterInfo, err)
			terCtx, terCtxCancel := context.WithTimeout(context.Background(), *MasterCheckTimeout)
			defer terCtxCancel()
			if err = agent.TabletExternallyReparented(terCtx, topoproto.TabletAliasString(alias)); err != nil {
				log.Errorf("could not become master tablet: %v, error: %v", tablet, err)
			}
			return
		}
	} else {
		// this tablet is the master but topology thinks there's another master, go back to spare
		if !topoproto.TabletAliasEqual(masterAlias, alias) {
			ctCtx, ctCancel := context.WithTimeout(context.Background(), *MasterCheckTimeout)
			defer ctCancel()
			if err := agent.ChangeType(ctCtx, topodata.TabletType_SPARE); err != nil {
				log.Errorf("could not change to spare: %v, error: %v", topoproto.TabletAliasString(tablet.Alias), err)
			}
			return
		}
	}
}

func (job *masterRepairJob) calculateDelay() time.Duration {
	jitter := rand.Int63n(MasterCheckFreq.Nanoseconds() / 10)
	return time.Duration(MasterCheckFreq.Nanoseconds() + jitter)
}
