package main

import (
	"flag"
	"net"
	"os"
	"sort"
	"syscall"
	"time"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager"
)

var (
	dnsPollerFreq = flag.Duration("dns-poller-freq", 0*time.Second, "frequency to poll dns for the db host, if it changes we shutdown ourselves")
	dnsPollerShutdownDelay = flag.Duration("dns-poller-shutdown-delay", 10*time.Second, "delay after detecting a DNS change before shutting down")
)

type DNSPoller struct {
	agent *tabletmanager.ActionAgent
	done  chan bool

	firstAddresses []string
}

func initDNSPoller(agent *tabletmanager.ActionAgent) {
	poller := &DNSPoller{agent: agent, done: make(chan bool)}
	poller.Start()
}

func (p *DNSPoller) Start() {
	frequency := *dnsPollerFreq
	if frequency == 0 {
		return
	}

	log.Infof("Polling for DNS changes to %v every %v", p.getDbHost(), frequency)
	go func() {
		for {
			select {
			case <-p.done:
				log.Infof("Exiting dns poller")
				return
			case <-time.After(frequency):
				p.checkDNS()
			}
		}
	}()

	servenv.OnTerm(func() {
		p.stop()
	})
}

func (p *DNSPoller) stop() {
	log.Infof("Stopping DNS poller")
	p.done <- true
}

func (p *DNSPoller) checkDNS() {
	dbHost := p.getDbHost()

	addrs, err := net.LookupHost(dbHost)
	if err != nil {
		log.Errorf("error refreshing db host dns name %v: %v", dbHost, err)
		return
	}
	sort.Strings(addrs)

	if p.firstAddresses == nil {
		// Store the first addresses we see, if that ever changes we demote and shut down (to be restarted by k8s)
		p.firstAddresses = addrs
	}

	if !stringsEqual(p.firstAddresses, addrs) {
		delay := *dnsPollerShutdownDelay
		log.Warningf("DNS entry for db host %v changed from %v to %v, shutting down in %v",
			dbHost, p.firstAddresses, addrs, delay)
		select {
		case <-p.done:
			log.Infof("Exiting dns poller, shutdown aborted")
			return
		case <-time.After(delay):
		}
		p.shutdown()
	}
}

func (p *DNSPoller) getDbHost() string {
	return p.agent.DBConfigs.AppWithDB().Host
}

func (p *DNSPoller) shutdown() {
	err := syscall.Kill(os.Getpid(), syscall.SIGTERM)
	if err != nil {
		log.Errorf("could not shut down via TERM signal, syscall.Exiting instead: %v", err)
		syscall.Exit(0)
	}
}

func stringsEqual(ips []string, ips2 []string) bool {
	if len(ips) != len(ips2) {
		return false
	}
	for i, ip := range ips {
		if ip != ips2[i] {
			return false
		}
	}
	return true
}
