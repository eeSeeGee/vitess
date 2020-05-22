/*
Copyright 2020 The Vitess Authors.

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

package mysql

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

var (
	mysqlAuthServerServiceCertUserFile       = flag.String("mysql_auth_server_service_cert_user_file", "", "JSON File to read the users/groups from.")
	mysqlAuthServerServiceCertReloadInterval = flag.Duration("mysql_auth_server_service_cert_reload_interval", 0, "Ticker to reload credentials")
)

// AuthServerServiceCert implements AuthServer using s2s certificate configuration.
type AuthServerServiceCert struct {
	file           string
	reloadInterval time.Duration
	// method can be set to:
	// - MysqlClearPassword
	// - MysqlDialog
	// It defaults to MysqlClearPassword.
	method string
	// This mutex helps us prevent data races between the multiple updates of entries.
	mu sync.Mutex
	// entries contains the users and groups.
	entries map[string]*AuthServerServiceCertEntry

	sigChan chan os.Signal
	ticker  *time.Ticker
}

// AuthServerServiceCertEntry stores the values for a given user.
type AuthServerServiceCertEntry struct {
	Groups []string
}

// ServiceCertUserData holds the username and groups
type ServiceCertUserData struct {
	username string
	groups   []string
}

// Get returns the wrapped username and groups
func (s *ServiceCertUserData) Get() *querypb.VTGateCallerID {
	return &querypb.VTGateCallerID{Username: s.username, Groups: s.groups}
}

// InitAuthServerServiceCert Handles initializing the AuthServerServiceCert if necessary.
func InitAuthServerServiceCert() {
	// Check parameters.
	if *mysqlAuthServerServiceCertUserFile == "" {
		// Not configured, nothing to do.
		log.Infof("Not configuring AuthServerServiceCert, as mysql_auth_server_service_cert_user_file is empty")
		return
	}

	// Create and register auth server.
	RegisterAuthServerServiceCertFromParams(*mysqlAuthServerServiceCertUserFile, *mysqlAuthServerServiceCertReloadInterval)
}

// RegisterAuthServerServiceCertFromParams creates and registers a new
// AuthServerServiceCertFromParams, loaded for a JSON file. It
// log.Exits out in case of error.
func RegisterAuthServerServiceCertFromParams(file string, reloadInterval time.Duration) {
	authServerServiceCert := NewAuthServerServiceCert(file, reloadInterval)
	if len(authServerServiceCert.entries) <= 0 {
		log.Exitf("Failed to populate entries from file: %v", file)
	}
	RegisterAuthServerImpl("servicecert", authServerServiceCert)
}

// NewAuthServerServiceCert returns a new empty AuthServerServiceCert.
func NewAuthServerServiceCert(file string, reloadInterval time.Duration) *AuthServerServiceCert {
	a := &AuthServerServiceCert{
		file:           file,
		reloadInterval: reloadInterval,
		method:         MysqlClearPassword,
		entries:        make(map[string]*AuthServerServiceCertEntry),
	}
	a.reload()
	a.installSignalHandlers()
	return a
}

func (assc *AuthServerServiceCert) reload() {
	data, err := ioutil.ReadFile(assc.file)
	if err != nil {
		log.Errorf("Failed to read mysql_auth_server_service_cert_user_file file: %v", err)
		return
	}

	entries := make(map[string]*AuthServerServiceCertEntry)
	if err := assc.parseConfig(data, &entries); err != nil {
		log.Errorf("Error parsing auth server config: %v", err)
		return
	}

	assc.mu.Lock()
	assc.entries = entries
	assc.mu.Unlock()
}

func (a *AuthServerServiceCert) installSignalHandlers() {
	if a.file == "" {
		return
	}

	a.sigChan = make(chan os.Signal, 1)
	signal.Notify(a.sigChan, syscall.SIGHUP)
	go func() {
		for range a.sigChan {
			a.reload()
		}
	}()

	// If duration is set, it will reload configuration every interval
	if a.reloadInterval > 0 {
		a.ticker = time.NewTicker(a.reloadInterval)
		go func() {
			for range a.ticker.C {
				a.sigChan <- syscall.SIGHUP
			}
		}()
	}
}

func (a *AuthServerServiceCert) close() {
	if a.ticker != nil {
		a.ticker.Stop()
	}
	if a.sigChan != nil {
		signal.Stop(a.sigChan)
	}
}

func (assc *AuthServerServiceCert) parseConfig(jsonBytes []byte, config *map[string]*AuthServerServiceCertEntry) error {
	decoder := json.NewDecoder(bytes.NewReader(jsonBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(config); err != nil {
		return err
	}
	return assc.validateConfig(*config)
}

func (assc *AuthServerServiceCert) validateConfig(config map[string]*AuthServerServiceCertEntry) error {
	for _, entry := range config {
		if len(entry.Groups) <= 0 {
			return vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "empty Groups found (at least one group required)")
		}
	}
	return nil
}

// AuthMethod is part of the AuthServer interface.
func (ascc *AuthServerServiceCert) AuthMethod(user string) (string, error) {
	return ascc.method, nil
}

// Salt is part of the AuthServer interface.
func (ascc *AuthServerServiceCert) Salt() ([]byte, error) {
	return NewSalt()
}

// ValidateHash is unimplemented.
func (assc *AuthServerServiceCert) ValidateHash(salt []byte, user string, authResponse []byte, remoteAddr net.Addr) (Getter, error) {
	panic("unimplemented")
}

// Negotiate is part of the AuthServer interface.
func (assc *AuthServerServiceCert) Negotiate(c *Conn, user string, remoteAddr net.Addr) (Getter, error) {
	// This code depends on the fact that golang's tls server enforces client cert verification.
	// Note that the -mysql_server_ssl_ca flag must be set in order for the vtgate to accept client certs.
	// If not set, the vtgate will effectively deny all incoming mysql connections, since they will all lack certificates.
	// For more info, check out go/vt/vtttls/vttls.go
	certs := c.GetTLSClientCerts()
	if len(certs) == 0 {
		return &ServiceCertUserData{}, fmt.Errorf("no client certs for connection ID %v", c.ConnectionID)
	}

	if _, err := AuthServerReadPacketString(c); err != nil {
		return &ServiceCertUserData{}, err
	}

	commonName := certs[0].Subject.CommonName

	if user != commonName {
		return &ServiceCertUserData{}, fmt.Errorf("MySQL connection username '%v' does not match client cert common name '%v'", user, commonName)
	}

	assc.mu.Lock()
	entry, ok := assc.entries[user]
	assc.mu.Unlock()

	if !ok {
		return &ServiceCertUserData{}, NewSQLError(ERAccessDeniedError, SSAccessDeniedError, "Access denied for user '%v' from address '%v'", user, remoteAddr.String())
	}

	return &ServiceCertUserData{
		username: commonName,
		groups:   entry.Groups,
	}, nil
}
